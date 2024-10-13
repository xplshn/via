package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"github.com/valyala/fasthttp"
)

type Msg struct {
	Id   int
	Data []byte
}

type Topic struct {
	sync.Mutex
	channels map[chan Msg]bool
	password string
	hasHistory bool
	history []Msg
	lastId int
}

var mux = &sync.RWMutex{}
var topics = make(map[string]*Topic)
var verbose = false
var maxHistorySize = 100
var dir = ""

func splitPassword(combined string) (string, string) {
	split := strings.SplitN(combined, ":", 2)
	if len(split) == 2 {
		return split[0], split[1]
	} else {
		return combined, ""
	}
}

func getStorePath(key string) string {
	hash := base64.URLEncoding.EncodeToString([]byte(key))
	return path.Join(dir, hash)
}

func (topic *Topic) storeHistory(key string) {
	path := getStorePath(fmt.Sprintf("%s:%s", key, topic.password))

	content, err := json.Marshal(topic.history)
	if err != nil {
		log.Println("error storing history:", err)
		return
	}

	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		log.Println("error storing history:", err)
		return
	}
}

func (topic *Topic) restoreHistory(key string) {
	path := getStorePath(fmt.Sprintf("%s:%s", key, topic.password))

	content, err := ioutil.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("error restoring history:", err)
		}
		return
	}

	var history []Msg
	err = json.Unmarshal(content, &history)
	if err != nil {
		log.Println("error restoring history:", err)
		return
	}

	topic.history = history
	if len(history) > 0 {
		topic.lastId = history[len(history)-1].Id
	}
}

func (topic *Topic) deleteHistory(key string) {
	path := getStorePath(fmt.Sprintf("%s:%s", key, topic.password))

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		log.Println("error deleting history:", err)
	}
}

func (topic *Topic) post(data []byte) {
	topic.lastId += 1
	msg := Msg{topic.lastId, data}

	if topic.hasHistory {
		topic.history = append(topic.history, msg)

		for len(topic.history) > maxHistorySize {
			topic.history = topic.history[1:]
		}
	}

	for ch := range topic.channels {
		ch <- msg
	}
}

func (topic *Topic) put(data []byte, lastId int) {
	if len(topic.history) > 0 && lastId < topic.history[0].Id {
		return
	}

	history := make([]Msg, 0)
	history = append(history, Msg{lastId, data})
	for _, msg := range topic.history {
		if msg.Id > lastId {
			history = append(history, msg)
		}
	}
	topic.history = history

	if lastId > topic.lastId {
		topic.lastId = lastId
	}
}

func getTopic(key string, password string) (*Topic, bool) {
	mux.RLock()
	topic, ok := topics[key]
	mux.RUnlock()

	if !ok {
		topic = &Topic{
			channels: make(map[chan Msg]bool, 0),
			password: password,
			hasHistory: strings.HasPrefix(key, "/hmsg/"),
			history: make([]Msg, 0),
			lastId: 0,
		}
		if topic.hasHistory {
			topic.restoreHistory(key)
		}
		mux.Lock()
		topics[key] = topic
		mux.Unlock()
	} else if topic.password != password {
		return nil, false
	}

	return topic, true
}

func pushChannel(key string, password string, ch chan Msg, lastId int) bool {
	topic, allowed := getTopic(key, password)
	if !allowed {
		return false
	}

	go func() {
		topic.Lock()
		defer topic.Unlock()

		for _, msg := range topic.history {
			if msg.Id > lastId {
				ch <- msg
			}
		}

		topic.channels[ch] = true
	}()

	return true
}

func popChannel(key string, ch chan Msg) {
	mux.RLock()
	topic := topics[key]
	mux.RUnlock()

	topic.Lock()
	delete(topic.channels, ch)
	topic.Unlock()

	if len(topic.channels) == 0 {
		if verbose {
			log.Println("clearing topic", key)
		}
		mux.Lock()
		delete(topics, key)
		mux.Unlock()
	}
}

func postHandler(ctx *fasthttp.RequestCtx) {
	key, password := splitPassword(string(ctx.Path()))

	if password != "" {
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		return
	}

	body := ctx.PostBody()

	mux.RLock()
	topic, ok := topics[key]
	mux.RUnlock()

	if !ok {
		return
	}

	topic.Lock()
	defer topic.Unlock()

	topic.post(body)

	if topic.hasHistory {
		topic.storeHistory(key)
	}
}

func getHandler(ctx *fasthttp.RequestCtx) {
	key, password := splitPassword(string(ctx.Path()))

	lastIdStr := ctx.Request.Header.Peek("Last-Event-ID")
	lastId, err := strconv.Atoi(string(lastIdStr))
	if err != nil {
		lastId = 0
	}

	ch := make(chan Msg)
	allowed := pushChannel(key, password, ch, lastId)
	if !allowed {
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		return
	}
	defer popChannel(key, ch)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx.Response.Header.Set("Content-Type", "text/event-stream")
	ctx.Response.Header.Set("X-Accel-Buffering", "no")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.WriteString(": ping\n\n")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctx.WriteString(": ping\n\n")
		case msg := <-ch:
			ctx.WriteString(fmt.Sprintf("id: %d\ndata: %s\n\n", msg.Id, msg.Data))
		}
	}
}

func putHandler(ctx *fasthttp.RequestCtx) {
	key, password := splitPassword(string(ctx.Path()))

	topic, allowed := getTopic(key, password)

	if !allowed {
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		return
	} else if !topic.hasHistory {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	lastIdStr := ctx.Request.Header.Peek("Last-Event-ID")
	lastId, err := strconv.Atoi(string(lastIdStr))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	body := ctx.PostBody()

	topic.Lock()
	defer topic.Unlock()

	topic.put(body, lastId)
	topic.storeHistory(key)
}

func delHandler(ctx *fasthttp.RequestCtx) {
	key, password := splitPassword(string(ctx.Path()))

	topic, allowed := getTopic(key, password)

	if !allowed {
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		return
	} else if !topic.hasHistory {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	topic.Lock()
	defer topic.Unlock()

	topic.history = make([]Msg, 0)
	topic.deleteHistory(key)
}

func handler(ctx *fasthttp.RequestCtx) {
	if verbose {
		log.Println(string(ctx.Method()), string(ctx.Path()))
	}

	switch string(ctx.Method()) {
	case fasthttp.MethodGet:
		getHandler(ctx)
	case fasthttp.MethodPost:
		postHandler(ctx)
	case fasthttp.MethodPut:
		putHandler(ctx)
	case fasthttp.MethodDelete:
		delHandler(ctx)
	default:
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "via [-v] [-d storage_dir] [port]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&verbose, "v", false, "enable verbose logs")
	flag.StringVar(&dir, "d", ".", "directory for storage")
	flag.Parse()

	addr := "localhost:8001"
	if len(flag.Args()) > 0 {
		addr = fmt.Sprintf("localhost:%s", flag.Args()[0])
	}

	s := &fasthttp.Server{
		Handler: handler,
	}

	ctx, unregisterSignals := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)

	go func() {
		log.Printf("Serving on http://%s", addr)
		err := s.ListenAndServe(addr)
		if err != nil {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	unregisterSignals()
	log.Println("Shutting down server…")
	s.Shutdown()
}
