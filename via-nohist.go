package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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
	lastId   int
}

var mux = &sync.RWMutex{}
var topics = make(map[string]*Topic)
var verbose = false

func splitPassword(combined string) (string, string) {
	split := strings.SplitN(combined, ":", 2)
	if len(split) == 2 {
		return split[0], split[1]
	} else {
		return combined, ""
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
			lastId:   0,
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

	topic.lastId += 1
	msg := Msg{topic.lastId, body}

	for ch := range topic.channels {
		ch <- msg
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

func statusHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.WriteString("Server is running")
}

func handler(ctx *fasthttp.RequestCtx) {
	if verbose {
		log.Println(string(ctx.Method()), string(ctx.Path()))
	}

	if string(ctx.Path()) == "/status" {
		statusHandler(ctx)
		return
	}

	switch string(ctx.Method()) {
	case fasthttp.MethodGet:
		getHandler(ctx)
	case fasthttp.MethodPost:
		postHandler(ctx)
	default:
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "via [-v] [port]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&verbose, "v", false, "enable verbose logs")
	flag.Parse()

	addr := "localhost:8080"
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
