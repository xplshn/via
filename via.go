// Simple pubsub server inspired by https://patchbay.pub
//
// Usage: via [-v] [[host]:port]
// curl http://localhost:8001/someid/  # block
// curl http://localhost:8001/someid/?sse  # server sent event stream
// curl http://localhost:8001/someid/ -d somedata
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Topic struct {
	sync.RWMutex
	channels map[chan []byte]bool
}

var mux = &sync.RWMutex{}
var topics = make(map[string]Topic)
var verbose = false

func pushChannel(key string, ch chan []byte) {
	mux.RLock()
	topic, ok := topics[key]
	mux.RUnlock()

	if !ok {
		topic = Topic{
			channels: make(map[chan []byte]bool, 0),
		}
		mux.Lock()
		topics[key] = topic
		mux.Unlock()
	}

	topic.Lock()
	topic.channels[ch] = true
	topic.Unlock()
}

func popChannel(key string, ch chan []byte) {
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

func post(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("error reading request body:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	mux.RLock()
	topic, ok := topics[key]
	mux.RUnlock()

	if !ok {
		return
	}

	topic.RLock()
	defer topic.RUnlock()

	for channel := range topic.channels {
		go func(ch chan []byte) {
			ch <- body
		}(channel)
	}
}

func getBlocking(w http.ResponseWriter, r *http.Request) {
	ch := make(chan []byte)
	pushChannel(r.URL.Path, ch)
	defer popChannel(r.URL.Path, ch)

	w.Write(<-ch)
}

func getSse(w http.ResponseWriter, r *http.Request) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan []byte)
	pushChannel(r.URL.Path, ch)
	defer popChannel(r.URL.Path, ch)

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case s := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", s)
			flusher.Flush()
		}
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	if verbose {
		log.Println(r.Method, r.URL)
	}

	if r.Method == "GET" {
		if r.URL.RawQuery == "sse" {
			getSse(w, r)
		} else {
			getBlocking(w, r)
		}
	} else if r.Method == "POST" {
		post(w, r)
	} else {
		http.Error(w, "Unsupported Method", http.StatusMethodNotAllowed)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "via [-v] [[host]:port]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&verbose, "v", false, "enable verbose logs")
	flag.Parse()

	addr := "localhost:8001"
	if len(flag.Args()) > 0 {
		addr = flag.Args()[0]
	}
	if addr[0] == ':' {
		addr = "localhost" + addr
	}

	http.HandleFunc("/", handler)
	log.Printf("Serving on http://%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
