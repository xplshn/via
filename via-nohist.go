package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/valyala/fasthttp"
)

var (
	verbose bool
	port    int
	password = "secret"
)

type PubSub struct {
	mu       sync.Mutex
	clients  map[string]chan string
	messages chan string
}

func NewPubSub() *PubSub {
	return &PubSub{
		clients:  make(map[string]chan string),
		messages: make(chan string),
	}
}

func (ps *PubSub) Subscribe(clientID string) chan string {
	ch := make(chan string, 1)
	ps.mu.Lock()
	ps.clients[clientID] = ch
	ps.mu.Unlock()
	return ch
}

func (ps *PubSub) Unsubscribe(clientID string) {
	ps.mu.Lock()
	delete(ps.clients, clientID)
	ps.mu.Unlock()
}

func (ps *PubSub) Publish(msg string) {
	ps.messages <- msg
}

func (ps *PubSub) Run() {
	for msg := range ps.messages {
		ps.mu.Lock()
		for _, ch := range ps.clients {
			ch <- msg
		}
		ps.mu.Unlock()
	}
}

func main() {
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose mode")
	flag.IntVar(&port, "port", 8080, "Port to listen on")
	flag.Parse()

	ps := NewPubSub()
	go ps.Run()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/subscribe":
			handleSubscribe(ctx, ps)
		case "/publish":
			handlePublish(ctx, ps)
		case "/status":
			handleStatus(ctx)
		default:
			ctx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

	log.Printf("Starting server on port %d\n", port)
	if err := fasthttp.ListenAndServe(fmt.Sprintf(":%d", port), requestHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func handleSubscribe(ctx *fasthttp.RequestCtx, ps *PubSub) {
	if string(ctx.QueryArgs().Peek("password")) != password {
		ctx.Error("Unauthorized", fasthttp.StatusUnauthorized)
		return
	}

	clientID := string(ctx.QueryArgs().Peek("clientID"))
	if clientID == "" {
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	ch := ps.Subscribe(clientID)
	defer ps.Unsubscribe(clientID)

	ctx.SetBody([]byte("Subscribed"))
	ctx.SetStatusCode(fasthttp.StatusOK)

	for msg := range ch {
		if verbose {
			log.Printf("Sending message to %s: %s\n", clientID, msg)
		}
		ctx.Write([]byte(msg))
	}
}

func handlePublish(ctx *fasthttp.RequestCtx, ps *PubSub) {
	if string(ctx.QueryArgs().Peek("password")) != password {
		ctx.Error("Unauthorized", fasthttp.StatusUnauthorized)
		return
	}

	msg := string(ctx.PostBody())
	if msg == "" {
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	ps.Publish(msg)
	ctx.SetBody([]byte("Published"))
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func handleStatus(ctx *fasthttp.RequestCtx) {
	ctx.SetBody([]byte("OK"))
	ctx.SetStatusCode(fasthttp.StatusOK)
}
