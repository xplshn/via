package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/valyala/fasthttp"
)

type Server struct {
	topics   map[string]*Topic
	mu       sync.RWMutex
	verbose  bool
	port     string
}

type Topic struct {
	subscribers map[string]chan string
	password    string
	mu          sync.RWMutex
}

type StatusResponse struct {
	Topics []string `json:"topics"`
}

func NewServer(verbose bool) *Server {
	return &Server{
		topics:  make(map[string]*Topic),
		verbose: verbose,
	}
}

func (s *Server) Publish(topic string, message string) {
	s.mu.RLock()
	t, exists := s.topics[topic]
	s.mu.RUnlock()

	if exists {
		t.mu.RLock()
		defer t.mu.RUnlock()
		for _, subscriber := range t.subscribers {
			subscriber <- message
		}
	}
}

func (s *Server) Subscribe(topic, password string) (chan string, error) {
	s.mu.RLock()
	t, exists := s.topics[topic]
	s.mu.RUnlock()

	if !exists || t.password != password {
		return nil, fmt.Errorf("subscription failed: topic not found or incorrect password")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan string)
	t.subscribers[password] = ch

	return ch, nil
}

func (s *Server) CreateTopic(name, password string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[name] = &Topic{
		subscribers: make(map[string]chan string),
		password:    password,
	}
}

func (s *Server) StatusHandler(ctx *fasthttp.RequestCtx) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := StatusResponse{Topics: make([]string, 0, len(s.topics))}
	for topic := range s.topics {
		status.Topics = append(status.Topics, topic)
	}

	if s.verbose {
		log.Println("Status requested")
	}

	ctx.SetContentType("application/json")
	json.NewEncoder(ctx).Encode(status)
}

func (s *Server) PublishHandler(ctx *fasthttp.RequestCtx) {
	topic := ctx.UserValue("topic").(string)
	message := string(ctx.PostBody())

	s.Publish(topic, message)

	if s.verbose {
		log.Printf("Published message to topic '%s': %s\n", topic, message)
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (s *Server) SubscribeHandler(ctx *fasthttp.RequestCtx) {
	topic := ctx.UserValue("topic").(string)
	password := ctx.QueryArgs().Peek("password")

	ch, err := s.Subscribe(topic, string(password))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusUnauthorized)
		ctx.SetBodyString(err.Error())
		return
	}

	if s.verbose {
		log.Printf("Subscriber added for topic '%s'\n", topic)
	}

	go func() {
		for msg := range ch {
			_, _ = ctx.Write([]byte(msg + "\n"))
		}
	}()

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (s *Server) SetupRoutes() {
	fasthttp.ListenAndServe(":"+s.port, func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/status":
			s.StatusHandler(ctx)
		case "/publish":
			s.PublishHandler(ctx)
		case "/subscribe":
			s.SubscribeHandler(ctx)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	})
}

func main() {
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	server := NewServer(*verbose)
	server.CreateTopic("testTopic", "secret")

	if *verbose {
		log.Printf("Starting server on port %s...\n", *port)
	}

	server.port = *port
	server.SetupRoutes()
}
