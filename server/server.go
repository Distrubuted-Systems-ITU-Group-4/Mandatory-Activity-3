package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"unicode/utf8"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clock   int64
	clients map[string]chan *pb.ChatMessage
}

func (s *Server) broadcast(msg *pb.ChatMessage) {
	for name, queue := range s.clients {
		select {
		case queue <- msg:
		default:
			log.Printf("event=DROP client=%s lamport=%d", name, msg.Lamport)
		}
	}
}

func (s *Server) removeClient(name string, queue chan *pb.ChatMessage) {
	if current, ok := s.clients[name]; !ok || current != queue {
		return
	}
	delete(s.clients, name)
	close(queue)
	log.Printf("event=CLIENT_DISCONNECT client=%s lamport=%d", name, s.clock)

	s.clock++
	text := fmt.Sprintf("Participant %s left Chit Chat at logical time %d", name, s.clock)
	log.Printf("event=BROADCAST_LEAVE client=%s lamport=%d", name, s.clock)
	s.broadcast(&pb.ChatMessage{Sender: "Server", Text: text, Lamport: s.clock})
}

func (s *Server) Join(req *pb.JoinRequest, stream pb.ChitChat_JoinServer) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("the name must not be empty")
	}
	queue := make(chan *pb.ChatMessage, 100)

	s.mu.Lock()
	if _, taken := s.clients[req.Name]; taken {
		s.mu.Unlock()
		return fmt.Errorf("the name %s is already taken", req.Name)
	}
	s.clock = max(s.clock, req.Lamport) + 1
	s.clients[req.Name] = queue
	log.Printf("event=CLIENT_CONNECT client=%s lamport=%d", req.Name, s.clock)

	s.clock++
	text := fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", req.Name, s.clock)
	log.Printf("event=BROADCAST_JOIN client=%s lamport=%d", req.Name, s.clock)
	s.broadcast(&pb.ChatMessage{Sender: "Server", Text: text, Lamport: s.clock})
	s.mu.Unlock()

	for {
		select {
		case msg, open := <-queue:
			if !open {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				s.mu.Lock()
				s.removeClient(req.Name, queue)
				s.mu.Unlock()
				return err
			}
			log.Printf("event=DELIVER client=%s sender=%s lamport=%d", req.Name, msg.Sender, msg.Lamport)
		case <-stream.Context().Done():
			s.mu.Lock()
			s.removeClient(req.Name, queue)
			s.mu.Unlock()
			return nil
		}
	}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.Answer, error) {
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("message must not be empty")
	}
	if utf8.RuneCountInString(req.Text) > 128 {
		return nil, fmt.Errorf("message is longer than 128 characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = max(s.clock, req.Lamport) + 1
	log.Printf("event=PUBLISH_RECEIVED client=%s lamport=%d text=%q", req.Name, s.clock, req.Text)

	s.clock++
	log.Printf("event=BROADCAST_MESSAGE client=%s lamport=%d", req.Name, s.clock)
	s.broadcast(&pb.ChatMessage{Sender: req.Name, Text: req.Text, Lamport: s.clock})

	s.clock++
	return &pb.Answer{Lamport: s.clock}, nil
}

func (s *Server) Disconnect(ctx context.Context, req *pb.DisconnectRequest) (*pb.Answer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = max(s.clock, req.Lamport) + 1
	s.removeClient(req.Name, s.clients[req.Name])

	s.clock++
	return &pb.Answer{Lamport: s.clock}, nil
}

func main() {
	os.MkdirAll("logs", 0755)
	logFile, err := os.OpenFile("logs/server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("could not open log file:", err)
		return
	}
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetPrefix("[Server] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("event=SERVER_ERROR reason=%v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServer(grpcServer, &Server{clients: make(map[string]chan *pb.ChatMessage)})
	log.Printf("event=SERVER_START port=5050")

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		log.Printf("event=SERVER_SHUTDOWN")
		grpcServer.Stop()
	}()

	grpcServer.Serve(listener)
}