package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	mu    sync.Mutex
	clock int64
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run ./client <name>")
		return
	}
	name := os.Args[1]

	os.MkdirAll("logs", 0755)
	logFile, err := os.OpenFile("logs/client_"+name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("could not open log file:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetPrefix("[Client] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("could not connect:", err)
		return
	}
	defer conn.Close()
	client := pb.NewChitChatClient(conn)

	mu.Lock()
	clock++
	t := clock
	mu.Unlock()
	log.Printf("event=JOIN client=%s lamport=%d", name, t)
	stream, err := client.Join(context.Background(), &pb.JoinRequest{Name: name, Lamport: t})
	if err != nil {
		fmt.Println("could not join:", err)
		return
	}

	msg, err := stream.Recv()
	if err != nil {
		fmt.Println("could not join:", err)
		return
	}

	done := make(chan bool)
	go func() {
		for {
			mu.Lock()
			clock = max(clock, msg.Lamport) + 1
			local := clock
			mu.Unlock()

			fmt.Printf("[Lamport %d] %s: %s\n", msg.Lamport, msg.Sender, msg.Text)
			log.Printf("event=RECEIVE client=%s sender=%s msg_lamport=%d local_lamport=%d text=%q",
				name, msg.Sender, msg.Lamport, local, msg.Text)

			next, err := stream.Recv()
			if err == io.EOF {
				close(done)
				return
			}
			if err != nil {
				fmt.Println("disconnected:", err)
				log.Printf("event=DISCONNECT client=%s reason=%q", name, err.Error())
				os.Exit(1)
			}
			msg = next
		}
	}()

	fmt.Println("Type a message and press Enter. Type /leave to leave.")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "/leave" {
			break
		}
		if text == "" {
			continue
		}

		mu.Lock()
		clock++
		t := clock
		mu.Unlock()
		log.Printf("event=PUBLISH client=%s lamport=%d text=%q", name, t, text)

		answer, err := client.Publish(context.Background(), &pb.PublishRequest{Name: name, Text: text, Lamport: t})
		if err != nil {
			fmt.Println("could not publish:", err)
			continue
		}
		mu.Lock()
		clock = max(clock, answer.Lamport) + 1
		mu.Unlock()
	}

	mu.Lock()
	clock++
	t = clock
	mu.Unlock()
	log.Printf("event=LEAVE client=%s lamport=%d", name, t)
	answer, err := client.Disconnect(context.Background(), &pb.DisconnectRequest{Name: name, Lamport: t})
	if err == nil {
		mu.Lock()
		clock = max(clock, answer.Lamport) + 1
		mu.Unlock()
	}

	<-done
	log.Printf("event=DISCONNECT client=%s reason=left lamport=%d", name, clock)
	fmt.Println("You left Chit Chat.")
}