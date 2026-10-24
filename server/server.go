package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
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
