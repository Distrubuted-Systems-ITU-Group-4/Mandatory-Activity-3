# Chit Chat

A distributed chat service written in Go using gRPC. One server and any number of clients run as separate processes. Participants can join, publish messages and leave at any time, and every message carries a Lamport timestamp.

## Requirements

- Go 1.23 or newer

## Project structure

```
client/   client program (client.go)
grpc/     chitchat.proto and the generated gRPC code
server/   server program (server.go)
logs/     log files from the server and clients
```

## How to run

Run every command from the project root (the folder containing go.mod), and use a separate terminal for the server and for each client.

1. Download the dependencies (first time only):

   ```
   go mod tidy
   ```

2. Start the server. It listens on port 5050:

   ```
   go run ./server
   ```

3. Start each client in its own terminal and give it a unique name:

   ```
   go run ./client Bob
   ```

   The server must be running before you start a client. 
   
## Using the client

- Type a message and press Enter to publish it to everyone. Messages must not be empty and can be at most 128 characters long.
- Type /leave and press Enter to leave the chat.
- Every received message is shown together with its Lamport timestamp, for example:

  ```
  [Lamport 3] Server: Participant Bob joined Chit Chat at logical time 3
  [Lamport 11] Bob: hello everyone
  ```

- If a name is already in use, the client is rejected and must be started again with another name.

To stop the server, press Ctrl+C in its terminal.

## Logs

Logs are written to the logs/ folder (relative to where the program is started):

- logs/server.log: server startup and shutdown, client connections and disconnections, join/leave broadcasts, and message deliveries
- logs/client_<name>.log: one file per client, recording join, publish, every received message and leave

Every log line contains a timestamp, the component ([Server] or [Client]), the event type (event=...) and the relevant client ID (client=...). The log files are appended to, so delete the logs/ folder to start fresh.

## Regenerating the gRPC code

The generated files in grpc/ are already included. If you change grpc/chitchat.proto, regenerate them with protoc, protoc-gen-go and protoc-gen-go-grpc installed:

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/chitchat.proto
```