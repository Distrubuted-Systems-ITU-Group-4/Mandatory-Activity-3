# Chit Chat

A chat service in Go using gRPC. One server and several clients, each running as its own process. Every message carries a Lamport timestamp.

## Setup (once)

You need Go 1.23 or newer. From the project folder, run:

    go mod tidy

## Run

Run every command from the project folder, each in its own terminal.

1. Start the server (port 5050): `go run ./server`
2. Start clients with different names: `go run ./client Sebastian`, `go run ./client Lars`, ...
3. Type a message (at most 128 characters) and press Enter. Everyone sees it with its Lamport time.
4. Type `/leave` to leave. Closing a client with Ctrl+C also works; the server tells the others.
5. Stop the server with Ctrl+C.

## Logs

The server logs to the terminal and to `logs/server.log`. Each client logs to `logs/client_<name>.log`. The `logs/` folder contains the test run used in the report.
