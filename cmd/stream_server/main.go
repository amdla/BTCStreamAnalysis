package main

import (
	"app/internal/stream_server"
)

func main() {
	srv := stream_server.NewStreamServer()
	if err := srv.MongoClient.Connect(); err != nil {
		srv.StreamServerLogger.Error("Server connection failed", "error", err)
		return
	}

	srv.StreamData()
}
