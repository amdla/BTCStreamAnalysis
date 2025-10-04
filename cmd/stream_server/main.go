package main

import (
	"fmt"
	"strings"

	"app/internal/stream_server"

	"github.com/gorilla/websocket"
)

func streamData(srv *stream_server.StreamServer) {
	symbols := srv.Config.Symbols
	logger := srv.Logger

	var streams []string
	for _, symbol := range symbols {
		stream := strings.ToLower(symbol) + "@aggTrade"
		streams = append(streams, stream)
	}

	streamsParam := strings.Join(streams, "/")
	url := fmt.Sprintf("wss://fstream.binance.com/stream?streams=%s", streamsParam)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		logger.Info("Failed to connect to Binance Futures:", "Error", err)
		return
	}
	defer conn.Close()

	transactionId := 0

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Error("Error reading message", "error", err)
			break
		}

		transactionId++
		logger.Debug("Message received", "transactionId", transactionId, "message", string(message))
	}
}

func main() {
	srv := stream_server.NewStreamServer()
	streamData(srv)
}
