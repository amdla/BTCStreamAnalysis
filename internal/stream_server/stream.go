package stream_server

import (
	"fmt"
	"strings"

	"github.com/gorilla/websocket"
)

func (srv *StreamServer) StreamData() {
	symbols := srv.StreamServerConfig.Symbols
	logger := srv.StreamServerLogger

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
