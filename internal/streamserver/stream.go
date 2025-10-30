package streamserver

import (
	"app/internal/jetstream"
	"fmt"
	"strings"

	"github.com/gorilla/websocket"
)

func (srv *StreamServer) StreamData() {
	symbols := srv.StreamServerConfig.Symbols
	logger := srv.StreamServerLogger
	srv.JetStreamClient = jetstream.NewJetStreamClient()

	if err := srv.JetStreamClient.InitNATS(); err != nil {
		logger.Error("Failed to initialize NATS JetStream", "error", err)
		return
	}

	var streams []string

	for _, symbol := range symbols {
		stream := strings.ToLower(symbol) + "@aggTrade"
		streams = append(streams, stream)
	}

	streamsParam := strings.Join(streams, "/")
	url := fmt.Sprintf("wss://fstream.binance.com/stream?streams=%s", streamsParam)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		logger.Error("Failed to connect to Binance Futures", "error", err)
		return
	}
	defer func(conn *websocket.Conn) {
		err := conn.Close()
		if err != nil {
			logger.Error("Failed to close websocket connection", "error", err)
		}
	}(conn)

	jsClient := srv.JetStreamClient
	subject := jsClient.Config.Subject

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Error("Error reading message", "error", err)
			break
		}

		logger.Debug("Received message", "message", string(message))

		// Create an Event
		event, err := PackObjToEvent(message)
		if err != nil {
			logger.Error("Failed to pack message to event", "error", err)
		}

		// Publish to NATS via JetStream
		if err := jsClient.SendEvent(subject, event); err != nil {
			logger.Error("Failed to send event to NATS", "error", err, "subject", subject, "event_id", event.ID)

			continue
		}

		logger.Debug("Published Binance trade event", "event_id", event.ID)
	}
}
