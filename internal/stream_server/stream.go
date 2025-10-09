package stream_server

import (
	dataconnector "app/internal/data_connector"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func (srv *StreamServer) StreamData() {
	symbols := srv.StreamServerConfig.Symbols
	logger := srv.StreamServerLogger
	srv.JetStreamClient = dataconnector.NewJetStreamClient()
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
	defer conn.Close()

	jsClient := srv.JetStreamClient
	subject := jsClient.JetStreamConfig.Subject // "input.events"

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Error("Error reading message", "error", err)
			break
		}
		logger.Debug("Received message", "message", string(message))

		// Create an Event
		event, err := PackWebSocketMessageToEvent(message)
		if err != nil {
			logger.Error("Failed to pack message to event", "error", err)
		}

		// Publish to NATS via JetStream
		if err := jsClient.SendEvent(subject, event); err != nil {
			logger.Info("============================================================")
			logger.Info("jsClient details", "jsClient", jsClient)
			logger.Error("Failed to send event to NATS", "error", err, "subject", subject, "event_id", event.ID)
			continue
		}

		logger.Debug("Published Binance trade event", "event_id", event.ID)
	}
}

func PackWebSocketMessageToEvent(message []byte) (dataconnector.Event, error) {
	event := dataconnector.Event{
		ID:         uuid.NewString(),
		Subscriber: []string{"consumer.mongo"},
		Type:       "BinanceTradeEvent",
		Source:     "WebSocketStreamServer",
		EventData:  map[string]string{"message": string(message)},
		CreatedAt:  time.Now().UTC().String(),
	}
	return event, nil
}
