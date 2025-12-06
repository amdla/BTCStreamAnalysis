package jetstream

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

type Event struct {
	ID          string   `json:"id" bson:"id"`
	Subscribers []string `json:"subscribers" bson:"subscribers"`
	Type        string   `json:"type" bson:"type"`
	Source      string   `json:"source" bson:"source"`
	EventData   any      `json:"eventData" bson:"eventData"`
	CreatedAt   string   `json:"createdAt" bson:"createdAt"`
}

func (jsClient *Client) InitNATS() error {
	logger := jsClient.JetStreamLogger
	cfg := jsClient.Config

	natsURL := cfg.NatsURL
	streamName := cfg.StreamName
	subject := cfg.Subject

	// Connect to NATS
	var err error

	jsClient.NatsConnection, err = nats.Connect(natsURL)
	if err != nil {
		logger.Error("Failed to connect to NATS", slog.String("nats_url", natsURL), slog.Any("error", err))

		return err
	}

	logger.Info("Connected to NATS", slog.String("nats_url", natsURL))

	// Get JetStream context
	jsClient.JetStreamContext, err = jsClient.NatsConnection.JetStream()
	if err != nil {
		logger.Error("Failed to get JetStream context", slog.Any("error", err))

		return err
	}

	logger.Info("JetStream context initialized", slog.String("stream", streamName), slog.String("subject", subject))

	// Ensure stream exists
	var msg string

	if _, err := jsClient.JetStreamContext.StreamInfo(streamName); err != nil {
		_, err := jsClient.JetStreamContext.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subject},
		})
		if err != nil {
			logger.Error("Failed to create JetStream stream", slog.String("stream", streamName), slog.Any("error", err))

			return err
		}

		msg = "JetStream stream created"
	} else {
		msg = "JetStream stream already exists"
	}

	logger.Info(msg, slog.String("stream", streamName), slog.String("subject", subject))

	return nil
}

func (jsClient *Client) SendEvent(subject string, event Event) error {
	logger := jsClient.JetStreamLogger

	if jsClient.JetStreamContext == nil {
		return nats.ErrConnectionClosed
	}

	data, err := json.Marshal(event)
	if err != nil {
		logger.Error("Failed to marshal event", slog.Any("error", err))

		return err
	}

	_, err = jsClient.JetStreamContext.Publish(subject, data, nats.AckWait(2*time.Second))
	if err != nil {
		logger.Error("Failed to publish event",
			slog.String("subject", subject),
			slog.Any("error", err),
		)

		return err
	}

	logger.Debug("Event published to JetStream",
		slog.String("subject", subject),
		slog.Int("payload_bytes", len(data)),
	)

	return nil
}
