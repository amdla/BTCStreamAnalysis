package data_connector

import (
	"encoding/json"
	"fmt"
	"time"

	"log/slog"

	"github.com/nats-io/nats.go"
)

func (jsClient *JetStreamClient) InitNATS() error {
	logger := jsClient.JetStreamLogger
	cfg := jsClient.JetStreamConfig

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

func (jsClient *JetStreamClient) SendEvent(subject string, event Event) error {
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

func (jsClient *JetStreamClient) Close() error {
	if jsClient == nil {
		return fmt.Errorf("JetStreamClient is nil")
	}

	logger := jsClient.JetStreamLogger
	if jsClient.NatsConnection != nil {
		if err := jsClient.NatsConnection.Drain(); err != nil {
			logger.Error("Error draining NATS connection", slog.Any("error", err))
		}
		jsClient.NatsConnection.Close()
		logger.Info("NATS connection closed")
	}

	return nil
}
