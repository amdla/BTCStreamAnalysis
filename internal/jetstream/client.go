package jetstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type Client struct {
	Config           *Config
	JetStreamLogger  *slog.Logger
	NatsConnection   *nats.Conn
	JetStreamContext nats.JetStreamContext
}

func NewJetStreamClient() *Client {
	cfg := InitializeJetStreamConfig()
	logger := InitializeJetStreamLogger(cfg.IsDebugMode)

	return &Client{
		Config:           cfg,
		JetStreamLogger:  logger,
		NatsConnection:   nil,
		JetStreamContext: nil,
	}
}

func InitializeJetStreamLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	logger.Info("JetStream logger initialized",
		slog.Bool("debug", debug),
	)

	return logger
}

func (jsClient *Client) Connect() error {
	cfg := jsClient.Config
	logger := jsClient.JetStreamLogger

	conn, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	jsClient.NatsConnection = conn
	jsClient.JetStreamLogger.Info("Connected to NATS", slog.String("url", cfg.NatsURL))

	jsClient.JetStreamContext, err = conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Ensure input stream exists
	_, err = jsClient.JetStreamContext.AddStream(&nats.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: []string{cfg.Subject},
		Storage:  nats.FileStorage,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		logger.Warn("Failed to create stream (may already exist)", "error", err)
	}

	return nil
}

func (jsClient *Client) Publish(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = jsClient.JetStreamContext.Publish(subject, data, nats.AckWait(2*time.Second))

	return err
}

func (jsClient *Client) Close() error {
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

func (jsClient *Client) DeferJetStreamClose() func() {
	return func() {
		if err := jsClient.Close(); err != nil {
			jsClient.JetStreamLogger.Error("Failed to close JetStream client", "error", err)
		}
	}
}
