package dataconnector

import (
	"app/internal/jetstream"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const BatchSize = 10

type Connector struct {
	Cfg      *Config
	JsClient *jetstream.Client
	Sub      *nats.Subscription
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewDataConnector(jsClient *jetstream.Client) *Connector {
	cfg := InitializeDataConnectorConfig()

	if jsClient.NatsConnection == nil || jsClient.JetStreamContext == nil {
		err := jsClient.Connect()
		if err != nil {
			return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Connector{
		Cfg:      cfg,
		JsClient: jsClient,
		ctx:      ctx,
		cancel:   cancel,
		Sub:      nil,
	}
}

func (c *Connector) Init() error {
	logger := c.JsClient.JetStreamLogger
	jsContext := c.JsClient.JetStreamContext

	if c.JsClient.NatsConnection == nil || c.JsClient.JetStreamContext == nil {
		if err := c.JsClient.InitNATS(); err != nil {
			return err
		}
	}

	// Create output streams
	for _, sub := range c.Cfg.OutputSubscribers {
		streamName := "STREAM_" + strings.ToUpper(strings.ReplaceAll(sub, ".", "_"))

		cfg := nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{sub},
			Storage:  nats.FileStorage,
		}

		_, err := jsContext.AddStream(&cfg)
		if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			logger.Warn("Failed to create output stream", "subject", sub, "error", err)
		}
	}

	// Set up durable consumer for input stream
	consumerCfg := &nats.ConsumerConfig{
		Durable:       c.Cfg.InputDurable,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 1000,
	}

	inputStreamName := c.Cfg.InputStreamName
	if _, err := jsContext.AddConsumer(inputStreamName, consumerCfg); err != nil {
		logger.Warn("Consumer may already exist", "error", err)
	}

	inputSubject := c.Cfg.InputSubject
	inputDurable := c.Cfg.InputDurable

	sub, err := jsContext.PullSubscribe(inputSubject, inputDurable)
	if err != nil {
		return err
	}

	c.Sub = sub

	logger.Info("Connector initialized successfully")

	return nil
}

func (c *Connector) Run() {
	logger := c.JsClient.JetStreamLogger

	for {
		select {
		case <-c.ctx.Done():
			logger.Info("Connector shutting down")
			return
		default:
		}

		msgs, err := c.Sub.Fetch(BatchSize, nats.MaxWait(2*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}

			logger.Error("Error fetching messages", "error", err)

			continue
		}

		for _, msg := range msgs {
			c.handleMessage(msg)
		}
	}
}

func (c *Connector) handleMessage(msg *nats.Msg) {
	var event jetstream.Event

	if err := json.Unmarshal(msg.Data, &event); err != nil {
		_ = msg.Nak()
		return
	}

	jsContext := c.JsClient.JetStreamContext
	logger := c.JsClient.JetStreamLogger

	outBytes, err := json.Marshal(event)
	if err != nil {
		logger.Error("Error marshalling event", "error", err)
	}

	for _, sub := range event.Subscribers {
		if _, err := jsContext.Publish(sub, outBytes); err != nil {
			_ = msg.Nak()

			logger.Error("Failed to publish to subscriber", "sub", sub, "error", err)

			return
		}

		logger.Debug("Forwarded event", "subscriber", sub)
	}

	_ = msg.Ack()
}

func (c *Connector) Stop() {
	c.cancel()

	if c.Sub != nil {
		_ = c.Sub.Unsubscribe()
	}

	_ = c.JsClient.Close()
}
