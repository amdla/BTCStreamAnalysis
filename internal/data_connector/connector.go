package data_connector

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

type Connector struct {
	jsClient         *JetStreamClient
	natsSubscription *nats.Subscription
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewConnector(jsClient *JetStreamClient) *Connector {
	ctx, cancel := context.WithCancel(context.Background())
	return &Connector{
		jsClient: jsClient,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (c *Connector) Init() error {
	logger := c.jsClient.JetStreamLogger
	logger.Info("Initializing Connector...")

	if c.jsClient.NatsConnection == nil || c.jsClient.JetStreamContext == nil {
		if err := c.jsClient.InitNATS(); err != nil {
			return err
		}
	}

	// Set up consumer and subscription
	subscription, err := c.setupSubscription()
	if err != nil {
		return err
	}
	c.natsSubscription = subscription

	return nil
}

func (c *Connector) setupSubscription() (*nats.Subscription, error) {
	cfg := c.jsClient.JetStreamConfig
	jsContext := c.jsClient.JetStreamContext

	consumerCfg := &nats.ConsumerConfig{
		Durable:       cfg.DurableName,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 1000,
	}

	// won't fail if already exists
	if _, err := jsContext.AddConsumer(cfg.StreamName, consumerCfg); err != nil {
		return nil, err
	}

	sub, err := jsContext.PullSubscribe(cfg.Subject, cfg.DurableName)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (c *Connector) Run() {
	logger := c.jsClient.JetStreamLogger
	logger.Info("Connector started")

	for {
		select {
		case <-c.ctx.Done():
			logger.Info("Connector shutting down...")
			return
		default:
		}

		msgs, err := c.natsSubscription.Fetch(10, nats.MaxWait(2*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			logger.Error("Error fetching messages", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, msg := range msgs {
			if err := c.handleMessage(msg); err != nil {
				logger.Warn("Failed to handle message", "error", err)
				_ = msg.Nak()
			}
		}
	}
}

func (c *Connector) handleMessage(msg *nats.Msg) error {
	var event Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return err
	}

	if event.Subscriber == "" {
		c.jsClient.JetStreamLogger.Warn("Empty subscriber field, skipping")
		return msg.Ack()
	}

	outBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	js := c.jsClient.JetStreamContext
	if _, err := js.Publish(event.Subscriber, outBytes); err != nil {
		return err
	}

	c.jsClient.JetStreamLogger.Info(" handled:",
		"subscriber_name", event.Subscriber,
		"target_subject", event.Subscriber,
		"size_bytes", len(outBytes),
	)

	return msg.Ack()
}

func (c *Connector) Stop() {
	c.cancel()
	if c.natsSubscription != nil {
		_ = c.natsSubscription.Unsubscribe()
	}
	c.jsClient.JetStreamLogger.Info("Connector stopped")
	_ = c.jsClient.Close()
}
