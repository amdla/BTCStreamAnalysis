package jetstream

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

const BatchSize = 10

func Subscribe(ctx context.Context, jsContext nats.JetStreamContext, logger *slog.Logger, subject, durable string, handler func(msg *nats.Msg) error) error {
	sub, err := waitForSubscription(ctx, jsContext, logger, subject, durable)
	if err != nil {
		return err
	}

	return processMessages(ctx, sub, logger, handler)
}

func waitForSubscription(ctx context.Context, jsContext nats.JetStreamContext, logger *slog.Logger, subject, durable string) (*nats.Subscription, error) {
	var sub *nats.Subscription

	var err error

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		sub, err = jsContext.PullSubscribe(subject, durable)
		if err != nil {
			if err.Error() == "nats: no stream matches subject" {
				logger.Warn("Stream not ready, retrying...", "subject", subject)

				time.Sleep(1 * time.Second)

				continue
			}

			return nil, err
		}

		logger.Info("Subscription ready", "subject", subject, "durable", durable)

		return sub, nil
	}
}

func processMessages(ctx context.Context, sub *nats.Subscription, logger *slog.Logger, handler func(msg *nats.Msg) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := sub.Fetch(BatchSize, nats.MaxWait(2*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}

			logger.Error("Failed to fetch messages", "error", err)

			continue
		}

		for _, msg := range msgs {
			if err := handler(msg); err != nil {
				_ = msg.Nak()

				logger.Warn("Failed to handle message", "error", err)

				continue
			}

			_ = msg.Ack()
		}
	}
}
