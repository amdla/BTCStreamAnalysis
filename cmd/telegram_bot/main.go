package main

import (
	"app/internal/jetstream"
	"app/internal/telegrambot"
	"app/internal/utils"

	"github.com/nats-io/nats.go"
)

func main() {
	jsClient := jetstream.NewJetStreamClient()

	bot := telegrambot.NewTelegramBot(jsClient)
	defer bot.Stop()

	logger := bot.TelegramBotLogger

	cfg := bot.Config
	subject := cfg.Subject
	durable := cfg.DurableName

	logger.Info("Starting Telegram bot subscriber", "subject", subject)

	if err := jsClient.InitNATS(); err != nil {
		logger.Error("Failed to initialize NATS JetStream", "error", err)

		return
	}
	defer jsClient.DeferJetStreamClose()

	ctx := utils.WithGracefulShutdown(logger)

	handler := func(msg *nats.Msg) error {
		return bot.HandleMessage(msg)
	}

	err := jetstream.Subscribe(ctx, jsClient.JetStreamContext, logger, subject, durable, handler)
	if err != nil {
		logger.Error("Telegram bot subscription failed", "error", err)
	}
}
