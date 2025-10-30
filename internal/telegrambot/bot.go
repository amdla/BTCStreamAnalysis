package telegrambot

import (
	"app/internal/jetstream"
	"log/slog"

	"github.com/nats-io/nats.go"
)

type TelegramBot struct {
	Config            *Config
	TelegramBotLogger *slog.Logger
	JsClient          *jetstream.Client
	Sub               *nats.Subscription
	cancelCalled      bool
}

func NewTelegramBot(jsClient *jetstream.Client) *TelegramBot {
	cfg := InitializeTelegramBotConfig()
	logger := InitializeTelegramLogger(cfg.IsDebugMode)

	return &TelegramBot{
		Config:            cfg,
		TelegramBotLogger: logger,
		JsClient:          jsClient,
		Sub:               nil,
		cancelCalled:      false,
	}
}

func (b *TelegramBot) Stop() {
	b.cancelCalled = true

	if b.Sub != nil {
		_ = b.Sub.Unsubscribe()
		b.Sub = nil
	}
}
