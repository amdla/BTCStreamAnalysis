package telegrambot

import (
	"app/internal/jetstream"
	"app/internal/models"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

func (b *TelegramBot) publishNotification(trade models.BinanceTradeData) error {
	logger := b.TelegramBotLogger

	totalPrice := trade.Price * trade.Quantity

	notification := models.NotificationData{
		EventTime:    trade.EventTime.UTC().Format(time.RFC3339Nano),
		Price:        trade.Price,
		Quantity:     trade.Quantity,
		Symbol:       trade.Symbol,
		IsBuyerMaker: trade.IsBuyerMaker,
		TotalPrice:   totalPrice,
		TradeID:      trade.ID,
	}

	notificationEvent := jetstream.Event{
		ID:          uuid.NewString(),
		Subscribers: []string{"consumer.mongo"},
		Type:        "NotificationEvent",
		Source:      "TelegramBot",
		EventData:   notification,
		CreatedAt:   time.Now().UTC().String(),
	}

	eventBytes, _ := json.Marshal(notificationEvent)
	if _, err := b.JsClient.JetStreamContext.Publish("consumer.mongo", eventBytes); err != nil {
		logger.Error("Failed to publish notification event", slog.Any("error", err))
		return err
	}

	logger.Info("Notification event sent", "symbol", trade.Symbol)

	return nil
}
