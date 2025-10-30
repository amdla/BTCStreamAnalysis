package telegrambot

import (
	"app/internal/jetstream"
	"app/internal/streamserver"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type NotificationData struct {
	EventTime    string  `json:"eventTime"`
	Price        string  `json:"price"`
	Quantity     string  `json:"quantity"`
	Symbol       string  `json:"symbol"`
	IsBuyerMaker bool    `json:"isBuyerMaker"`
	TotalPrice   float64 `json:"totalPrice"`
}

func (b *TelegramBot) publishNotification(trade streamserver.BinanceTradeData) error {
	logger := b.TelegramBotLogger

	price, _ := strconv.ParseFloat(trade.Price, 64)
	quantity, _ := strconv.ParseFloat(trade.Quantity, 64)
	totalPrice := price * quantity

	notification := NotificationData{
		EventTime:    trade.EventTime.String(),
		Price:        trade.Price,
		Quantity:     trade.Quantity,
		Symbol:       trade.Symbol,
		IsBuyerMaker: trade.IsBuyerMaker,
		TotalPrice:   totalPrice,
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
