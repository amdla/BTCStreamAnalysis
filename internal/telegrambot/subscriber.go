package telegrambot

import (
	"app/internal/jetstream"
	"app/internal/models"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/nats-io/nats.go"
)

func (b *TelegramBot) HandleMessage(msg *nats.Msg) error {
	logger := b.TelegramBotLogger

	var event jetstream.Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.Error("Failed to unmarshal event", slog.Any("error", err))

		_ = msg.Nak()

		return err
	}

	// Convert event.EventData to []byte
	eventBytes, err := json.Marshal(event.EventData)
	if err != nil {
		logger.Error("Failed to marshal event data", slog.Any("error", err))

		_ = msg.Nak()

		return err
	}

	// Unmarshal into BinanceTradeData
	var trade models.BinanceTradeData
	if err := json.Unmarshal(eventBytes, &trade); err != nil {
		logger.Error("Failed to parse BinanceTradeData", slog.Any("error", err))

		_ = msg.Nak()

		return err
	}

	if !ValidateTrade(trade) {
		// nothing to do
		_ = msg.Ack()

		return nil
	}

	b.TelegramBotLogger.Info("Trade passed filters", slog.String("symbol", trade.Symbol), slog.Float64("price", trade.Price))

	totalPrice := trade.Price * trade.Quantity

	formattedTotalPrice := formatTotalPrice(totalPrice)

	message := fmt.Sprintf(
		"🚨 *Trade Alert!*\nSymbol: %s\nPrice: %f\nQty: %f\nTotalPrice: %s",
		trade.Symbol, trade.Price, trade.Quantity, formattedTotalPrice)

	if err := b.sendTelegramMessage(message); err != nil {
		logger.Error("Failed to send Telegram message", slog.Any("error", err))

		_ = msg.Nak()

		return err
	}

	logger.Info("Sent Telegram alert", slog.String("symbol", trade.Symbol), slog.Float64("price", trade.Price))

	_ = msg.Ack()

	err = b.publishNotification(trade)
	if err != nil {
		logger.Error("Failed to publish notification", slog.Any("error", err))
	}

	return nil
}

func (b *TelegramBot) sendTelegramMessage(text string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", b.Config.BotToken)

	data := url.Values{}
	data.Set("chat_id", b.Config.ChatID)
	data.Set("text", text)
	data.Set("parse_mode", "Markdown")

	resp, err := http.PostForm(apiURL, data)

	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("telegram API returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
