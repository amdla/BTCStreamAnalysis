package streamserver

import (
	"app/internal/jetstream"
	"app/internal/models"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type binanceTradeDataWrapper struct {
	Stream string `json:"stream"`
	Data   struct {
		EventType    string `json:"e"`
		EventTime    int64  `json:"E"`
		AggregateID  int64  `json:"a"`
		Symbol       string `json:"s"`
		Price        string `json:"p"`
		Quantity     string `json:"q"`
		FirstTradeID int64  `json:"f"`
		LastTradeID  int64  `json:"l"`
		TradeTime    int64  `json:"T"`
		IsBuyerMaker bool   `json:"m"`
	} `json:"data"`
}

func createDataObj(message []byte) models.BinanceTradeData {
	var raw binanceTradeDataWrapper

	if err := json.Unmarshal(message, &raw); err != nil {
		log.Printf("Failed to unmarshal raw data: %v", err)

		return models.BinanceTradeData{ID: uuid.New().String()}
	}

	price, err := strconv.ParseFloat(raw.Data.Price, 64)
	if err != nil {
		log.Printf("Failed to parse price: %v", err)
	}

	quantity, err := strconv.ParseFloat(raw.Data.Quantity, 64)
	if err != nil {
		log.Printf("Failed to parse quantity: %v", err)
	}

	return models.BinanceTradeData{
		ID:           uuid.New().String(),
		Stream:       raw.Stream,
		EventType:    raw.Data.EventType,
		EventTime:    time.UnixMilli(raw.Data.EventTime),
		AggregateID:  raw.Data.AggregateID,
		Symbol:       raw.Data.Symbol,
		Price:        price,
		Quantity:     quantity,
		FirstTradeID: raw.Data.FirstTradeID,
		LastTradeID:  raw.Data.LastTradeID,
		TradeTime:    time.UnixMilli(raw.Data.TradeTime),
		IsBuyerMaker: raw.Data.IsBuyerMaker,
	}
}

func PackObjToEvent(message []byte) (jetstream.Event, error) {
	binanceData := createDataObj(message)

	event := jetstream.Event{
		ID:          uuid.NewString(),
		Subscribers: []string{"consumer.mongo", "consumer.telegrambot"},
		Type:        "BinanceTradeEvent",
		Source:      "WebSocketStreamServer",
		EventData:   binanceData,
		CreatedAt:   time.Now().UTC().String(),
	}

	return event, nil
}
