package streamserver

import (
	"app/internal/jetstream"
	"encoding/json"
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

func createDataObj(message []byte) BinanceTradeData {
	var raw binanceTradeDataWrapper
	if err := json.Unmarshal(message, &raw); err != nil {
		return BinanceTradeData{ID: uuid.New().String()}
	}

	return BinanceTradeData{
		ID:           uuid.New().String(),
		Stream:       raw.Stream,
		EventType:    raw.Data.EventType,
		EventTime:    time.UnixMilli(raw.Data.EventTime),
		AggregateID:  raw.Data.AggregateID,
		Symbol:       raw.Data.Symbol,
		Price:        raw.Data.Price,
		Quantity:     raw.Data.Quantity,
		FirstTradeID: raw.Data.FirstTradeID,
		LastTradeID:  raw.Data.LastTradeID,
		TradeTime:    time.UnixMilli(raw.Data.TradeTime),
		IsBuyerMaker: raw.Data.IsBuyerMaker,
	}
}

type BinanceTradeData struct {
	ID           string    `json:"id" bson:"id"`
	Stream       string    `json:"stream" bson:"stream"`
	EventType    string    `json:"eventType" bson:"eventType"`
	EventTime    time.Time `json:"eventTime" bson:"eventTime"`
	AggregateID  int64     `json:"aggregateTradeId" bson:"aggregateTradeId"`
	Symbol       string    `json:"symbol" bson:"symbol"`
	Price        string    `json:"price" bson:"price"`
	Quantity     string    `json:"quantity" bson:"quantity"`
	FirstTradeID int64     `json:"firstTradeId" bson:"firstTradeID"`
	LastTradeID  int64     `json:"lastTradeId" bson:"lastTradeID"`
	TradeTime    time.Time `json:"tradeTime" bson:"tradeTime"`
	IsBuyerMaker bool      `json:"isBuyerMaker" bson:"isBuyerMaker"`
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
