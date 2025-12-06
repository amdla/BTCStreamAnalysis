package models

import "time"

type BinanceTradeData struct {
	ID           string    `bson:"ID" json:"ID"`
	Stream       string    `bson:"Stream" json:"Stream"`
	EventType    string    `bson:"EventType" json:"EventType"`
	EventTime    time.Time `bson:"EventTime" json:"EventTime"`
	AggregateID  int64     `bson:"AggregateID" json:"AggregateID"`
	Symbol       string    `bson:"Symbol" json:"Symbol"`
	Price        float64   `bson:"Price" json:"Price"`
	Quantity     float64   `bson:"Quantity" json:"Quantity"`
	FirstTradeID int64     `bson:"FirstTradeID" json:"FirstTradeID"`
	LastTradeID  int64     `bson:"LastTradeID" json:"LastTradeID"`
	TradeTime    time.Time `bson:"TradeTime" json:"TradeTime"`
	IsBuyerMaker bool      `bson:"IsBuyerMaker" json:"IsBuyerMaker"`
}

type EnrichedTrade struct {
	BinanceTradeData
	TriggeredNotification bool `json:"triggeredNotification"`
}

type ProcessedTrade struct {
	TradeTime             time.Time `json:"TradeTime"`
	Symbol                string    `json:"Symbol"`
	Price                 float64   `json:"Price"`
	Quantity              float64   `json:"Quantity"`
	AmountOfTrades        int64     `json:"AmountOfTrades"`
	IsBuyerMaker          bool      `json:"IsBuyerMaker"`
	TriggeredNotification bool      `json:"triggeredNotification"`
}

func (e EnrichedTrade) ToProcessedTrade() ProcessedTrade {
	amount := e.LastTradeID - e.FirstTradeID + 1
	if amount < 0 {
		amount = 0
	}

	return ProcessedTrade{
		TradeTime:             e.TradeTime,
		Symbol:                e.Symbol,
		Price:                 e.Price,
		Quantity:              e.Quantity,
		AmountOfTrades:        amount,
		IsBuyerMaker:          e.IsBuyerMaker,
		TriggeredNotification: e.TriggeredNotification,
	}
}
