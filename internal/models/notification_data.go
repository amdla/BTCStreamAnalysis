package models

type NotificationData struct {
	EventTime    string  `bson:"EventTime" json:"EventTime"`
	Price        float64 `bson:"Price" json:"Price"`
	Quantity     float64 `bson:"Quantity" json:"Quantity"`
	Symbol       string  `bson:"Symbol" json:"Symbol"`
	IsBuyerMaker bool    `bson:"IsBuyerMaker" json:"IsBuyerMaker"`
	TotalPrice   float64 `bson:"TotalPrice" json:"TotalPrice"`
	TradeID      string  `bson:"TradeID" json:"TradeID"`
}
