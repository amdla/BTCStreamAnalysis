package repository

import (
	"app/internal/models"
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BinanceTradeFilter struct {
	ID           *string
	Stream       *string
	EventType    *string
	AggregateID  *int64
	Symbol       *string
	FirstTradeID *int64
	LastTradeID  *int64
	IsBuyerMaker *bool

	Price    *float64
	PriceGT  *float64
	PriceGTE *float64
	PriceLT  *float64
	PriceLTE *float64
	PriceNE  *float64

	Quantity    *float64
	QuantityGT  *float64
	QuantityGTE *float64
	QuantityLT  *float64
	QuantityLTE *float64
	QuantityNE  *float64

	EventTimeAfter  *time.Time
	EventTimeBefore *time.Time
	TradeTimeAfter  *time.Time
	TradeTimeBefore *time.Time

	Regex *string
	Limit *int64

	SortField1 *string
	SortDir1   *SortDirection
	SortField2 *string
	SortDir2   *SortDirection
}

type BinanceTradeRepository interface {
	Find(ctx context.Context, filter *BinanceTradeFilter) ([]models.BinanceTradeData, error)
}

type MongoBinanceTradeRepo struct {
	collection *mongo.Collection
}

func NewMongoBinanceTradeRepo(client *mongo.Client, dbName, collectionName string) *MongoBinanceTradeRepo {
	return &MongoBinanceTradeRepo{
		collection: client.Database(dbName).Collection(collectionName),
	}
}

func (r *MongoBinanceTradeRepo) Find(ctx context.Context, filter *BinanceTradeFilter) ([]models.BinanceTradeData, error) {
	query, findOptions := buildBinanceQuery(filter)

	findOptions.SetProjection(bson.M{"eventData": 1, "_id": 0})

	cursor, err := r.collection.Find(ctx, query, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []struct {
		EventData models.BinanceTradeData `bson:"eventData"`
	}

	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	trades := make([]models.BinanceTradeData, len(results))
	for i, res := range results {
		trades[i] = res.EventData
	}

	return trades, nil
}

func buildBinanceQuery(filter *BinanceTradeFilter) (bson.M, *options.FindOptions) {
	query := bson.M{}
	findOptions := options.Find()

	const fieldPrefix = "eventData."

	if filter.ID != nil {
		query[fieldPrefix+"ID"] = *filter.ID
	}

	if filter.Stream != nil {
		query[fieldPrefix+"Stream"] = *filter.Stream
	}

	if filter.EventType != nil {
		query[fieldPrefix+"EventType"] = *filter.EventType
	}

	if filter.AggregateID != nil {
		query[fieldPrefix+"AggregateID"] = *filter.AggregateID
	}

	if filter.Symbol != nil {
		query[fieldPrefix+"Symbol"] = *filter.Symbol
	}

	if filter.FirstTradeID != nil {
		query[fieldPrefix+"FirstTradeID"] = *filter.FirstTradeID
	}

	if filter.LastTradeID != nil {
		query[fieldPrefix+"LastTradeID"] = *filter.LastTradeID
	}

	if filter.IsBuyerMaker != nil {
		query[fieldPrefix+"IsBuyerMaker"] = *filter.IsBuyerMaker
	}

	if priceFilter := buildNumericRange(
		filter.Price, filter.PriceGT, filter.PriceGTE,
		filter.PriceLT, filter.PriceLTE, filter.PriceNE,
	); len(priceFilter) > 0 {
		query[fieldPrefix+"Price"] = priceFilter
	}

	if qtyFilter := buildNumericRange(
		filter.Quantity, filter.QuantityGT, filter.QuantityGTE,
		filter.QuantityLT, filter.QuantityLTE, filter.QuantityNE,
	); len(qtyFilter) > 0 {
		query[fieldPrefix+"Quantity"] = qtyFilter
	}

	if eventTimeFilter := buildTimeRange(filter.EventTimeAfter, filter.EventTimeBefore); len(eventTimeFilter) > 0 {
		query[fieldPrefix+"EventTime"] = eventTimeFilter
	}

	if tradeTimeFilter := buildTimeRange(filter.TradeTimeAfter, filter.TradeTimeBefore); len(tradeTimeFilter) > 0 {
		query[fieldPrefix+"TradeTime"] = tradeTimeFilter
	}

	if filter.Regex != nil {
		regexQuery := bson.M{"$regex": *filter.Regex, "$options": "i"}

		query["$or"] = []bson.M{
			{fieldPrefix + "Stream": regexQuery},
			{fieldPrefix + "EventType": regexQuery},
			{fieldPrefix + "Symbol": regexQuery},
		}
	}

	if filter.Limit != nil {
		findOptions.SetLimit(*filter.Limit)
	}

	if sortDoc := buildSortDocument(fieldPrefix, filter.SortField1, filter.SortField2, filter.SortDir1, filter.SortDir2); len(sortDoc) > 0 {
		findOptions.SetSort(sortDoc)
	}

	return query, findOptions
}
