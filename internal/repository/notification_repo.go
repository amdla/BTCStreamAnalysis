package repository

import (
	"app/internal/models"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NotificationFilter struct {
	EventTime    *string
	Symbol       *string
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

	TotalPrice    *float64
	TotalPriceGT  *float64
	TotalPriceGTE *float64
	TotalPriceLT  *float64
	TotalPriceLTE *float64
	TotalPriceNE  *float64

	Regex *string
	Limit *int64

	SortField1 *string
	SortDir1   *SortDirection
	SortField2 *string
	SortDir2   *SortDirection
}

type NotificationRepository interface {
	Find(ctx context.Context, filter *NotificationFilter) ([]models.NotificationData, error)
}

type MongoNotificationRepo struct {
	collection *mongo.Collection
}

func NewMongoNotificationRepo(client *mongo.Client, dbName, collectionName string) *MongoNotificationRepo {
	return &MongoNotificationRepo{
		collection: client.Database(dbName).Collection(collectionName),
	}
}

func (r *MongoNotificationRepo) Find(ctx context.Context, filter *NotificationFilter) ([]models.NotificationData, error) {
	query, findOptions := buildNotificationQuery(filter)

	findOptions.SetProjection(bson.M{"eventData": 1, "_id": 0})

	cursor, err := r.collection.Find(ctx, query, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []struct {
		EventData models.NotificationData `bson:"eventData"`
	}

	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	notifications := make([]models.NotificationData, len(results))
	for i, res := range results {
		notifications[i] = res.EventData
	}

	return notifications, nil
}

func buildNotificationQuery(filter *NotificationFilter) (bson.M, *options.FindOptions) {
	query := bson.M{}
	findOptions := options.Find()

	const fieldPrefix = "eventData."

	if filter.EventTime != nil {
		query[fieldPrefix+"EventTime"] = *filter.EventTime
	}

	if filter.Symbol != nil {
		query[fieldPrefix+"Symbol"] = *filter.Symbol
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

	if totalFilter := buildNumericRange(
		filter.TotalPrice, filter.TotalPriceGT, filter.TotalPriceGTE,
		filter.TotalPriceLT, filter.TotalPriceLTE, filter.TotalPriceNE,
	); len(totalFilter) > 0 {
		query[fieldPrefix+"TotalPrice"] = totalFilter
	}

	if filter.Regex != nil {
		regexQuery := bson.M{"$regex": *filter.Regex, "$options": "i"}

		query["$or"] = []bson.M{
			{fieldPrefix + "EventTime": regexQuery},
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
