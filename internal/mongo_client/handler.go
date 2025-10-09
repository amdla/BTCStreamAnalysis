package mongo_client

import (
	dataconnector "app/internal/data_connector"
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
)

func HandleMessage(msg *nats.Msg, mongoClient *MongoClient) {
	logger := mongoClient.MongoLogger
	var event dataconnector.Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.Error("Failed to unmarshal event", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := bson.M{
		"event_id":    event.ID,
		"subscriber":  event.Subscriber,
		"type":        event.Type,
		"source":      event.Source,
		"eventData":   event.EventData,
		"createdAt":   event.CreatedAt,
		"processedAt": time.Now().UTC().String(),
	}

	collection := mongoClient.GetCollection("events")
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		logger.Error("Failed to insert event into MongoDB", "error", err)
		return
	}

	logger.Info("✅ Event stored in MongoDB", "event_id", event.ID)
}
