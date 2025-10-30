package mongoclient

import (
	"app/internal/jetstream"
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
)

func HandleMessage(msg *nats.Msg, mongoClient *MongoClient) error {
	logger := mongoClient.MongoLogger

	var event jetstream.Event

	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.Error("Failed to unmarshal event", "error", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := bson.M{
		"event_id":   event.ID,
		"subscriber": event.Subscribers,
		"type":       event.Type,
		"source":     event.Source,
		"eventData":  event.EventData,
		"createdAt":  event.CreatedAt,
	}

	collection := mongoClient.GetCollection(event.Type)

	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		logger.Error("Failed to insert event into MongoDB", "error", err)
		return err
	}

	logger.Info("✅ Event stored in MongoDB", "event_id", event.ID)

	return nil
}
