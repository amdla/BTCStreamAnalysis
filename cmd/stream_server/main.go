package main

import (
	eventstream "app/internal/data_connector"
	"app/internal/mongo_client"
	"app/internal/stream_server"
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	srv := stream_server.NewStreamServer()
	if err := srv.MongoClient.Connect(); err != nil {
		srv.StreamServerLogger.Error("Server connection failed", "error", err)
		return
	}

	testMongoConnection(srv.MongoClient, srv.StreamServerLogger)
	testNATSConnection(srv.JetStreamClient, srv.StreamServerLogger)
	srv.StreamData()
}

func testMongoConnection(mongoClient *mongo_client.MongoClient, serverLogger *slog.Logger) {
	serverLogger.Info("✅ Entering MongoDB connection test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collection := mongoClient.Client.Database("test_db").Collection("test_connection")

	testDoc := bson.M{
		"message":   "Hello MongoDB!",
		"timestamp": time.Now(),
		"purpose":   "Connection test",
	}

	_, err := collection.InsertOne(ctx, testDoc)
	if err != nil {
		serverLogger.Error("Failed to insert test document", "error", err)
		return
	}
	serverLogger.Info("✅ Test document inserted into MongoDB")
}

func testNATSConnection(jsClient *eventstream.JetStreamClient, serverLogger *slog.Logger) {
	serverLogger.Info("✅ Entering NATS JetStream connection test")
	cfg := jsClient.JetStreamConfig

	if err := jsClient.InitNATS(); err != nil {
		serverLogger.Error("NATS initialization failed", slog.Any("error", err))
		return
	}

	defer func(jsClient *eventstream.JetStreamClient) {
		err := jsClient.Close()
		if err != nil {
			serverLogger.Error("Failed to close NATS connection", "error", err)
		}
		serverLogger.Info("✅ NATS connection closed")
	}(jsClient)

	newEvent := eventstream.Event{
		ID: "test-event-1",
	}

	err := jsClient.SendEvent(cfg.Subject, newEvent)
	if err != nil {
		serverLogger.Error("Failed to send test event to NATS JetStream", "error", err)
		return
	}
}
