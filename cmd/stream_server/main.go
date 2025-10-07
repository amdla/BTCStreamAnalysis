package main

import (
	"app/internal/stream_server"
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	srv := stream_server.NewStreamServer()
	if err := srv.ConnectServer(); err != nil {
		srv.StreamServerLogger.Error("Server connection failed", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collection := srv.MongoClient.Client.Database("test_db").Collection("test_connection")

	testDoc := bson.M{
		"message":   "Hello MongoDB!",
		"timestamp": time.Now(),
		"purpose":   "Connection test",
	}

	_, err := collection.InsertOne(ctx, testDoc)
	if err != nil {
		srv.StreamServerLogger.Error("Failed to insert test document", "error", err)
		return
	}

	srv.StreamData()
}
