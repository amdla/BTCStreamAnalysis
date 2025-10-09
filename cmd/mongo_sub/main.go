package main

import (
	dataconnector "app/internal/data_connector"
	mongoclient "app/internal/mongo_client"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// Initialize clients
	jsClient := dataconnector.NewJetStreamClient()
	mongoClient := mongoclient.NewMongoClient()
	logger := mongoClient.MongoLogger

	if err := mongoClient.Connect(); err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)
		return
	}
	defer func(mongoClient *mongoclient.MongoClient) {
		err := mongoClient.Disconnect()
		if err != nil {

		}
	}(mongoClient)

	if err := jsClient.InitNATS(); err != nil {
		logger.Error("Failed to initialize NATS JetStream", "error", err)
		return
	}
	defer func(jsClient *dataconnector.JetStreamClient) {
		err := jsClient.Close()
		if err != nil {
			logger.Error("Failed to close NATS connection", "error", err)
		}
	}(jsClient)

	// Subscribe to target subject (e.g. consumer.alpha)
	subject := "consumer.mongo"
	logger.Info("Starting Mongo subscriber", "subject", subject)

	var subscription *nats.Subscription
	var err error
	for {
		subscription, err = jsClient.JetStreamContext.PullSubscribe("consumer.mongo", "mongo-subscriber-durable")
		if err == nil {
			break
		}

		if err.Error() == "nats: no stream matches subject" {
			logger.Warn("Stream not ready yet, retrying in 1s...")
			time.Sleep(1 * time.Second)
			continue
		}
		logger.Error("Failed to subscribe", "error", err)
		return
	}

	for {
		msgs, err := subscription.Fetch(10)
		if err != nil {
			continue
		}

		for _, msg := range msgs {
			mongoclient.HandleMessage(msg, mongoClient)
			err := msg.Ack()
			if err != nil {
				logger.Error("Failed to acknowledge message", "error", err)
				continue
			}
		}
	}
}
