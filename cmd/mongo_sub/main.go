package main

import (
	"app/internal/jetstream"
	"app/internal/mongo"
	"app/internal/utils"

	"github.com/nats-io/nats.go"
)

func main() {
	mongoClient := mongo.NewMongoClient()
	jsClient := jetstream.NewJetStreamClient()
	logger := mongoClient.MongoLogger

	if err := jsClient.InitNATS(); err != nil {
		logger.Error("Failed to initialize NATS JetStream", "error", err)

		return
	}
	defer jsClient.DeferJetStreamClose()

	if err := mongoClient.Connect(); err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)

		return
	}
	defer mongoClient.DeferMongoDisconnect()

	ctx := utils.WithGracefulShutdown(logger)

	subject := "consumer.mongo"
	durable := "mongo-subscriber-durable"

	logger.Info("Starting MongoClient subscriber", "subject", subject)

	handler := func(msg *nats.Msg) error {
		err := mongo.HandleMessage(msg, mongoClient)

		return err
	}

	err := jetstream.Subscribe(ctx, jsClient.JetStreamContext, logger, subject, durable, handler)
	if err != nil {
		logger.Error("Subscription failed", "error", err)
	}
}
