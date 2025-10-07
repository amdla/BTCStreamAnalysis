package main

import "app/internal/mongo_client"

func main() {
	mongoClient := mongo_client.NewMongoClient()
	logger := mongoClient.MongoLogger

	defer func() {
		if err := mongoClient.Disconnect(); err != nil {
			logger.Error("Failed to disconnect from MongoDB", "error", err)
		} else {
			logger.Info("MongoDB disconnected successfully")
		}
	}()

	logger.Info("Connecting to MongoDB", "uri", mongoClient.MongoConfig.URI)
	if err := mongoClient.Connect(); err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)
		return
	}
}
