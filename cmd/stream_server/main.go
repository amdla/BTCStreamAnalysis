package main

import (
	"app/internal/stream_server"
)

func main() {
	srv := stream_server.NewStreamServer()

	logger := srv.StreamServerLogger
	logger.Info("Stream Server is starting")

	//mongoClient := mongo_client.NewMongoClient()
	//err := mongoClient.Connect()
	//if err != nil {
	//	srv.StreamServerLogger.Error("Failed to connect to MongoDB", "error", err)
	//	return
	//}
	//logger.Info("Connected to MongoDB")

	srv.StreamData()
}
