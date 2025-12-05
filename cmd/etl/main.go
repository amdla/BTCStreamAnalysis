package main

import (
	"app/internal/etl"
	"app/internal/minio"
	"app/internal/mongo"
	"app/internal/utils"
	"log/slog"
)

func main() {
	cfg := etl.InitializeConfig()
	logger := etl.InitializeLogger(cfg.IsDebugMode)

	mongoClient := mongo.NewMongoClient()
	if err := mongoClient.Connect(); err != nil {
		logger.Error("Failed to connect to MongoDB", slog.Any("error", err))

		return
	}
	defer mongoClient.DeferMongoDisconnect()

	minioClient := minio.NewMinioClient()
	if minioClient.MinioClient == nil {
		logger.Error("Failed to initialize MinIO client")

		return
	}

	service := etl.NewService(cfg, logger, mongoClient, minioClient)

	ctx := utils.WithGracefulShutdown(logger)
	if err := service.Run(ctx); err != nil {
		logger.Error("ETL service terminated with error", slog.Any("error", err))
	}
}
