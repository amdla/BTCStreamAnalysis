package graphql

import (
	"app/internal/mongo"
	"log/slog"
	"os"
)

type Client struct {
	GraphqlConfig Config
	MongoClient   *mongo.Client
	Logger        *slog.Logger
}

func NewGqlClient() *Client {
	cfg := InitializeGqlConfig()
	logger := InitializeGqlLogger(cfg.IsDebugMode)
	mongoClient := mongo.NewMongoClient()

	return &Client{
		GraphqlConfig: *cfg,
		Logger:        logger,
		MongoClient:   mongoClient,
	}
}

func InitializeGqlLogger(debug bool) *slog.Logger {
	var level slog.Level
	if debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	return logger
}
