package mongo

import (
	"app/internal/repository"
	"log/slog"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
)

type Client struct {
	MongoConfig      Config
	MongoLogger      *slog.Logger
	Client           *mongo.Client
	BinanceTradeRepo repository.BinanceTradeRepository
	NotificationRepo repository.NotificationRepository
}

func NewMongoClient() *Client {
	cfg := InitializeMongoConfig()
	logger := InitializeMongoLogger(cfg.IsDebugMode)

	return &Client{
		MongoConfig: *cfg,
		MongoLogger: logger,
		Client:      nil,
	}
}

func InitializeMongoLogger(debug bool) *slog.Logger {
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
