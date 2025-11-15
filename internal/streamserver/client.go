package streamserver

import (
	"app/internal/jetstream"
	"app/internal/mongo"
	"log"
	"log/slog"
	"os"
)

type StreamServer struct {
	StreamServerConfig Config
	StreamServerLogger *slog.Logger
	MongoClient        *mongo.Client
	JetStreamClient    *jetstream.Client
}

func NewStreamServer() *StreamServer {
	config, err := InitializeStreamServerConfig()
	if err != nil {
		log.Fatalf("Failed to initialize Stream Server config: %v", err)
	}

	logger := InitializeStreamServerLogger(config)

	mongoClient := mongo.NewMongoClient()
	jsClient := jetstream.NewJetStreamClient()

	return &StreamServer{
		StreamServerConfig: *config,
		StreamServerLogger: logger,
		MongoClient:        mongoClient,
		JetStreamClient:    jsClient,
	}
}

func InitializeStreamServerLogger(config *Config) *slog.Logger {
	var level slog.Level
	if config.IsDebugMode {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	return logger
}
