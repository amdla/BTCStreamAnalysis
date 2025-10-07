package stream_server

import (
	"app/internal/mongo_client"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type StreamServer struct {
	StreamServerConfig StreamServerConfig
	StreamServerLogger *slog.Logger
	MongoClient        *mongo_client.MongoClient
}

type StreamServerConfig struct {
	Symbols     []string
	IsDebugMode bool
}

func NewStreamServer() *StreamServer {
	config, err := InitializeStreamServerConfig()
	if err != nil {
		log.Fatalf("Failed to initialize Stream Server config: %v", err)
	}
	logger := InitializeStreamServerLogger(config)

	mongoClient := mongo_client.NewMongoClient()

	return &StreamServer{
		StreamServerConfig: *config,
		StreamServerLogger: logger,
		MongoClient:        mongoClient,
	}
}

func InitializeStreamServerLogger(config *StreamServerConfig) *slog.Logger {
	var level slog.Level
	if config.IsDebugMode {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	slog.SetDefault(logger)

	log.SetOutput(os.Stderr)
	log.Printf("Stream Server starting - Debug: %v, Symbols: %v", config.IsDebugMode, config.Symbols)

	return logger
}

func InitializeStreamServerConfig() (*StreamServerConfig, error) {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("SYMBOLS", []string{"BTCUSDT"})
	viper.SetDefault("STREAM_SERVER_DEBUG_MODE", false)

	rawSymbols := viper.GetString("SYMBOLS")
	symbols := strings.Split(rawSymbols, ",")
	for i, s := range symbols {
		symbols[i] = strings.TrimSpace(s)
	}

	isDebugMode := viper.GetBool("STREAM_SERVER_DEBUG_MODE")

	return &StreamServerConfig{
		Symbols:     symbols,
		IsDebugMode: isDebugMode,
	}, nil
}
