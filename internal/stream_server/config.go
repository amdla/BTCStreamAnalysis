package stream_server

import (
	"app/internal/data_connector"
	"app/internal/mongo_client"
	"fmt"
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
	JetStreamClient    *data_connector.JetStreamClient
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
	jsClient := data_connector.NewJetStreamClient()

	return &StreamServer{
		StreamServerConfig: *config,
		StreamServerLogger: logger,
		MongoClient:        mongoClient,
		JetStreamClient:    jsClient,
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

	logger.Debug("Stream Server starting - Debug: %v Symbols: %v", config.IsDebugMode, config.Symbols)

	return logger
}

func InitializeStreamServerConfig() (*StreamServerConfig, error) {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("STREAM_SERVER_DEBUG_MODE", false)

	rawSymbols := viper.GetString("SYMBOLS")
	symbols := strings.Split(rawSymbols, ",")
	for i, s := range symbols {
		symbols[i] = strings.TrimSpace(s)
	}

	isDebugMode := viper.GetBool("STREAM_SERVER_DEBUG_MODE")

	if len(symbols) == 0 || (len(symbols) == 1 && symbols[0] == "") {
		return nil, fmt.Errorf("missing STREAM_SERVER_DEBUG_MODE env variable")
	}

	return &StreamServerConfig{
		Symbols:     symbols,
		IsDebugMode: isDebugMode,
	}, nil
}
