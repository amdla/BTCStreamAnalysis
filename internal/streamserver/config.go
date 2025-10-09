package streamserver

import (
	"app/internal/jetstream"
	"app/internal/mongoclient"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type StreamServer struct {
	StreamServerConfig Config
	StreamServerLogger *slog.Logger
	MongoClient        *mongoclient.MongoClient
	JetStreamClient    *jetstream.Client
}

type Config struct {
	Symbols     []string
	IsDebugMode bool
}

func NewStreamServer() *StreamServer {
	config, err := InitializeStreamServerConfig()
	if err != nil {
		log.Fatalf("Failed to initialize Stream Server config: %v", err)
	}

	logger := InitializeStreamServerLogger(config)

	mongoClient := mongoclient.NewMongoClient()
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

func InitializeStreamServerConfig() (*Config, error) {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("STREAM_SERVER_DEBUG_MODE", false)
	viper.SetDefault("SYMBOLS", "BTCUSDT")

	isDebugMode := viper.GetBool("STREAM_SERVER_DEBUG_MODE")
	rawSymbols := viper.GetString("SYMBOLS")

	symbols := strings.Split(rawSymbols, ",")
	for i, s := range symbols {
		symbols[i] = strings.TrimSpace(s)
	}

	return &Config{
		Symbols:     symbols,
		IsDebugMode: isDebugMode,
	}, nil
}
