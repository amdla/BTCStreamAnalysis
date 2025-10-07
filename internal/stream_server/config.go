package stream_server

import (
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type StreamServer struct {
	Config StreamServerConfig
	Logger slog.Logger
}

type StreamServerConfig struct {
	Symbols     []string
	IsDebugMode bool
}

func NewStreamServer() *StreamServer {
	config := InitializeStreamServerConfig()
	logger := InitializeLogger(config)

	return &StreamServer{
		Config: *config,
		Logger: *logger,
	}
}

func InitializeLogger(config *StreamServerConfig) *slog.Logger {
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

func InitializeStreamServerConfig() *StreamServerConfig {
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found: %v", err)
	}

	viper.AutomaticEnv()

	viper.SetDefault("SYMBOLS", []string{"BTCUSDT"})
	viper.SetDefault("IS_DEBUG_MODE", true)

	rawSymbols := viper.GetString("SYMBOLS")
	symbols := strings.Split(rawSymbols, ",")
	for i, s := range symbols {
		symbols[i] = strings.TrimSpace(s)
	}

	isDebugMode := viper.GetBool("IS_DEBUG_MODE")

	return &StreamServerConfig{
		Symbols:     symbols,
		IsDebugMode: isDebugMode,
	}
}
