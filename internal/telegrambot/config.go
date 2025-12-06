package telegrambot

import (
	"log"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	BotToken    string
	ChatID      string
	Subject     string
	DurableName string
	StreamName  string
	IsDebugMode bool
}

func InitializeTelegramBotConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("TELEGRAM_BOT_TOKEN", "")
	viper.SetDefault("TELEGRAM_CHAT_ID", "")
	viper.SetDefault("TELEGRAM_SUBJECT", "consumer.telegrambot")
	viper.SetDefault("TELEGRAM_DURABLE_NAME", "telegram-bot-durable")
	viper.SetDefault("TELEGRAM_STREAM_NAME", "STREAM_CONSUMER_TELEGRAM_BOT")
	viper.SetDefault("TELEGRAM_DEBUG_MODE", false)

	cfg := &Config{
		BotToken:    viper.GetString("TELEGRAM_BOT_TOKEN"),
		ChatID:      viper.GetString("TELEGRAM_CHAT_ID"),
		Subject:     viper.GetString("TELEGRAM_SUBJECT"),
		DurableName: viper.GetString("TELEGRAM_DURABLE_NAME"),
		StreamName:  viper.GetString("TELEGRAM_STREAM_NAME"),
		IsDebugMode: viper.GetBool("TELEGRAM_DEBUG_MODE"),
	}

	if cfg.BotToken == "" || cfg.ChatID == "" {
		log.Fatalf("Incomplete Telegram bot config: token and chat id are required")
	}

	return cfg
}

func InitializeTelegramLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	logger.Info("Telegram logger initialized", slog.Bool("debug", debug))

	return logger
}
