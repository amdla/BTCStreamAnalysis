package jetstream

import (
	"log"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	NatsURL     string
	StreamName  string
	Subject     string
	DurableName string
	IsDebugMode bool
}

func InitializeJetStreamConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("NATS_URL", "nats://nats:4222")
	viper.SetDefault("JETSTREAM_STREAM_NAME", "EVENTS")
	viper.SetDefault("JETSTREAM_SUBJECT", "input.events")
	viper.SetDefault("JETSTREAM_DURABLE", "connector-durable")
	viper.SetDefault("JETSTREAM_DEBUG_MODE", false)

	cfg := &Config{
		NatsURL:     viper.GetString("NATS_URL"),
		StreamName:  viper.GetString("JETSTREAM_STREAM_NAME"),
		Subject:     viper.GetString("JETSTREAM_SUBJECT"),
		DurableName: viper.GetString("JETSTREAM_DURABLE"),
		IsDebugMode: viper.GetBool("JETSTREAM_DEBUG_MODE"),
	}

	if cfg.NatsURL == "" || cfg.StreamName == "" || cfg.Subject == "" {
		log.Fatalf("Incomplete JetStream config: %+v", cfg)
	}

	return cfg
}
