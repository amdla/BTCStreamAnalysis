package data_connector

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
)

type JetStreamClient struct {
	JetStreamConfig  JetStreamConfig
	JetStreamLogger  *slog.Logger
	NatsConnection   *nats.Conn
	JetStreamContext nats.JetStreamContext
}

type JetStreamConfig struct {
	StreamName     string
	Subject        string
	NatsURL        string
	DurableName    string
	OutputSubjects []string
	IsDebugMode    bool
}

func NewJetStreamClient() *JetStreamClient {
	config, err := InitializeJetStreamConfig()
	if err != nil {
		log.Fatalf("Failed to initialize JetStream config: %v", err)
	}
	logger := InitializeJetStreamLogger(config)

	return &JetStreamClient{
		JetStreamConfig:  *config,
		JetStreamLogger:  logger,
		NatsConnection:   nil,
		JetStreamContext: nil,
	}
}

func InitializeJetStreamLogger(config *JetStreamConfig) *slog.Logger {
	var level slog.Level
	if config.IsDebugMode {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	logger.Info("Initialized JetStream: ",
		slog.Bool("DebugMode", config.IsDebugMode),
		slog.String("StreamName", config.StreamName),
		slog.String("Subject", config.Subject),
		slog.String("NatsURL", config.NatsURL),
	)

	return logger
}

func InitializeJetStreamConfig() (*JetStreamConfig, error) {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("NATS_URL", "nats://nats:4222")
	viper.SetDefault("INPUT_SUBJECT", "input.events")
	viper.SetDefault("STREAM_NAME", "EVENTS")
	viper.SetDefault("DURABLE_CONSUMER", "connector-durable")
	viper.SetDefault("OUTPUT_SUBJECTS", "consumer.alpha,consumer.beta")
	viper.SetDefault("JETSTREAM_DEBUG_MODE", false)

	natsURL := viper.GetString("NATS_URL")
	subject := viper.GetString("INPUT_SUBJECT")
	streamName := viper.GetString("STREAM_NAME")
	durableName := viper.GetString("DURABLE_CONSUMER")

	rawOutputSubjects := viper.GetString("OUTPUT_SUBJECTS")
	outputSubjects := strings.Split(rawOutputSubjects, ",")
	for i, s := range outputSubjects {
		outputSubjects[i] = strings.TrimSpace(s)
	}

	isDebugMode := viper.GetBool("JETSTREAM_DEBUG_MODE")

	if natsURL == "" || subject == "" || streamName == "" || durableName == "" || len(outputSubjects) == 0 {
		fmt.Printf("Configuration incomplete: NATS_URL='%s', INPUT_SUBJECT='%s', STREAM_NAME='%s',"+
			"DURABLE_CONSUMER='%s',OUTPUT_SUBJECTS='%v'\n", natsURL, subject, streamName, durableName, outputSubjects)
		return nil, fmt.Errorf("missing env variables")
	}

	return &JetStreamConfig{
		NatsURL:        natsURL,
		Subject:        subject,
		StreamName:     streamName,
		IsDebugMode:    isDebugMode,
		DurableName:    durableName,
		OutputSubjects: outputSubjects,
	}, nil
}
