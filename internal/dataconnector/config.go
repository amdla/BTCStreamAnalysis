package dataconnector

import (
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	InputStreamName   string
	InputSubject      string
	InputDurable      string
	OutputSubscribers []string
	DurableConsumer   string
}

func InitializeDataConnectorConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("CONNECTOR_DURABLE_CONSUMER", "data-connector")
	viper.SetDefault("CONNECTOR_INPUT_STREAM", "EVENTS")
	viper.SetDefault("CONNECTOR_INPUT_SUBJECT", "input.events")
	viper.SetDefault("CONNECTOR_OUTPUT_SUBSCRIBERS", "consumer.mongo,consumer.telegram.bot,consumer.stream.analytics")

	rawOutputs := viper.GetString("CONNECTOR_OUTPUT_SUBSCRIBERS")

	outputList := strings.Split(rawOutputs, ",")
	for i, s := range outputList {
		outputList[i] = strings.TrimSpace(s)
	}

	cfg := &Config{
		InputStreamName:   viper.GetString("CONNECTOR_INPUT_STREAM"),
		InputSubject:      viper.GetString("CONNECTOR_INPUT_SUBJECT"),
		InputDurable:      viper.GetString("CONNECTOR_DURABLE_CONSUMER"),
		OutputSubscribers: outputList,
		DurableConsumer:   viper.GetString("CONNECTOR_DURABLE_CONSUMER"),
	}

	return cfg
}
