package graphql

import (
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	ServerUrl   string
	IsDebugMode bool
}

func InitializeGqlConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("GRAPHQL_DEBUG_MODE", false)
	viper.SetDefault("GRAPHQL_SERVER_URL", ":8085")

	cfg := &Config{
		ServerUrl:   viper.GetString("GRAPHQL_SERVER_URL"),
		IsDebugMode: viper.GetBool("GRAPHQL_DEBUG_MODE"),
	}

	return cfg
}
