package streamserver

import (
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Symbols     []string
	IsDebugMode bool
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
