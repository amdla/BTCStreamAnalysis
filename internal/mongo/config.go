package mongo

import (
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	URI                        string
	Database                   string
	IsDebugMode                bool
	Username                   string
	Password                   string
	BinanceTradeCollectionName string
	NotificationCollectionName string
}

func InitializeMongoConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("MONGO_URI", "fake_uri")
	viper.SetDefault("MONGO_DATABASE_NAME", "fake_db_name")
	viper.SetDefault("MONGO_DEBUG_MODE", false)
	viper.SetDefault("MONGO_USERNAME", "")
	viper.SetDefault("MONGO_PASSWORD", "")
	viper.SetDefault("MONGO_BINANCE_TRADE_COLLECTION_NAME", "BinanceTradeEvent")
	viper.SetDefault("MONGO_NOTIFICATION_COLLECTION_NAME", "NotificationEvent")

	return &Config{
		URI:                        viper.GetString("MONGO_URI"),
		Database:                   viper.GetString("MONGO_DATABASE_NAME"),
		IsDebugMode:                viper.GetBool("MONGO_DEBUG_MODE"),
		Username:                   viper.GetString("MONGO_INITDB_ROOT_USERNAME"),
		Password:                   viper.GetString("MONGO_INITDB_ROOT_PASSWORD"),
		BinanceTradeCollectionName: viper.GetString("MONGO_BINANCE_TRADE_COLLECTION_NAME"),
		NotificationCollectionName: viper.GetString("MONGO_NOTIFICATION_COLLECTION_NAME"),
	}
}
