package etl

import (
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	BatchSize     int
	RunInterval   time.Duration
	BucketName    string
	ObjectPrefix  string
	RawTradesPath string
	RawNotifsPath string
	CuratedPath   string
	AnalyticsPath string
	IsDebugMode   bool
}

func InitializeConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("ETL_BATCH_SIZE", 25)
	viper.SetDefault("ETL_RUN_INTERVAL_MINUTES", 60)
	viper.SetDefault("ETL_BUCKET_NAME", "binance-trades")
	viper.SetDefault("ETL_OBJECT_PREFIX", "trades")
	viper.SetDefault("ETL_DEBUG_MODE", false)
	viper.SetDefault("ETL_RAW_TRADES_PATH", "raw/trades")
	viper.SetDefault("ETL_RAW_NOTIFS_PATH", "raw/notifications")
	viper.SetDefault("ETL_CURATED_PATH", "curated")
	viper.SetDefault("ETL_ANALYTICS_PATH", "analytics")

	batchSize := viper.GetInt("ETL_BATCH_SIZE")
	if batchSize <= 0 {
		batchSize = 25
	}

	intervalMinutes := viper.GetInt("ETL_RUN_INTERVAL_MINUTES")
	if intervalMinutes <= 0 {
		intervalMinutes = 60
	}

	return &Config{
		BatchSize:     batchSize,
		RunInterval:   time.Duration(intervalMinutes) * time.Minute,
		BucketName:    viper.GetString("ETL_BUCKET_NAME"),
		ObjectPrefix:  viper.GetString("ETL_OBJECT_PREFIX"),
		RawTradesPath: viper.GetString("ETL_RAW_TRADES_PATH"),
		RawNotifsPath: viper.GetString("ETL_RAW_NOTIFS_PATH"),
		CuratedPath:   viper.GetString("ETL_CURATED_PATH"),
		AnalyticsPath: viper.GetString("ETL_ANALYTICS_PATH"),
		IsDebugMode:   viper.GetBool("ETL_DEBUG_MODE"),
	}
}
