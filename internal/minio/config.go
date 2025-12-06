package minio

import (
	"github.com/spf13/viper"
)

type Config struct {
	IsDebugMode     bool
	Endpoint        string `mapstructure:"MINIO_ENDPOINT"`
	AccessKeyID     string `mapstructure:"MINIO_ACCESS_KEY_ID"`
	SecretAccessKey string `mapstructure:"MINIO_SECRET_ACCESS_KEY"`
}

func InitializeMinioConfig() *Config {
	viper.AutomaticEnv()
	viper.SetDefault("MINIO_DEBUG_MODE", false)
	viper.SetDefault("MINIO_ENDPOINT", "minio:9000")
	viper.SetDefault("MINIO_ACCESS_KEY_ID", "minioadmin")
	viper.SetDefault("MINIO_SECRET_ACCESS_KEY", "minioadmin")

	return &Config{
		IsDebugMode:     viper.GetBool("MINIO_DEBUG_MODE"),
		Endpoint:        viper.GetString("MINIO_ENDPOINT"),
		AccessKeyID:     viper.GetString("MINIO_ACCESS_KEY_ID"),
		SecretAccessKey: viper.GetString("MINIO_SECRET_ACCESS_KEY"),
	}
}
