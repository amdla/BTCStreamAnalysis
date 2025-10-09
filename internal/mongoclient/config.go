package mongoclient

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoClient struct {
	MongoConfig Config
	MongoLogger *slog.Logger
	Client      *mongo.Client
}

type Config struct {
	URI         string
	Database    string
	IsDebugMode bool
	Username    string
	Password    string
}

func NewMongoClient() *MongoClient {
	cfg := InitializeMongoConfig()
	logger := InitializeMongoLogger(cfg.IsDebugMode)

	return &MongoClient{
		MongoConfig: *cfg,
		MongoLogger: logger,
		Client:      nil,
	}
}

func InitializeMongoLogger(debug bool) *slog.Logger {
	var level slog.Level
	if debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	return logger
}

func InitializeMongoConfig() *Config {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("MONGO_URI", "fake_uri")
	viper.SetDefault("MONGO_DATABASE_NAME", "fake_db_name")
	viper.SetDefault("MONGO_DEBUG_MODE", false)
	viper.SetDefault("MONGO_USERNAME", "")
	viper.SetDefault("MONGO_PASSWORD", "")

	return &Config{
		URI:         viper.GetString("MONGO_URI"),
		Database:    viper.GetString("MONGO_DATABASE_NAME"),
		IsDebugMode: viper.GetBool("MONGO_DEBUG_MODE"),
		Username:    viper.GetString("MONGO_INITDB_ROOT_USERNAME"),
		Password:    viper.GetString("MONGO_INITDB_ROOT_PASSWORD"),
	}
}
