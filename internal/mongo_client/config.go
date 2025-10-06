package mongo_client

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoClient struct {
	MongoConfig MongoConfig
	MongoLogger *slog.Logger
	Client      *mongo.Client
}

type MongoConfig struct {
	URI         string
	Database    string
	IsDebugMode bool
	Username    string
	Password    string
}

func NewMongoClient() *MongoClient {
	config, err := InitializeMongoConfig()
	if err != nil {
		log.Fatalf("Failed to initialize Mongo Client config: %v", err)
	}
	logger := InitializeMongoLogger(config)

	return &MongoClient{
		MongoConfig: *config,
		MongoLogger: logger,
		Client:      nil,
	}
}

func InitializeMongoLogger(config *MongoConfig) *slog.Logger {
	var level slog.Level
	if config.IsDebugMode {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	slog.SetDefault(logger)

	log.SetOutput(os.Stderr)
	log.Printf("Mongo Client starting - Debug: %v, URI: %s, Database name: %s",
		config.IsDebugMode, config.URI, config.Database)

	return logger
}

func InitializeMongoConfig() (*MongoConfig, error) {
	_ = godotenv.Load()

	viper.AutomaticEnv()

	viper.SetDefault("MONGO_URI", "fake_uri")
	viper.SetDefault("MONGO_DATABASE_NAME", "fake_db_name")
	viper.SetDefault("MONGO_DEBUG_MODE", false)
	viper.SetDefault("MONGO_USERNAME", "")
	viper.SetDefault("MONGO_PASSWORD", "")

	mongoURI := viper.GetString("MONGO_URI")
	mongoDatabaseName := viper.GetString("MONGO_DATABASE_NAME")
	isDebugMode := viper.GetBool("MONGO_DEBUG_MODE")
	username := viper.GetString("MONGO_INITDB_ROOT_USERNAME")
	password := viper.GetString("MONGO_INITDB_ROOT_PASSWORD")

	if mongoURI == "" || mongoDatabaseName == "" || username == "" || password == "" {
		fmt.Printf("MongoDB configuration is incomplete. URI: '%s', Database: '%s', Username: '%s', Password set: %s\n", mongoURI, mongoDatabaseName, username, password)
		return nil, fmt.Errorf("MongoDB configuration is incomplete, missing environment variables")
	}
	fmt.Printf("MongoDB configuration is complete. URI: '%s', Database: '%s', Username: '%s', Password set: %s\n", mongoURI, mongoDatabaseName, username, password)
	return &MongoConfig{
		URI:         mongoURI,
		Database:    mongoDatabaseName,
		IsDebugMode: isDebugMode,
		Username:    username,
		Password:    password,
	}, nil
}
