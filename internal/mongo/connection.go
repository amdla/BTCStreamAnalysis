package mongo

import (
	"app/internal/repository"
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func (mc *Client) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := mc.MongoConfig

	uri := cfg.URI
	clientOptions := options.Client().ApplyURI(uri)

	if cfg.Username != "" && cfg.Password != "" {
		clientOptions.SetAuth(options.Credential{
			Username: cfg.Username,
			Password: cfg.Password,
		})
	}

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return err
	}

	mc.Client = client
	mc.MongoLogger.Info("MongoDB connected successfully", "uri", cfg.URI, "database", cfg.Database)

	mc.BinanceTradeRepo = repository.NewMongoBinanceTradeRepo(client, cfg.Database, cfg.BinanceTradeCollectionName)
	mc.NotificationRepo = repository.NewMongoNotificationRepo(client, cfg.Database, cfg.NotificationCollectionName)

	return nil
}

func (mc *Client) Disconnect() error {
	if mc.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		return mc.Client.Disconnect(ctx)
	}

	return nil
}

func (mc *Client) GetDatabase() *mongo.Database {
	return mc.Client.Database(mc.MongoConfig.Database)
}

func (mc *Client) GetCollection(collectionName string) *mongo.Collection {
	return mc.GetDatabase().Collection(collectionName)
}

func (mc *Client) DeferMongoDisconnect() func() {
	return func() {
		if mc != nil {
			if err := mc.Disconnect(); err != nil {
				mc.MongoLogger.Error("Failed to disconnect Client", "error", err)
			}
		}
	}
}
