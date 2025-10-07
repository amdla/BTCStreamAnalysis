package mongo_client

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func (mc *MongoClient) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := mc.MongoConfig.URI
	clientOptions := options.Client().ApplyURI(uri)

	if mc.MongoConfig.Username != "" && mc.MongoConfig.Password != "" {
		clientOptions.SetAuth(options.Credential{
			Username: mc.MongoConfig.Username,
			Password: mc.MongoConfig.Password,
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
	mc.MongoLogger.Info("MongoDB connected successfully", "uri", mc.MongoConfig.URI, "database", mc.MongoConfig.Database)
	return nil
}

func (mc *MongoClient) Disconnect() error {
	if mc.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return mc.Client.Disconnect(ctx)
	}
	return nil
}

func (mc *MongoClient) GetDatabase() *mongo.Database {
	return mc.Client.Database(mc.MongoConfig.Database)
}

func (mc *MongoClient) GetCollection(collectionName string) *mongo.Collection {
	return mc.GetDatabase().Collection(collectionName)
}
