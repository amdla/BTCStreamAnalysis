package resolvers

import (
	"app/internal/mongo"
	"log/slog"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	MongoClient *mongo.Client
	Logger      *slog.Logger
}
