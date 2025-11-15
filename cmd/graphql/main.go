package main

import (
	"app/internal/graphql"
	"log/slog"
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"

	"app/graph/generated"
	"app/graph/resolvers"
)

func main() {
	gqlClient := graphql.NewGqlClient()
	logger := gqlClient.Logger
	mongoClient := gqlClient.MongoClient

	if err := mongoClient.Connect(); err != nil {
		logger.Error("Failed to connect to MongoDB", slog.Any("error", err))
	}
	defer func() {
		if err := mongoClient.Disconnect(); err != nil {
			logger.Error("Failed to disconnect from MongoDB", slog.Any("error", err))
		}
	}()

	resolver := &resolvers.Resolver{
		MongoClient: mongoClient,
		Logger:      logger,
	}

	srv := handler.NewDefaultServer(generated.NewExecutableSchema(
		generated.Config{Resolvers: resolver},
	))

	http.Handle("/graphql", playground.Handler("GraphQL playground", "/query"))
	http.Handle("/query", srv)

	serverUrl := gqlClient.GraphqlConfig.ServerUrl
	logger.Info("Started GraphQL server at", slog.String("url", serverUrl))

	if err := http.ListenAndServe(serverUrl, nil); err != nil {
		logger.Error("HTTP server failed", slog.Any("error", err))
	}
}
