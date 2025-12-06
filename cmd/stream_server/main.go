package main

import "app/internal/streamserver"

func main() {
	srv := streamserver.NewStreamServer()
	if err := srv.MongoClient.Connect(); err != nil {
		srv.StreamServerLogger.Error("Server connection failed", "error", err)

		return
	}
	defer srv.MongoClient.DeferMongoDisconnect()

	srv.StreamData()
}
