package main

import (
	"app/internal/dataconnector"
	"app/internal/jetstream"
)

func main() {
	jsClient := jetstream.NewJetStreamClient()
	connector := dataconnector.NewDataConnector(jsClient)
	logger := jsClient.JetStreamLogger

	if err := connector.Init(); err != nil {
		logger.Error("Failed to initialize connector", "error", err)
		connector.Stop()

		return
	}
	defer connector.Stop()

	connector.Run()
}
