package main

import "app/internal/data_connector"

func main() {
	jsClient := data_connector.NewJetStreamClient()
	connector := data_connector.NewConnector(jsClient)

	if err := connector.Init(); err != nil {
		jsClient.JetStreamLogger.Error("Failed to initialize connector", "error", err)
		connector.Stop()
		return
	}

	connector.Run()
}
