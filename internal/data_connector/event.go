package data_connector

type Event struct {
	ID         string    `json:"id" bson:"id"`
	Subscriber string    `json:"subscriber" bson:"subscriber"`
	Type       string    `json:"type" bson:"type"`
	Source     string    `json:"source" bson:"source"`
	Timestamp  string    `json:"timestamp" bson:"timestamp"`
	EventData  EventData `json:"eventData" bson:"eventData"`
}

type EventData struct {
	DataBefore map[string]interface{} `json:"dataBefore" bson:"dataBefore"`
	DataAfter  map[string]interface{} `json:"dataAfter" bson:"dataAfter"`
}
