package jetstream

type Event struct {
	ID          string            `json:"id" bson:"id"`
	Subscriber  []string          `json:"subscriber" bson:"subscriber"`
	Type        string            `json:"type" bson:"type"`
	Source      string            `json:"source" bson:"source"`
	EventData   map[string]string `json:"eventData" bson:"eventData"`
	CreatedAt   string            `json:"createdAt" bson:"createdAt"`
	ProcessedAt string            `json:"processedAt" bson:"processedAt"`
}
