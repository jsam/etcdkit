package etcdbus

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

func NewEvent(topic string, data []byte) *Event {
	return &Event{
		ID:        generateID(),
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
	}
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func UnmarshalEvent(data []byte) (*Event, error) {
	var e Event
	err := json.Unmarshal(data, &e)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func generateID() string {
	// Implement a unique ID generation method (e.g., UUID)
	// For simplicity, we'll use a timestamp-based ID here
	return time.Now().Format("20060102150405.000")
}
