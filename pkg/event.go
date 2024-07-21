package etcdkit

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Data      []byte            `json:"data"`
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers"`
}

func NewEvent(topic string, data []byte) *Event {
	return &Event{
		ID:        generateID(),
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
		Headers:   make(map[string]string),
	}
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func UnmarshalEvent(data []byte) (*Event, error) {
	var event Event
	err := json.Unmarshal(data, &event)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

func generateID() string {
	// Implement a unique ID generation method (e.g., UUID)
	// For simplicity, we'll use a timestamp-based ID here
	return time.Now().UTC().Format("20060102150405.000000")
}
