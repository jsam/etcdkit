package etcdkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEvent(t *testing.T) {
	topic := "test_topic"
	data := []byte("test data")

	event := NewEvent(topic, data)

	assert.Equal(t, topic, event.Topic)
	assert.Equal(t, data, event.Data)
	assert.NotEmpty(t, event.ID)
	assert.NotZero(t, event.Timestamp)
	assert.NotNil(t, event.Headers)
}

func TestEventMarshalUnmarshal(t *testing.T) {
	originalEvent := &Event{
		ID:        "test_id",
		Topic:     "test_topic",
		Data:      []byte("test data"),
		Timestamp: time.Now().Round(time.Millisecond),
		Headers:   map[string]string{"key": "value"},
	}

	marshaled, err := originalEvent.Marshal()
	assert.NoError(t, err)
	assert.NotEmpty(t, marshaled)

	unmarshaledEvent, err := UnmarshalEvent(marshaled)
	assert.NoError(t, err)
	assert.NotNil(t, unmarshaledEvent)

	assert.Equal(t, originalEvent.ID, unmarshaledEvent.ID)
	assert.Equal(t, originalEvent.Topic, unmarshaledEvent.Topic)
	assert.Equal(t, originalEvent.Data, unmarshaledEvent.Data)
	assert.Equal(t, originalEvent.Timestamp.Unix(), unmarshaledEvent.Timestamp.Unix())
	assert.Equal(t, originalEvent.Headers, unmarshaledEvent.Headers)
}

func TestUnmarshalEventInvalidData(t *testing.T) {
	invalidData := []byte("invalid json")

	_, err := UnmarshalEvent(invalidData)
	assert.Error(t, err)
}

func TestGenerateID(t *testing.T) {
	id1 := generateID()
	time.Sleep(time.Microsecond) // Ensure a different timestamp
	id2 := generateID()

	assert.NotEqual(t, id1, id2)
	assert.Len(t, id1, 21) // Format: YYYYMMDDHHmmss.SSSSSS
	assert.Len(t, id2, 21)

	// Additional checks
	_, err1 := time.Parse("20060102150405.000000", id1)
	assert.NoError(t, err1)
	_, err2 := time.Parse("20060102150405.000000", id2)
	assert.NoError(t, err2)
}
