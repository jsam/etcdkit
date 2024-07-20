package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jsam/etcdbus"
)

func main() {
	// Create a new EventBus
	eb, err := etcdbus.NewEventBus([]string{"localhost:2379"}, "/events")
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eb.Close()

	// Subscribe to a topic
	ch, err := eb.Subscribe("test-topic")
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Start a goroutine to receive events
	go func() {
		for event := range ch {
			fmt.Printf("Received event: %+v\n", event)
		}
	}()

	// Publish events
	for i := 0; i < 5; i++ {
		event := etcdbus.NewEvent("test-topic", []byte(fmt.Sprintf("Hello, World! %d", i)))
		err := eb.Publish(context.Background(), event)
		if err != nil {
			log.Printf("Failed to publish event: %v", err)
		}
		time.Sleep(time.Second)
	}

	// Wait for events to be processed
	time.Sleep(5 * time.Second)
}
