package main

import (
	"context"
	"fmt"
	"log"
	"time"

	etcdbus "github.com/jsam/etcdkit/pkg"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eb, err := etcdbus.NewEventBus([]string{"localhost:2379"}, "/pubsub")
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eb.Close()

	// Subscriber
	sub, err := etcdbus.NewSubscriber(eb, "sports/football")
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	sub.Start()
	defer sub.Close()

	// Add a small delay to ensure the subscriber is ready
	time.Sleep(time.Second)

	// Publisher
	go func() {
		for i := 0; i < 10; i++ {
			event := etcdbus.NewEvent("sports/football", []byte(fmt.Sprintf("Event %d", i)))
			err := eb.Publish(ctx, event)
			if err != nil {
				log.Printf("Failed to publish event: %v", err)
			} else {
				fmt.Printf("Published: Event %d\n", i)
			}
			time.Sleep(time.Second)
		}
	}()

	// Wait for all events to be published and potentially received
	time.Sleep(15 * time.Second)

	// Fetch history
	history, err := eb.GetHistory(ctx, "sports/football", 5)
	if err != nil {
		log.Printf("Failed to fetch history: %v", err)
	} else {
		for _, event := range history {
			fmt.Printf("Historical event: %s\n", string(event.Data))
		}
	}

	fmt.Println("Program completed")
}
