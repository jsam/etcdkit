# etcstruct

etcstruct is a comprehensive, etcd-based distributed systems toolkit implemented in Go. It provides a collection of data structures and patterns for building robust, distributed applications based on etcd.

## Features

### Implemented
- Publish-Subscribe (Pub/Sub) System
- Stack
- Queue
- Graph
- Event Bus

### Upcoming
- Distributed Lock
- Leader Election
- Distributed Counter
- Distributed Configuration
- Rate Limiter
- Job Queue
- Workflow Engine

## Installation

```bash
go get github.com/jsam/etcstruct
```

# Usage

Here's a quick example using the Pub/Sub system:
```
import "github.com/jsam/etcstruct/pkg"

// Create EventBus
eb, _ := etcdbus.NewEventBus([]string{"localhost:2379"}, "/pubsub")
defer eb.Close()

// Publish
event := etcdbus.NewEvent("topic", []byte("Hello, World!"))
eb.Publish(context.Background(), event)

// Subscribe
sub, _ := etcdbus.NewSubscriber(eb, "topic")
sub.Start()
event, _ := sub.Receive(context.Background())
fmt.Printf("Received: %s\n", string(event.Data))
```

For usage of other data structures and patterns, please refer to the documentation.

## Requirements

Go 1.15+
etcd 3.0+