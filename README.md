## PGQ consumers in GO

Package for writing PGQ consumers in Golang.

## Installation

    go get github.com/sasha-alias/pgq

## Description

There are two types of consumers you can create:

- pgq.Consumer - a single instance consumer
- pgq.CoopConsumer - a cooperative consumer, running several subconsumers in goroutines

In order to implement own event or batch processing you have to define the following interfaces:

```go
func MyEventHandler(event pgq.Event) error {}
func MyBatchHandler(event []pgq.Event) error {}
```

Then create an instance of appropriate consumer and pass your function to it via _EventHandler_ or _BatchHandler_ methods.

## Consumer


```go
package main

import (
    "github.com/sasha-alias/pgq"
    "log"
)

// Define own event handler
func ProcessEvent(event pgq.Event) error {
    log.Printf("%+v", event)
    return nil
}

func main() {
    consumer, _ := pgq.NewConsumer("consumer_name", "queue_name", "postgresql connect string")  // Create consumer
    consumer.EventHandler(ProcessEvent)  // Set the event handler you defined before
    consumer.Work()  // Start events processing
}
```


## Cooperative consumer


```go
package main

import (
    "github.com/sasha-alias/pgq"
    "log"
)

func ProcessEvent(event pgq.Event) error {
    log.Printf("%+v", event)
    return nil
}

func main() {
    consumer, _ := pgq.NewCoopConsumer("consumer_name", number_of_subconsumers, "queue_name", "postgresql connect string")
    consumer.EventHandler(ProcessEvent)
    consumer.Work()
}
```
