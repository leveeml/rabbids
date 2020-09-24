package main

import (
	"log"
	"os"
	"time"

	"github.com/leveeml/rabbids"
)

type delayEvent struct {
	ID                 int       `json:"id,omitempty"`
	CreatedAt          time.Time `json:"created_at,omitempty"`
	ExpectedDeliveryAt time.Time `json:"expected_process_at,omitempty"`
}

func main() {
	// showing another way to create a new producer directly without the config and rabbids
	producer, err := rabbids.NewProducer(
		os.Getenv("RABBITMQ_ADDRESS"),
		rabbids.WithLogger(logRabbids),
	)
	if err != nil {
		log.Fatalf("failed to create the producer: %s", err)
	}

	for i := 1; i <= 10; i++ {
		duration := time.Minute * time.Duration(i)
		event := delayEvent{
			ID:                 i,
			CreatedAt:          time.Now(),
			ExpectedDeliveryAt: time.Now().Add(duration),
		}
		queue := "queue-consumer-example-1"
		err := producer.Send(rabbids.NewDelayedPublishing(queue, duration, event))
		if err != nil {
			log.Printf("failed to publish a message: %s", err)
			continue
		}
		log.Printf("send message with delay: ID: %d, deliveryAt: %s", event.ID, event.ExpectedDeliveryAt)
	}

	err = producer.Close()
	if err != nil {
		log.Printf("faield to close the producer: %s", err)
	}
}

func logRabbids(message string, fields rabbids.Fields) {
	format := "[rabbids] " + message + " fields: "
	values := []interface{}{}

	for k, v := range fields {
		format += "%s=%v "

		values = append(values, k, v)
	}

	log.Printf(format, values...)
}
