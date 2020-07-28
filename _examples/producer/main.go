package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/empregoligado/rabbids"
)

type event struct {
	ID        int       `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	var wg sync.WaitGroup

	// Create the initial config from a yaml file
	config, err := rabbids.ConfigFromFilename("rabbids.yaml")
	if err != nil {
		log.Fatalf("failed getting the rabbids config from file: %s", err)
	}

	rab, err := rabbids.New(config, logRabbids)
	if err != nil {
		log.Fatalf("failed to create the rabbids client: %s", err)
	}

	producer, err := rab.CreateProducer("default")
	if err != nil {
		log.Fatalf("failed to create the producer: %s", err)
	}

	// goroutine to publish some messages
	tick := time.NewTicker(3 * time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		currentID := 0
		for t := range tick.C {
			currentID++
			key := "example.user.updated"
			if currentID%2 == 0 {
				key = "example.company.updated"
			}
			err := producer.Send(rabbids.NewPublishing(
				"events",
				key,
				event{ID: currentID, CreatedAt: t},
			))
			if err != nil {
				log.Printf("failed to publish a message: %s", err)
			}
		}
	}()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("received an interrupt signal, shutting down")
	tick.Stop()
	wg.Wait()
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
