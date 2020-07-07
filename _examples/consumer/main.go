package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/empregoligado/rabbids"
)

func main() {
	// Create the initial config from a yaml file
	config, err := rabbids.ConfigFromFile("rabbids.yaml")
	if err != nil {
		log.Fatalf("failed getting the rabbids config from file: %s", err)
	}

	// register all the handlers for all the consumers
	// The name MUST be equal to the name used inside the yaml or in the map index
	config.RegisterHandler("consumer-example-1", rabbids.MessageHandlerFunc(processExample1))

	rab, err := rabbids.New(config, logRabbids)
	if err != nil {
		log.Fatalf("failed to create the rabbids client: %s", err)
	}

	// start running the consumers
	stop, err := rabbids.StartSupervisor(rab, time.Second)
	if err != nil {
		log.Fatalf("failed to run the consumer supervisor: %s", err)
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("received an interrupt signal, shutting down the consumers")
	// shutdown the consumers
	stop()
}

func processExample1(m rabbids.Message) {
	log.Printf("[consumer-example-1] ID: %s, key: %s, body: %s", m.MessageId, m.RoutingKey, string(m.Body))

	m.Ack(false)
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
