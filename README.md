# Rabbids

A library to create AMQP consumers and producers nicely.

[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE.md)
[![Go](https://github.com/EmpregoLigado/rabbids/workflows/Go/badge.svg?style=flat-square)](https://github.com/EmpregoLigado/rabbids/actions?query=workflow%3AGo)
[![Coverage Status](https://img.shields.io/codecov/c/github/empregoligado/rabbids/master.svg?style=flat-square)](https://codecov.io/gh/empregoligado/rabbids)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/leveeml/rabbids)
[![Go Report Card](https://goreportcard.com/badge/github.com/leveeml/rabbids?style=flat-square)](https://goreportcard.com/report/github.com/leveeml/rabbids)

- A wrapper over [amqp](https://github.com/streadway/amqp) to make possible declare all the blocks (exchanges, queues, dead-letters, bindings) from a YAML or struct.
- Handle connection problems
  - reconnect when a connection is lost or closed.
  - retry with exponential backoff for sending messages
- Go channel API for the producer (we are fans of github.com/rafaeljesus/rabbus API).
- Support for multiple connections.
- Delayed messages - send messages to arrive in the queue only after the time duration is passed.
- The consumer uses a handler approach, so it's possible to add middlewares wrapping the handler

## Installation

```bash
go get -u github.com/leveeml/rabbids
```

## Usage

We create some examples inside the `_example` directory.
To run the examples first you need a rabbitMQ server running.
If you didn't have a server already running you can run one with docker:

```sh
docker run -d -p 15672:15672 -p 5672:5672 rabbitmq:3-management
```

The examples expect an ENV var `RABBITMQ_ADDRESS` with the amqp address.

In one console tab run the consumer:

```sh
cd _examples
export RABBITMQ_ADDRESS=amqp://0.0.0.0:5672
go run consumer/main.go
```

In another tab run the producer:

```sh
cd _examples
export RABBITMQ_ADDRESS=amqp://0.0.0.0:5672
go run producer/main.go
```

Or send some delayed messages:

```sh
cd _examples
export RABBITMQ_ADDRESS=amqp://0.0.0.0:5672
go run delay-message/main.go
```

## Delayed Messages

The delayed message implementation is based on the implementation created by the NServiceBus project.
For more information go to the docs [here](https://docs.particular.net/transports/rabbitmq/delayed-delivery).

## MessageHandler

MessageHandler is an interface expected by a consumer to process the messages from rabbitMQ.
See the godocs for more details. If you don't need the close something you can use the `rabbids.MessageHandlerFunc` to pass a function as a MessageHandler.

## Concurency

Every consumer runs on a separated goroutine and by default process every message (call the MessageHandler) synchronously but it's possible to change that and process the messages with a pool of goroutines.
To make this you need to set the `worker` attribute inside the ConsumerConfig with the number of concurrent workers you need. [example](https://github.com/EmpregoLigado/rabbids/blob/master/_examples/rabbids.yaml#L29).
