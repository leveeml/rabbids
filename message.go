package rabbids

import (
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Message is an ampq.Delivery with some helper methods used by our systems
type Message struct {
	amqp.Delivery
}

// Publishing is
type Publishing struct {
	Exchange string
	Key      string
	options  []PublishingOption
	amqp.Publishing
}

type PublishingError struct {
	Publishing
	Err error
}

func NewPublishing(exchange, key string, options ...PublishingOption) Publishing {
	id, err := uuid.NewRandom()
	if err != nil {
		// TODO add log?
		id = uuid.Must(uuid.NewUUID())
	}

	return Publishing{
		Exchange: exchange,
		Key:      key,
		Publishing: amqp.Publishing{
			MessageId: id.String(),
			Priority:  0,
			Headers:   amqp.Table{},
		},
		options: options,
	}
}

// MessageHandler is the base interface used to consumer AMPQ messages.
type MessageHandler interface {
	Handle(m Message)
	Close()
}

// MessageHandlerFunc implements the MessageHandler interface
type MessageHandlerFunc func(m Message)

func (h MessageHandlerFunc) Handle(m Message) {
	h(m)
}

func (h MessageHandlerFunc) Close() {}
