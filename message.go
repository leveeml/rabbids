package rabbids

import (
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

//Serializer is the base interface for all message serializers
type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	// Name return the name used on the content type of the messsage.
	Name() string
}

// Publishing have the fields for sending a message.
type Publishing struct {
	// Exchange name
	Exchange string
	// The routing key
	Key string
	// Data to be encoded inside the message
	Data interface{}

	options []PublishingOption
	amqp.Publishing
}

type PublishingError struct {
	Publishing
	Err error
}

func NewPublishing(exchange, key string, data interface{}, options ...PublishingOption) Publishing {
	id, err := uuid.NewRandom()
	if err != nil {
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

// Message is an ampq.Delivery with some helper methods used by our systems
type Message struct {
	amqp.Delivery
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
