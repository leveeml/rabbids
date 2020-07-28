package rabbids

import (
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Serializer is the base interface for all message serializers.
type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	// Name return the name used on the content type of the messsage
	Name() string
}

// Publishing have the fields for sending a message to rabbitMQ.
type Publishing struct {
	// Exchange name
	Exchange string
	// The routing key
	Key string
	// Data to be encoded inside the message
	Data interface{}
	// Delay is the duration to wait until the message is delivered to the queue.
	// The max delay period is 268,435,455 seconds, or about 8.5 years.
	Delay time.Duration

	options []PublishingOption
	amqp.Publishing
}

// PublishingError is returned by the async error reporting.
// When an async publishing message is sent and an error happens
// the Publishing and the error will be sent to the EmitErr channel.
// To get this channel, call the EmitErr method inside the producer.
type PublishingError struct {
	Publishing
	Err error
}

// NewPublishing create a message to be sent by some consumer.
func NewPublishing(exchange, key string, data interface{}, options ...PublishingOption) Publishing {
	id, err := uuid.NewRandom()
	if err != nil {
		id = uuid.Must(uuid.NewUUID())
	}

	return Publishing{
		Exchange: exchange,
		Key:      key,
		Data:     data,
		Publishing: amqp.Publishing{
			MessageId: id.String(),
			Priority:  0,
			Headers:   amqp.Table{},
		},
		options: options,
	}
}

// SendWithDelay send a message to arrive the queue only after the time is passed.
// The minimum delay is one second, if the delay is less than the minimum, the minimum will be used.
// The max delay period is 268,435,455 seconds, or about 8.5 years.
func NewDelayedPublishing(queue string, delay time.Duration, data interface{}, options ...PublishingOption) Publishing {
	if delay < time.Second {
		delay = time.Second
	}

	id, err := uuid.NewRandom()
	if err != nil {
		id = uuid.Must(uuid.NewUUID())
	}

	key, ex := calculateRoutingKey(delay, queue)

	return Publishing{
		Exchange: ex,
		Key:      key,
		Data:     data,
		Delay:    delay,
		Publishing: amqp.Publishing{
			Priority:  0,
			MessageId: id.String(),
			Headers:   amqp.Table{},
		},
		options: options,
	}
}

// Message is an ampq.Delivery with some helper methods used by our systems.
type Message struct {
	amqp.Delivery
}

// MessageHandler is the base interface used to consumer AMPQ messages.
type MessageHandler interface {
	// Handle a single message, this method MUST be safe for concurrent use
	Handle(m Message)
	// Close the handler, this method is called when the consumer is closing
	Close()
}

// MessageHandlerFunc implements the MessageHandler interface.
type MessageHandlerFunc func(m Message)

func (h MessageHandlerFunc) Handle(m Message) {
	h(m)
}

func (h MessageHandlerFunc) Close() {}
