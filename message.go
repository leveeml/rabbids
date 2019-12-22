package rabbids

import (
	"github.com/streadway/amqp"
)

// Message is and ampq.Delivery with some helper methods used by our systems
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
