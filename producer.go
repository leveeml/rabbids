package rabbids

import (
	"fmt"
	"sync"
	"time"

	"github.com/empregoligado/rabbids/serialization"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/streadway/amqp"
)

type Producer struct {
	mutex       sync.RWMutex
	Conf        Connection
	conn        *amqp.Connection
	ch          *amqp.Channel
	closed      chan struct{}
	emit        chan Publishing
	emitErr     chan PublishingError
	notifyClose chan *amqp.Error
	log         LoggerFN
	factory     *Factory
	serializer  Serializer
	exDeclared  map[string]struct{}
}

func NewProducer(dsn string, opts ...ProducerOption) (*Producer, error) {
	p := &Producer{
		Conf: Connection{
			DSN:     dsn,
			Retries: DefaultRetries,
			Sleep:   DefaultSleep,
			Timeout: DefaultTimeout,
		},
		emit:       make(chan Publishing, 250),
		emitErr:    make(chan PublishingError, 250),
		closed:     make(chan struct{}),
		log:        NoOPLoggerFN,
		serializer: &serialization.JSON{},
		exDeclared: make(map[string]struct{}),
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	err := p.startConnection()

	return p, err
}

// Run starts rabbids channels for emitting and listening for amqp connections closed
// returns when the producer is shutting down.
func (p *Producer) Run() {
	for {
		select {
		case err := <-p.notifyClose:
			if err == nil {
				return // graceful shutdown?
			}

			p.handleAMPQClose(err)
		case pub, ok := <-p.emit:
			if !ok {
				p.closed <- struct{}{}
				return // graceful shutdown
			}

			err := p.Send(pub)
			if err != nil {
				p.tryToEmitErr(pub, err)
			}
		}
	}
}

// Emit emits a message to rabbitMQ but does not wait for the response from the broker.
// Errors with the Publishing (encoding, validation) or with the broker will be sent to the EmitErr channel.
// It's your responsibility to handle these errors somehow.
func (p *Producer) Emit() chan<- Publishing { return p.emit }

// EmitErr returns a channel used to receive all the errors from Emit channel.
// The error handle is not required but and the send inside this channel is buffered.
// WARNING: If the channel gets full, new errors will be dropped to avoid stop the producer internal loop.
func (p *Producer) EmitErr() <-chan PublishingError { return p.emitErr }

// Send a message to rabbitMQ.
// In case of connection errors, the send will block and retry until the reconnection is done.
// It returns an error if the Serializer returned an error OR the connection error persisted after the retries.
func (p *Producer) Send(m Publishing) error {
	for _, op := range m.options {
		op(&m)
	}

	b, err := p.serializer.Marshal(m.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	m.Body = b
	m.ContentType = p.serializer.Name()

	return retry.Do(func() error {
		p.mutex.RLock()
		p.tryToDeclareTopic(m.Exchange)
		err := p.ch.Publish(m.Exchange, m.Key, false, false, m.Publishing)
		p.mutex.RUnlock()

		return err
	}, 10, 10*time.Millisecond)
}

// Close will close all the underline channels and close the connection with rabbitMQ.
// Any Emit call after calling the Close method will panic.
func (p *Producer) Close() error {
	close(p.emit)
	<-p.closed

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.ch != nil && p.conn != nil && !p.conn.IsClosed() {
		if err := p.ch.Close(); err != nil {
			return fmt.Errorf("error closing the channel: %w", err)
		}

		if err := p.conn.Close(); err != nil {
			return fmt.Errorf("error closing the connection: %w", err)
		}
	}

	close(p.emitErr)

	return nil
}

// GetAMQPChannel returns the current connection channel.
func (p *Producer) GetAMPQChannel() *amqp.Channel {
	return p.ch
}

func (p *Producer) handleAMPQClose(err error) {
	p.log("ampq connection closed", Fields{"error": err})

	for {
		connErr := p.startConnection()
		if connErr == nil {
			return
		}

		p.log("ampq reconnection failed", Fields{"error": connErr})
		time.Sleep(time.Second)
	}
}

func (p *Producer) startConnection() error {
	p.log("opening a new rabbitmq connection", Fields{})
	conn, err := openConnection(p.Conf)

	if err != nil {
		return err
	}

	p.mutex.Lock()

	p.conn = conn
	p.ch, err = p.conn.Channel()
	p.notifyClose = p.conn.NotifyClose(make(chan *amqp.Error))

	p.mutex.Unlock()

	return err
}

func (p *Producer) tryToEmitErr(m Publishing, err error) {
	data := PublishingError{Publishing: m, Err: err}
	select {
	case p.emitErr <- data:
	default:
	}
}

func (p *Producer) tryToDeclareTopic(ex string) {
	if p.factory == nil || p.factory.config == nil || ex == "" {
		return
	}

	if _, ok := p.exDeclared[ex]; !ok {
		err := p.factory.declareExchange(p.ch, ex)
		if err != nil {
			p.log("failed declaring a exchange", Fields{"err": err, "ex": ex})
			return
		}

		p.exDeclared[ex] = struct{}{}
	}
}
