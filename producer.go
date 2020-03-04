package rabbids

import (
	"fmt"
	"sync"
	"time"

	retry "github.com/rafaeljesus/retry-go"
	"github.com/streadway/amqp"
)

type Producer struct {
	mutex       sync.RWMutex
	Conf        Connection
	conn        *amqp.Connection
	ch          *amqp.Channel
	emit        chan Publishing
	emitErr     chan PublishingError
	notifyClose chan *amqp.Error
	log         LoggerFN
}

func NewProducerFromDSN(dsn string) (*Producer, error) {
	p := &Producer{
		Conf: Connection{
			DSN:     dsn,
			Timeout: DefaultTimeout,
			Sleep:   DefaultSleep,
			Retries: DefaultRetries,
		},
		emit:    make(chan Publishing, 250),
		emitErr: make(chan PublishingError, 250),
		log:     NoOPLoggerFN,
	}
	err := p.startConnection()

	return p, err
}

// NewProducerFromConfig create a new producer passing
func NewProducerFromConfig(c Connection) (*Producer, error) {
	p := &Producer{
		Conf:    c,
		emit:    make(chan Publishing, 250),
		emitErr: make(chan PublishingError, 250),
		log:     NoOPLoggerFN,
	}
	err := p.startConnection()

	return p, err
}

// WithLogger will override the default logger (no Operation Log)
func (p *Producer) WithLogger(log LoggerFN) {
	p.log = log
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

// Run starts rabbids channels for emitting and listening for amqp connections closed
// returns when the producer is shutting down.
func (p *Producer) Run() {
	for {
		select {
		case err := <-p.notifyClose:
			if err == nil {
				return // graceful shutdown
			}

			p.handleAMPQClose(err)
		case pub, ok := <-p.emit:
			if !ok {
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
// It returns an error if the Publishing options returned an error OR the connection error persisted after the retries.
func (p *Producer) Send(m Publishing) error {
	for _, op := range m.options {
		if err := op(&m); err != nil {
			return err
		}
	}

	return retry.Do(func() error {
		p.mutex.RLock()
		err := p.ch.Publish(m.Exchange, m.Key, false, false, m.Publishing)
		p.mutex.RUnlock()

		return err
	}, 10, 10*time.Millisecond)
}

func (p *Producer) tryToEmitErr(m Publishing, err error) {
	data := PublishingError{Publishing: m, Err: err}
	select {
	case p.emitErr <- data:
	default:
	}
}

// Close will close all the underline channels and close the connection with rabbitMQ.
// Any Emit call after calling the Close method will panic.
func (p *Producer) Close() error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.ch != nil && p.conn != nil && !p.conn.IsClosed() {
		if err := p.ch.Close(); err != nil {
			return fmt.Errorf("error closing the channel: %w", err)
		}

		if err := p.conn.Close(); err != nil {
			return fmt.Errorf("error closing the connection: %w", err)
		}
	}

	close(p.emit)
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
