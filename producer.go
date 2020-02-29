package rabbids

import (
	"fmt"
	"sync"
	"time"

	retry "github.com/rafaeljesus/retry-go"
	"github.com/streadway/amqp"
)

type Producer struct {
	sync.RWMutex
	Conf    Connection
	conn    *amqp.Connection
	ch      *amqp.Channel
	emit    chan Publishing
	emitErr chan PublishingError
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
	}
	err := p.startConnection()

	return p, err
}

func NewProducerFromConfig(c Connection) (*Producer, error) {
	p := &Producer{
		Conf:    c,
		emit:    make(chan Publishing, 250),
		emitErr: make(chan PublishingError, 250),
	}
	err := p.startConnection()

	return p, err
}

func (p *Producer) startConnection() error {
	conn, err := openConnection(p.Conf)
	if err != nil {
		return err
	}

	p.Lock()

	p.conn = conn
	p.ch, err = p.conn.Channel()

	p.Unlock()

	return err
}

func (p *Producer) Run() error {
	notifyClose := p.conn.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case err := <-notifyClose:
			if err == nil {
				return nil // graceful shutdown
			}

			p.handleAMPQClose(err)
			notifyClose = p.conn.NotifyClose(make(chan *amqp.Error))
		case pub, ok := <-p.emit:
			if !ok {
				return nil // graceful shutdown
			}

			p.send(pub)
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

func (p *Producer) send(m Publishing) {
	for _, op := range m.options {
		if err := op(&m); err != nil {
			p.tryToEmitErr(m, err)
			return
		}
	}

	p.RLock()
	err := retry.Do(func() error {
		return p.ch.Publish(m.Exchange, m.Key, false, false, m.Publishing)
	}, 5, 100*time.Microsecond)
	p.RUnlock()

	if err != nil {
		p.tryToEmitErr(m, err)
	}
}

func (p *Producer) tryToEmitErr(m Publishing, err error) {
	data := PublishingError{Publishing: m, Err: err}
	select {
	case p.emitErr <- data:
	default:
	}
}

func (p *Producer) Close() error {
	p.RLock()
	defer p.RUnlock()

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

func (p *Producer) GetAMPQChannel() *amqp.Channel {
	return p.ch
}

func (p *Producer) handleAMPQClose(err error) {
	for {
		connErr := p.startConnection()
		if connErr == nil {
			return
		}
		// TODO: Add log or hook for this type of error
		time.Sleep(time.Second)
	}
}
