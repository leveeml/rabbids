package rabbids

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"time"
)

type Producer struct {
	Conf   Connection
	conn   *amqp.Connection
	ch     *amqp.Channel
	sendCH chan Publishing
}

func NewProducerFromDSN(dsn string) (*Producer, error) {
	p := &Producer{
		Conf: Connection{
			DSN:     dsn,
			Timeout: DefaultTimeout,
			Sleep:   DefaultSleep,
			Retries: DefaultRetries,
		},
		sendCH: make(chan Publishing, 250),
	}
	err := p.startConnection()

	return p, err
}

func NewProducerFromConfig(c Connection) (*Producer, error) {
	p := &Producer{
		Conf:   c,
		sendCH: make(chan Publishing, 250),
	}
	err := p.startConnection()

	return p, err
}

func (p *Producer) startConnection() error {
	conn, err := openConnection(p.Conf)
	if err != nil {
		return err
	}

	p.conn = conn
	p.ch, err = p.conn.Channel()

	return err
}

func (p *Producer) Run() error {
	notifyClose := p.conn.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case err := <-notifyClose:
			if err == nil {
				return nil //gracefull shutdown
			}
			p.handleAMPQClose(err)

			notifyClose = p.conn.NotifyClose(notifyClose)
		case pub, ok := <-p.sendCH:
			if !ok {
				return errors.New("unexpected close of send channel")
			}
			p.Send(context.TODO(), pub)
		}
	}
}

func (p *Producer) Send(ctx context.Context, m Publishing) error {
	return p.ch.Publish(m.Exchange, m.Key, false, false, m.Publishing)
}

func (p *Producer) SendAsync() chan<- Publishing {
	return p.sendCH
}

func (p *Producer) Close() error {
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			return err
		}
	}

	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			return err
		}
	}
	close(p.sendCH)

	return nil
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
