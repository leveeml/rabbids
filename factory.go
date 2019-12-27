package rabbids

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"

	"gopkg.in/tomb.v2"

	"github.com/ivpusic/grpool"
	"github.com/pkg/errors"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/streadway/amqp"
)

// Factory is the block responsible for create consumers and restart the rabbitMQ connections.
type Factory struct {
	config *Config
	conns  map[string]*amqp.Connection
	log    LoggerFN
	number int64
}

// NewFactory will open the initial connections and start the recover connections procedure.
func NewFactory(config *Config, log LoggerFN) (*Factory, error) {
	setConfigDefaults(config)

	conns := make(map[string]*amqp.Connection)

	for name, cfgConn := range config.Connections {
		log("opening connection with rabbitMQ", Fields{
			"sleep":      cfgConn.Sleep,
			"timeout":    cfgConn.Timeout,
			"connection": name,
		})

		conn, err := openConnection(cfgConn)
		if err != nil {
			return nil, fmt.Errorf("error opening the connection \"%s\": %w", name, err)
		}

		conns[name] = conn
	}

	f := &Factory{
		config: config,
		conns:  conns,
		log:    log,
		number: 0,
	}

	return f, nil
}

// CreateConsumers will iterate over config and create all the consumers
func (f *Factory) CreateConsumers() ([]*Consumer, error) {
	var consumers []*Consumer

	for name, cfg := range f.config.Consumers {
		consumer, err := f.newConsumer(name, cfg)
		if err != nil {
			return consumers, err
		}

		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

// CreateConsumer create a new consumer for a specific name using the config provided.
func (f *Factory) CreateConsumer(name string) (*Consumer, error) {
	cfg, ok := f.config.Consumers[name]
	if !ok {
		return nil, fmt.Errorf("consumer \"%s\" did not exist", name)
	}

	return f.newConsumer(name, cfg)
}

func (f *Factory) newConsumer(name string, cfg ConsumerConfig) (*Consumer, error) {
	ch, err := f.getChannel(cfg.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to open the rabbitMQ channel for consumer %s: %w", name, err)
	}

	if len(cfg.DeadLetter) > 0 {
		err = f.declareDeadLetters(ch, cfg.DeadLetter)
		if err != nil {
			return nil, err
		}
	}

	err = f.declareQueue(ch, cfg.Queue)
	if err != nil {
		return nil, err
	}

	if err = ch.Qos(cfg.PrefetchCount, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	handler, ok := f.config.Handlers[name]
	if !ok {
		return nil, fmt.Errorf("failed to create the \"%s\" consumer, Handler not registered", name)
	}

	f.log("consumer created",
		Fields{
			"max-workers": cfg.Workers,
			"consumer":    name,
		})

	return &Consumer{
		queue:      cfg.Queue.Name,
		name:       name,
		number:     atomic.AddInt64(&f.number, 1),
		opts:       cfg.Options,
		channel:    ch,
		t:          tomb.Tomb{},
		handler:    handler,
		workerPool: grpool.NewPool(cfg.Workers, 0),
	}, nil
}

func (f *Factory) declareExchange(ch *amqp.Channel, name string) error {
	if len(name) == 0 {
		return fmt.Errorf("receive a blank exchange. Wrong config?")
	}

	ex, ok := f.config.Exchanges[name]
	if !ok {
		f.log("exchange config didn't exist, we will try to continue", Fields{"name": name})
		return nil
	}

	f.log("declaring exchange", Fields{
		"ex":      name,
		"type":    ex.Type,
		"options": ex.Options,
	})

	err := ch.ExchangeDeclare(
		name,
		ex.Type,
		ex.Options.Durable,
		ex.Options.AutoDelete,
		ex.Options.Internal,
		ex.Options.NoWait,
		assertRightTableTypes(ex.Options.Args))
	if err != nil {
		return fmt.Errorf("failed to declare the exchange %s, err: %w", name, err)
	}

	return nil
}

func (f *Factory) declareQueue(ch *amqp.Channel, queue QueueConfig) error {
	f.log("declaring queue", Fields{
		"queue":   queue.Name,
		"options": queue.Options,
	})

	q, err := ch.QueueDeclare(
		queue.Name,
		queue.Options.Durable,
		queue.Options.AutoDelete,
		queue.Options.Exclusive,
		queue.Options.NoWait,
		assertRightTableTypes(queue.Options.Args))
	if err != nil {
		return fmt.Errorf("failed to declare the queue \"%s\"", queue.Name)
	}

	for _, b := range queue.Bindings {
		f.log("declaring queue bind", Fields{
			"queue":    queue.Name,
			"exchange": b.Exchange,
		})

		err = f.declareExchange(ch, b.Exchange)
		if err != nil {
			return err
		}

		for _, k := range b.RoutingKeys {
			err = ch.QueueBind(q.Name, k, b.Exchange,
				b.Options.NoWait, assertRightTableTypes(b.Options.Args))
			if err != nil {
				return errors.Wrapf(err, "failed to bind the queue \"%s\" to exchange: \"%s\"", q.Name, b.Exchange)
			}
		}
	}

	return nil
}

func (f *Factory) declareDeadLetters(ch *amqp.Channel, name string) error {
	f.log("declaring deadletter", Fields{"dlx": name})

	dead, ok := f.config.DeadLetters[name]
	if !ok {
		f.log("deadletter config didn't exist, we will try to continue", Fields{"dlx": name})
		return nil
	}

	err := f.declareQueue(ch, dead.Queue)

	return errors.Wrapf(err, "failed to declare the queue for deadletter %s", name)
}

func (f *Factory) getChannel(connectionName string) (*amqp.Channel, error) {
	_, ok := f.conns[connectionName]
	if !ok {
		available := []string{}
		for name := range f.conns {
			available = append(available, name)
		}

		return nil, fmt.Errorf(
			"connection (%s) did not exist, connections names available: %s",
			connectionName,
			strings.Join(available, ", "))
	}

	var ch *amqp.Channel
	var errCH error

	conn := f.conns[connectionName]
	ch, errCH = conn.Channel()
	// Reconnect the connection when receive an connection closed error
	if errCH != nil && errCH.Error() == amqp.ErrClosed.Error() {
		cfgConn := f.config.Connections[connectionName]
		f.log("reopening one connection closed",
			Fields{
				"sleep":      cfgConn.Sleep,
				"timeout":    cfgConn.Timeout,
				"connection": connectionName,
			},
		)

		conn, err := openConnection(cfgConn)
		if err != nil {
			return nil, errors.Wrapf(err, "error reopening the connection \"%s\"", connectionName)
		}

		f.conns[connectionName] = conn
		ch, errCH = conn.Channel()
	}

	return ch, errCH
}

func openConnection(config Connection) (*amqp.Connection, error) {
	var conn *amqp.Connection

	err := retry.Do(func() error {
		var err error
		conn, err = amqp.DialConfig(config.DSN, amqp.Config{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, config.Timeout)
			},
			Properties: amqp.Table{
				"product": "rabbids",
				"version": Version,
			},
		})
		return err
	}, 5, config.Sleep)

	return conn, err
}

func assertRightTableTypes(args amqp.Table) amqp.Table {
	nArgs := amqp.Table{}

	for k, v := range args {
		switch v := v.(type) {
		case int:
			nArgs[k] = int64(v)
		default:
			nArgs[k] = v
		}
	}

	return nArgs
}
