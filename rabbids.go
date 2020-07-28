package rabbids

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ivpusic/grpool"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/streadway/amqp"
	"gopkg.in/tomb.v2"
)

// Rabbids is the main block used to create and run rabbitMQ consumers and producers.
type Rabbids struct {
	conns        map[string]*amqp.Connection
	config       *Config
	declarations *declarations
	log          LoggerFN
	number       int64
}

func New(config *Config, log LoggerFN) (*Rabbids, error) {
	setConfigDefaults(config)

	conns := make(map[string]*amqp.Connection)

	for name, cfgConn := range config.Connections {
		log("opening connection with rabbitMQ", Fields{
			"sleep":      cfgConn.Sleep,
			"timeout":    cfgConn.Timeout,
			"connection": name,
		})

		conn, err := openConnection(cfgConn, fmt.Sprintf("rabbids.%s", name))
		if err != nil {
			return nil, fmt.Errorf("error opening the connection \"%s\": %w", name, err)
		}

		conns[name] = conn
	}

	r := &Rabbids{
		conns:  conns,
		config: config,
		declarations: &declarations{
			config: config,
			log:    log,
		},
		log:    log,
		number: 0,
	}

	return r, nil
}

// CreateConsumers will iterate over config and create all the consumers.
func (r *Rabbids) CreateConsumers() ([]*Consumer, error) {
	var consumers []*Consumer

	for name, cfg := range r.config.Consumers {
		consumer, err := r.newConsumer(name, cfg)
		if err != nil {
			return consumers, err
		}

		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

// CreateConsumer create a new consumer for a specific name using the config provided.
func (r *Rabbids) CreateConsumer(name string) (*Consumer, error) {
	cfg, ok := r.config.Consumers[name]
	if !ok {
		return nil, fmt.Errorf("consumer \"%s\" did not exist", name)
	}

	return r.newConsumer(name, cfg)
}

func (r *Rabbids) newConsumer(name string, cfg ConsumerConfig) (*Consumer, error) {
	ch, err := r.getChannel(cfg.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to open the rabbitMQ channel for consumer %s: %w", name, err)
	}

	if len(cfg.DeadLetter) > 0 {
		err = r.declarations.declareDeadLetters(ch, cfg.DeadLetter)
		if err != nil {
			return nil, err
		}
	}

	err = r.declarations.declareQueue(ch, cfg.Queue)
	if err != nil {
		return nil, err
	}

	if err = ch.Qos(cfg.PrefetchCount, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	handler, ok := r.config.Handlers[name]
	if !ok {
		return nil, fmt.Errorf("failed to create the \"%s\" consumer, Handler not registered", name)
	}

	r.log("consumer created",
		Fields{
			"max-workers": cfg.Workers,
			"consumer":    name,
		})

	return &Consumer{
		queue:      cfg.Queue.Name,
		name:       name,
		number:     atomic.AddInt64(&r.number, 1),
		opts:       cfg.Options,
		channel:    ch,
		t:          tomb.Tomb{},
		handler:    handler,
		workerPool: grpool.NewPool(cfg.Workers, 0),
		log:        r.log,
	}, nil
}

// CreateConsumer create a new consumer using the connection inside the config.
func (r *Rabbids) CreateProducer(connectionName string, customOpts ...ProducerOption) (*Producer, error) {
	conn, exists := r.config.Connections[connectionName]
	if !exists {
		return nil, fmt.Errorf("connection \"%s\" did not exist", connectionName)
	}

	opts := []ProducerOption{
		withConnection(conn),
		WithLogger(r.log),
		withDeclarations(r.declarations),
	}

	return NewProducer("", append(opts, customOpts...)...)
}

func (r *Rabbids) getChannel(connectionName string) (*amqp.Channel, error) {
	_, ok := r.conns[connectionName]
	if !ok {
		available := []string{}
		for name := range r.conns {
			available = append(available, name)
		}

		return nil, fmt.Errorf(
			"connection (%s) did not exist, connections names available: %s",
			connectionName,
			strings.Join(available, ", "))
	}

	var ch *amqp.Channel
	var errCH error

	conn := r.conns[connectionName]
	ch, errCH = conn.Channel()
	// Reconnect the connection when receive an connection closed error
	if errCH != nil && errCH.Error() == amqp.ErrClosed.Error() {
		cfgConn := r.config.Connections[connectionName]
		r.log("reopening one connection closed",
			Fields{
				"sleep":      cfgConn.Sleep,
				"timeout":    cfgConn.Timeout,
				"connection": connectionName,
			},
		)

		conn, err := openConnection(cfgConn, fmt.Sprintf("rabbids.%s", connectionName))
		if err != nil {
			return nil, fmt.Errorf("error reopening the connection \"%s\": %w", connectionName, err)
		}

		r.conns[connectionName] = conn
		ch, errCH = conn.Channel()
	}

	return ch, errCH
}

func openConnection(config Connection, name string) (*amqp.Connection, error) {
	var conn *amqp.Connection

	id, err := uuid.NewRandom()
	if err != nil {
		id = uuid.Must(uuid.NewUUID())
	}

	err = retry.Do(func() error {
		var err error
		conn, err = amqp.DialConfig(config.DSN, amqp.Config{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, config.Timeout)
			},
			Properties: amqp.Table{
				"information":     "https://github.com/EmpregoLigado/rabbids",
				"product":         "Rabbids",
				"version":         Version,
				"id":              id.String(),
				"connection_name": name,
			},
		})
		return err
	}, 5, config.Sleep)

	return conn, err
}
