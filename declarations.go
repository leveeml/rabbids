package rabbids

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// declarations is the block responsible for create consumers and restart the rabbitMQ connections.
type declarations struct {
	config *Config
	log    LoggerFN
}

func (f *declarations) declareExchange(ch *amqp.Channel, name string) error {
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

func (f *declarations) declareQueue(ch *amqp.Channel, queue QueueConfig) error {
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

func (f *declarations) declareDeadLetters(ch *amqp.Channel, name string) error {
	f.log("declaring deadletter", Fields{"dlx": name})

	dead, ok := f.config.DeadLetters[name]
	if !ok {
		f.log("deadletter config didn't exist, we will try to continue", Fields{"dlx": name})
		return nil
	}

	err := f.declareQueue(ch, dead.Queue)

	return errors.Wrapf(err, "failed to declare the queue for deadletter %s", name)
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
