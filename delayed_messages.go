package rabbids

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/streadway/amqp"
)

const (
	maxNumberOfBitsToUse int = 28
	maxLevel             int = maxNumberOfBitsToUse - 1

	MaxDelay              time.Duration = ((1 << maxNumberOfBitsToUse) - 1) * time.Second
	DelayDeliveryExchange string        = "rabbids.delay-delivery"
)

// delayDelivery is based on the setup of delay messages created by the NServiceBus project.
// For more information go to the docs on https://docs.particular.net/transports/rabbitmq/delayed-delivery.
type delayDelivery struct {
}

func (d *delayDelivery) Build(ch *amqp.Channel) error {
	var bindingKey = "1.#"

	for level := maxLevel; level >= 0; level-- {
		currentLevel := d.levelName(level)
		nextLevel := d.levelName(level - 1)

		if level == 0 {
			nextLevel = DelayDeliveryExchange
		}

		err := ch.ExchangeDeclare(currentLevel, amqp.ExchangeTopic, true, false, false, false, amqp.Table{})
		if err != nil {
			return err
		}

		_, err = ch.QueueDeclare(currentLevel, true, false, false, false, amqp.Table{
			"x-queue-mode":           "lazy",
			"x-message-ttl":          int64(math.Pow(2, float64(level)) * 1000),
			"x-dead-letter-exchange": nextLevel,
		})
		if err != nil {
			return err
		}

		err = ch.QueueBind(currentLevel, bindingKey, currentLevel, false, amqp.Table{})
		if err != nil {
			return err
		}

		bindingKey = "*." + bindingKey
	}

	bindingKey = "0.#"

	for level := maxLevel; level >= 0; level-- {
		currentLevel := d.levelName(level)
		nextLevel := d.levelName(level - 1)

		err := ch.ExchangeBind(nextLevel, bindingKey, currentLevel, false, amqp.Table{})
		if err != nil {
			return err
		}

		bindingKey = "*." + bindingKey
	}

	err := ch.ExchangeDeclare(DelayDeliveryExchange, amqp.ExchangeTopic, true, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	err = ch.ExchangeBind(DelayDeliveryExchange, bindingKey, d.levelName(0), false, amqp.Table{})

	return err
}

// CalculateRoutingKey return the routingkey and the first applicable exchange
// to avoid unnecessary traversal through the delay infrastructure.
func (d *delayDelivery) CalculateRoutingKey(delay time.Duration, address string) (string, string) {
	if delay > MaxDelay {
		delay = MaxDelay
	}

	sec := uint(delay.Seconds())
	var buf bytes.Buffer
	firstLevel := 0

	for level := maxLevel; level >= 0; level-- {
		if firstLevel == 0 && sec&(1<<uint(level)) != 0 {
			firstLevel = level
		}
		if sec&(1<<uint(level)) != 0 {
			buf.WriteString("1.")
		} else {
			buf.WriteString("0.")
		}
	}

	buf.WriteString(address)

	return buf.String(), d.levelName(firstLevel)
}

func (d delayDelivery) levelName(level int) string {
	return fmt.Sprintf("rabbids.delay-level-%d", level)
}
