package rabbids

import (
	"bytes"
	"fmt"
	"math"
	"sync"
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
	delayDeclaredOnce sync.Once
}

// Declare create all the layers of exchanges and queues on rabbitMQ
// and declare the bind between the last rabbids.delay-delivery ex and the queue.
func (d *delayDelivery) Declare(ch *amqp.Channel, key string) error {
	var declaredErr error

	queue := getQueueFromRoutingKey(key)

	d.delayDeclaredOnce.Do(func() {
		declaredErr = d.build(ch)
	})

	if declaredErr != nil {
		return declaredErr
	}

	return ch.QueueBind(queue, fmt.Sprintf("#.%s", queue), DelayDeliveryExchange, false, amqp.Table{})
}

func (d *delayDelivery) build(ch *amqp.Channel) error {
	var bindingKey = "1.#"

	for level := maxLevel; level >= 0; level-- {
		currentLevel := delayedLevelName(level)
		nextLevel := delayedLevelName(level - 1)

		if level == 0 {
			nextLevel = DelayDeliveryExchange
		}

		err := ch.ExchangeDeclare(currentLevel, amqp.ExchangeTopic, true, false, false, false, amqp.Table{})
		if err != nil {
			return fmt.Errorf("failed to declare exchange \"%s\": %v", currentLevel, err)
		}

		_, err = ch.QueueDeclare(currentLevel, true, false, false, false, amqp.Table{
			"x-queue-mode":           "lazy",
			"x-message-ttl":          int64(math.Pow(2, float64(level)) * 1000),
			"x-dead-letter-exchange": nextLevel,
		})
		if err != nil {
			return fmt.Errorf("failed to declare queue \"%s\": %v", currentLevel, err)
		}

		err = ch.QueueBind(currentLevel, bindingKey, currentLevel, false, amqp.Table{})
		if err != nil {
			return fmt.Errorf("failed to bind queue \"%s\" to exchange \"%s\": %v", currentLevel, currentLevel, err)
		}

		bindingKey = "*." + bindingKey
	}

	bindingKey = "0.#"

	for level := maxLevel; level >= 0; level-- {
		currentLevel := delayedLevelName(level)
		nextLevel := delayedLevelName(level - 1)

		if level == 0 {
			break
		}

		err := ch.ExchangeBind(nextLevel, bindingKey, currentLevel, false, amqp.Table{})
		if err != nil {
			return fmt.Errorf("failed to exchange the bind %s->%s: %v", currentLevel, nextLevel, err)
		}

		bindingKey = "*." + bindingKey
	}

	err := ch.ExchangeDeclare(DelayDeliveryExchange, amqp.ExchangeTopic, true, false, false, false, amqp.Table{})
	if err != nil {
		return fmt.Errorf("failed to declare exchange %s: %v", DelayDeliveryExchange, err)
	}

	err = ch.ExchangeBind(DelayDeliveryExchange, bindingKey, delayedLevelName(0), false, amqp.Table{})

	return err
}

// calculateRoutingKey return the routingkey and the first applicable exchange
// to avoid unnecessary traversal through the delay infrastructure.
func calculateRoutingKey(delay time.Duration, queue string) (string, string) {
	if delay > MaxDelay {
		delay = MaxDelay
	}

	var buf bytes.Buffer

	sec := uint(delay.Seconds())
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

	buf.WriteString(queue)

	return buf.String(), delayedLevelName(firstLevel)
}

// getQueueFromKey return the original queue name
// used to generate the delay routing key.
func getQueueFromRoutingKey(key string) string {
	return key[maxNumberOfBitsToUse*2:]
}

func delayedLevelName(level int) string {
	return fmt.Sprintf("rabbids.delay-level-%d", level)
}
