package rabbids

import (
	"errors"
	"fmt"

	"gopkg.in/tomb.v2"

	"github.com/ivpusic/grpool"
	"github.com/streadway/amqp"
)

// Consumer is a high level rabbitMQ consumer
type Consumer struct {
	handler    MessageHandler
	number     int64
	name       string
	queue      string
	workerPool *grpool.Pool
	opts       Options
	channel    *amqp.Channel
	t          tomb.Tomb
	log        LoggerFN
}

// Run start a goroutine to consume messages from a queue and pass to one runner.
func (c *Consumer) Run() {
	c.t.Go(func() error {
		defer func() {
			if c.channel == nil {
				return
			}
			err := c.channel.Close()
			if err != nil {
				c.log("Error closing the consumer channel", Fields{"error": err, "name": c.name})
			}
		}()
		d, err := c.channel.Consume(c.queue, fmt.Sprintf("rabbitmq-%s-%d", c.name, c.number),
			c.opts.AutoAck,
			c.opts.Exclusive,
			c.opts.NoLocal,
			c.opts.NoWait,
			c.opts.Args)
		if err != nil {
			c.log("Failed to start consume", Fields{"error": err, "name": c.name})
			return err
		}
		dying := c.t.Dying()
		closed := c.channel.NotifyClose(make(chan *amqp.Error))
		for {
			select {
			case <-dying:
				// When dying we wait for any remaining worker to finish and close the handler
				c.workerPool.WaitAll()
				c.handler.Close()
				return nil
			case err := <-closed:
				return err
			case msg, ok := <-d:
				if !ok {
					return errors.New("internal channel closed")
				}
				c.workerPool.WaitCount(1)
				fn := func(msg amqp.Delivery) func() {
					return func() {
						c.handler.Handle(Message{msg})
						c.workerPool.JobDone()
					}
				}(msg)
				// When Workers goroutines are in flight, Send a Job blocks until one of the
				// workers finishes.
				c.workerPool.JobQueue <- fn
			}
		}
	})
}

// Kill will try to stop the internal work.
func (c *Consumer) Kill() {
	c.t.Kill(nil)
	<-c.t.Dead()
}

// Alive returns true if the tomb is not in a dying or dead state.
func (c *Consumer) Alive() bool {
	return c.t.Alive()
}

// Name return the consumer name
func (c *Consumer) Name() string {
	return c.name
}
