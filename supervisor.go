package rabbids

import (
	"time"
)

// Supervisor is the block responsible for creating all the consumers.
// Keeping track of the current state of consumers and stop/restart consumers when needed.
type Supervisor struct {
	checkAliveness time.Duration
	factory        *Factory
	consumers      map[string]*Consumer
	close          chan struct{}
	log            LoggerFN
}

// NewSupervisor init a new consumer supervisor to make sure all the consumers are alive.
func NewSupervisor(config *Config, intervalChecks time.Duration, log LoggerFN) (*Supervisor, error) {
	factory, err := NewFactory(config, log)
	if err != nil {
		return nil, err
	}

	m := &Supervisor{
		checkAliveness: intervalChecks,
		factory:        factory,
		log:            log,
		consumers:      map[string]*Consumer{},
		close:          make(chan struct{}),
	}

	cs, err := m.factory.CreateConsumers()
	if err != nil {
		return nil, err
	}

	for _, c := range cs {
		c.Run()
		m.consumers[c.Name()] = c
	}

	//we use a supervisor as a program structure and didn`t need to close this goroutines
	go m.checks()

	return m, nil
}

// checks will execute all the operations received from the internal operation channel
func (m *Supervisor) checks() {
	ticker := time.NewTicker(m.checkAliveness)

	for {
		select {
		case <-m.close:
			for name, c := range m.consumers {
				c.Kill()
				delete(m.consumers, name)
			}
			m.close <- struct{}{}
		case <-ticker.C:
			m.restartDeadConsumers()
		}
	}
}

// Stop all the consumers
func (m *Supervisor) Stop() {
	m.close <- struct{}{}
	<-m.close
}

func (m *Supervisor) restartDeadConsumers() {
	for name, c := range m.consumers {
		if !c.Alive() {
			m.log("recreating one consumer", Fields{
				"consumer-name": name,
			})

			nc, err := m.factory.CreateConsumer(name)
			if err != nil {
				m.log("error recreating one consumer", Fields{
					"consumer-name": name,
					"error":         err,
				})

				continue
			}

			delete(m.consumers, name)
			m.consumers[name] = nc
			nc.Run()
		}
	}
}
