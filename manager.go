package rabbids

import (
	"time"
)

// Manager is the block responsible for creating all the consumers.
// Keeping track of the current state of consumers and stop/restart consumers when needed.
type Manager struct {
	checkAliveness time.Duration
	factory        *Factory
	consumers      map[string]*consumer
	close          chan struct{}
	log            LoggerFN
}

// NewManager init a new manager and wait for operations.
func NewManager(factory *Factory, intervalChecks time.Duration, log LoggerFN) (*Manager, error) {
	m := &Manager{
		checkAliveness: intervalChecks,
		factory:        factory,
	}

	cs, err := m.factory.CreateConsumers()
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		c.Run()
		m.consumers[c.Name()] = c
	}

	//we use a Manager as a program structure and didn`t need to close this goroutines
	go m.checks()
	return m, nil
}

// work will execute all te operations received from the internal operation channel
func (m *Manager) checks() {
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
func (m *Manager) Stop() {
	m.close <- struct{}{}
	<-m.close
}

func (m *Manager) restartDeadConsumers() {
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
