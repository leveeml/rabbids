package rabbids

import (
	"time"
)

// supervisor start all the consumers from Rabbids and
// keep track of the consumers status, restating them when needed
type supervisor struct {
	checkAliveness time.Duration
	rabbids        *Rabbids
	consumers      map[string]*Consumer
	close          chan struct{}
}

// StartSupervisor init a new supervisor that will start all the consumers from Rabbids
// and check if the consumers are alive, if not alive it will be restarted.
// It returns the stop function to gracefully shutdown the consumers and
// an error if fail to create the consumers the first time.
func StartSupervisor(rabbids *Rabbids, intervalChecks time.Duration) (stop func(), err error) {
	s := &supervisor{
		checkAliveness: intervalChecks,
		rabbids:        rabbids,
		consumers:      map[string]*Consumer{},
		close:          make(chan struct{}),
	}

	cs, err := s.rabbids.CreateConsumers()
	if err != nil {
		return s.Stop, err
	}

	for _, c := range cs {
		c.Run()
		s.consumers[c.Name()] = c
	}

	go s.loop()

	return s.Stop, nil
}

func (s *supervisor) loop() {
	ticker := time.NewTicker(s.checkAliveness)

	for {
		select {
		case <-s.close:
			for name, c := range s.consumers {
				c.Kill()
				delete(s.consumers, name)
			}
			s.close <- struct{}{}
			return
		case <-ticker.C:
			s.restartDeadConsumers()
		}
	}
}

// Stop all the running consumers
func (s *supervisor) Stop() {
	s.close <- struct{}{}
	<-s.close
}

func (s *supervisor) restartDeadConsumers() {
	for name, c := range s.consumers {
		if !c.Alive() {
			s.rabbids.log("recreating one consumer", Fields{
				"consumer-name": name,
			})

			nc, err := s.rabbids.CreateConsumer(name)
			if err != nil {
				s.rabbids.log("error recreating one consumer", Fields{
					"consumer-name": name,
					"error":         err,
				})

				continue
			}

			delete(s.consumers, name)
			s.consumers[name] = nc
			nc.Run()
		}
	}
}
