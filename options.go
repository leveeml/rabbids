package rabbids

// PublishingOption represents an option you can pass to setup some data inside the Publishing.
type PublishingOption func(*Publishing)

// ProducerOption represents an option function to add some functionality or change the producer
// state on creation time.
type ProducerOption func(*Producer) error

// WithPriority change the priority of the Publishing message.
func WithPriority(v int) PublishingOption {
	return func(p *Publishing) {
		if v < 0 {
			v = 0
		}

		if v > 9 {
			v = 9
		}

		p.Priority = uint8(v)
	}
}

// WithLogger will override the default logger (no Operation Log).
func WithLogger(log LoggerFN) ProducerOption {
	return func(p *Producer) error {
		p.log = log

		return nil
	}
}

// WithFactory will add the factory used to get and declare the exchanges used.
func WithFactory(f *Factory) ProducerOption {
	return func(p *Producer) error {
		p.factory = f

		return nil
	}
}

// WithConnection add the connection config to set up the Connection instead the default values.
func WithConnection(conf Connection) ProducerOption {
	return func(p *Producer) error {
		p.Conf = conf

		return nil
	}
}

func WithSerializer(s Serializer) ProducerOption {
	return func(p *Producer) error {
		p.serializer = s

		return nil
	}
}
