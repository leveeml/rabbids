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

func WithCustomName(name string) ProducerOption {
	return func(p *Producer) error {
		p.name = name

		return nil
	}
}

// WithLogger will override the default logger (no Operation Log).
func WithLogger(log LoggerFN) ProducerOption {
	return func(p *Producer) error {
		p.log = log

		return nil
	}
}

// withDeclarations will add the AMQP declarations and be able to declare the exchanges used.
func withDeclarations(d *declarations) ProducerOption {
	return func(p *Producer) error {
		p.declarations = d

		return nil
	}
}

// withConnection add the connection config to set up the Connection instead the default values.
func withConnection(conf Connection) ProducerOption {
	return func(p *Producer) error {
		p.conf = conf

		return nil
	}
}

func WithSerializer(s Serializer) ProducerOption {
	return func(p *Producer) error {
		p.serializer = s

		return nil
	}
}
