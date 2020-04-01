package rabbids

import (
	"encoding/json"
)

// PublishingOption represents an option you can pass to setup some data inside the Publishing.
type PublishingOption func(*Publishing) error

// ProducerOption represents an option function to add some functionality or change the producer
// state on creation time.
type ProducerOption func(*Producer) error

// WithJSONEncoding marshal the data type and add the result to the Publishing
// with all the necessary headers included.
func WithJSONEncoding(data interface{}) PublishingOption {
	return func(p *Publishing) error {
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}

		p.Body = b
		p.ContentEncoding = "UTF-8"
		p.ContentType = "application/json"

		return nil
	}
}

// WithPriority change the priority of the Publishing message.
func WithPriority(v int) PublishingOption {
	return func(p *Publishing) error {
		if v < 0 {
			v = 0
		}

		if v > 9 {
			v = 9
		}

		p.Priority = uint8(v)

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
