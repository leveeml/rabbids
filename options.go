package rabbids

import (
	"encoding/json"
)

// PublishingOption represents an option you can pass to setup some data inside the Publishing.
type PublishingOption func(*Publishing) error

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
