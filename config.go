package rabbids

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/a8m/envsubst"
	"github.com/mitchellh/mapstructure"
	"github.com/streadway/amqp"
	yaml "gopkg.in/yaml.v3"
)

const Version = "0.0.1"

// Config describes all available options for amqp connection creation.
type Config struct {
	// Connections describe the connections used by consumers.
	Connections map[string]Connection `mapstructure:"connections"`
	// Exchanges have all the exchanges used by consumers.
	// This exchanges are declared on startup of the rabbitMQ factory.
	Exchanges map[string]ExchangeConfig `mapstructure:"exchanges"`
	// DeadLetters have all the deadletters queues used internally by other queues
	// This will be declared at startup of the rabbitMQ factory
	DeadLetters map[string]DeadLetter `mapstructure:"dead_letters"`
	// Consumers describes configuration list for consumers.
	Consumers map[string]ConsumerConfig `mapstructure:"consumers"`
	// Registered Message handlers used by consumers
	Handlers map[string]MessageHandler
}

// Connection describe a config for one connection.
type Connection struct {
	DSN     string        `mapstructure:"dsn"`
	Timeout time.Duration `mapstructure:"timeout"`
	Sleep   time.Duration `mapstructure:"sleep"`
	Retries int           `mapstructure:"retries"`
}

// ConsumerConfig describes consumer's configuration.
type ConsumerConfig struct {
	Connection    string      `mapstructure:"connection"`
	Workers       int         `mapstructure:"workers"`
	PrefetchCount int         `mapstructure:"prefetch_count"`
	DeadLetter    string      `mapstructure:"dead_letter"`
	Queue         QueueConfig `mapstructure:"queue"`
	Options       Options     `mapstructure:"options"`
}

// ExchangeConfig describes exchange's configuration.
type ExchangeConfig struct {
	Type    string  `mapstructure:"type"`
	Options Options `mapstructure:"options"`
}

// DeadLetter describe all the dead letters queues to be declared before declare other queues.
type DeadLetter struct {
	Queue QueueConfig `mapstructure:"queue"`
}

// QueueConfig describes queue's configuration.
type QueueConfig struct {
	Name     string    `mapstructure:"name"`
	Bindings []Binding `mapstructure:"bindings"`
	Options  Options   `mapstructure:"options"`
}

// Binding describe how a queue connects to a exchange.
type Binding struct {
	Exchange    string   `mapstructure:"exchange"`
	RoutingKeys []string `mapstructure:"routing_keys"`
	Options     Options  `mapstructure:"options"`
}

// Options describes optionals configuration for consumer, queue, bindings and exchanges.
type Options struct {
	Durable    bool       `mapstructure:"durable"`
	Internal   bool       `mapstructure:"internal"`
	AutoDelete bool       `mapstructure:"auto_delete"`
	Exclusive  bool       `mapstructure:"exclusive"`
	NoWait     bool       `mapstructure:"no_wait"`
	NoLocal    bool       `mapstructure:"no_local"`
	AutoAck    bool       `mapstructure:"auto_ack"`
	Args       amqp.Table `mapstructure:"args"`
}

func setConfigDefaults(config *Config) {
	for k := range config.Connections {
		cfg := config.Connections[k]
		if cfg.Retries == 0 {
			cfg.Retries = 5
		}

		if cfg.Sleep == 0 {
			cfg.Sleep = 500 * time.Millisecond
		}

		if cfg.Timeout == 0 {
			cfg.Timeout = 2 * time.Second
		}

		config.Connections[k] = cfg
	}

	for k := range config.Consumers {
		cfg := config.Consumers[k]
		if cfg.Workers <= 0 {
			cfg.Workers = 1
		}

		if cfg.PrefetchCount <= 0 {
			// we need at least 2 more messages than our worker to be able to see workers blocked
			cfg.PrefetchCount = cfg.Workers + 2
		}

		config.Consumers[k] = cfg
	}
}

func (c *Config) RegisterHandler(name string, h MessageHandler) {
	if c.Handlers == nil {
		c.Handlers = map[string]MessageHandler{}
	}

	c.Handlers[name] = h
}

func ConfigFromFile(filename string) (*Config, error) {
	input := map[string]interface{}{}
	output := &Config{}

	in, err := envsubst.ReadFileRestricted(filename, true, false)
	if err != nil {
		return nil, fmt.Errorf("failed to read the file: %w", err)
	}

	switch getConfigType(filename) {
	case "yaml", "yml":
		err = yaml.Unmarshal(in, &input)
		if err != nil {
			return nil, fmt.Errorf("failed to decode the yaml configuration. %w", err)
		}
	default:
		return nil, fmt.Errorf("file extension %s not supported", getConfigType(filename))
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	})
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(input)

	return output, err
}

func getConfigType(file string) string {
	ext := filepath.Ext(file)

	if len(ext) > 1 {
		return ext[1:]
	}

	return ""
}
