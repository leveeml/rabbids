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

const (
	Version        = "0.0.1"
	DefaultTimeout = 2 * time.Second
	DefaultSleep   = 500 * time.Millisecond
	DefaultRetries = 5
)

// Config describes all available options to declare all the components used by
// rabbids Consumers and Producers.
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
	// Producers describes the configuration list for producers.
	// In most cases you only need one producer and will need other producers
	// if you has connection rules (virtual host or permissions by user)
	Producers map[string]ProducerConfig `mapstructure:"producers"`
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

// Options describes optionals configuration
// for consumer, queue, bindings and exchanges declaration.
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

// ProducerConfig set the Connection name used for this producer
// The only required param is the Connection string, the rest of the params
// will be set by default values.
type ProducerConfig struct {
	Connection string        `mapstructure:"connection"`
	Sleep      time.Duration `mapstructure:"sleep"`
	Retries    int           `mapstructure:"retries"`
}

func setConfigDefaults(config *Config) {
	for k := range config.Connections {
		cfg := config.Connections[k]
		if cfg.Retries == 0 {
			cfg.Retries = DefaultRetries
		}

		if cfg.Sleep == 0 {
			cfg.Sleep = DefaultSleep
		}

		if cfg.Timeout == 0 {
			cfg.Timeout = DefaultTimeout
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

// RegisterHandler is used to set the MessageHandler used by one Consumer.
// The consumerName MUST be equal as the name used by the Consumer
// (the key inside the map of consumers)
func (c *Config) RegisterHandler(consumerName string, h MessageHandler) {
	if c.Handlers == nil {
		c.Handlers = map[string]MessageHandler{}
	}

	c.Handlers[consumerName] = h
}

// ConfigFromFile read a YAML file and convert it into a Config struct
// with all the configuration to build the Consumers and producers.
// Also, it Is possible to use environment variables values inside the YAML file.
// The syntax is like the syntax used inside the docker-compose file.
// To use a required variable just use like this: ${ENV_NAME}
// and to put an default value you can use: ${ENV_NAME:=some-value} inside any value.
// If a required variable didn't exist, an error will be returned
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
