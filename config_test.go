package rabbids

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_withDefaults(t *testing.T) {
	config := Config{
		Connections: map[string]Connection{
			"de": {DSN: "amqp://localhost:5672"},
		},
		Consumers: map[string]ConsumerConfig{
			"consumer1": {
				Connection: "server1",
				Queue:      QueueConfig{Name: "fooo"},
			},
		},
	}

	setConfigDefaults(&config)
	require.Equal(t, 5, config.Connections["de"].Retries)
	require.Equal(t, 2*time.Second, config.Connections["de"].Timeout)
	require.Equal(t, 500*time.Millisecond, config.Connections["de"].Sleep)
	require.Equal(t, 1, config.Consumers["consumer1"].Workers)
	require.Equal(t, 3, config.Consumers["consumer1"].PrefetchCount)
}
