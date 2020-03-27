package rabbids_test

import (
	"testing"
	"time"

	"github.com/empregoligado/rabbids"
	"github.com/stretchr/testify/require"
)

func TestConnectionErrors(t *testing.T) {
	t.Parallel()
	t.Run("passing an invalid port", func(t *testing.T) {
		t.Parallel()

		_, err := rabbids.NewProducerFromDSN("amqp://guest:guest@localhost:80/")
		require.Error(t, err, "expect an error")
		require.Contains(t, err.Error(), "connect: connection refuse")
	})
	t.Run("passing an invalid host", func(t *testing.T) {
		t.Parallel()

		_, err := rabbids.NewProducerFromConfig(rabbids.Connection{
			DSN:     "amqp://guest:guest@10.255.255.1:5672/",
			Timeout: 100 * time.Millisecond,
			Sleep:   10 * time.Millisecond,
			Retries: 2,
		})

		require.Error(t, err, "expect to return an error")
		require.EqualError(t, err, "dial tcp 10.255.255.1:5672: i/o timeout")
	})
}
