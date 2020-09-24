package rabbids_test

import (
	"testing"

	"github.com/leveeml/rabbids"
	"github.com/stretchr/testify/require"
)

func TestConnectionErrors(t *testing.T) {
	t.Parallel()
	t.Run("passing an invalid port", func(t *testing.T) {
		t.Parallel()

		_, err := rabbids.NewProducer("amqp://guest:guest@localhost:80/")
		require.Error(t, err, "expect an error")
		require.Contains(t, err.Error(), "connect: connection refuse")
	})
	t.Run("passing an invalid host", func(t *testing.T) {
		t.Parallel()

		_, err := rabbids.NewProducer("amqp://guest:guest@10.255.255.1:5672/")

		require.Error(t, err, "expect to return an error")
		require.EqualError(t, err, "dial tcp 10.255.255.1:5672: i/o timeout")
	})
}
