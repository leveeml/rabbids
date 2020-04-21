package rabbids_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/empregoligado/rabbids"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"gopkg.in/ory-am/dockertest.v3"
)

func TestBasicIntegrationProducer(t *testing.T) {
	integrationTest(t)
	t.Parallel()

	tests := []struct {
		scenario string
		method   func(*testing.T, *dockertest.Resource)
	}{
		{
			scenario: "test producer with connection problems",
			method:   testProducerWithReconnect,
		},
	}
	// -> Setup
	dockerPool, err := dockertest.NewPool("")
	require.NoError(t, err, "Coud not connect to docker")
	resource, err := dockerPool.Run("rabbitmq", "3.6.12-management", []string{})
	require.NoError(t, err, "Could not start resource")

	// -> TearDown
	defer func() {
		if err := dockerPool.Purge(resource); err != nil {
			t.Errorf("Could not purge resource: %s", err)
		}
	}()
	// -> Run!
	for _, test := range tests {
		tt := test
		t.Run(test.scenario, func(st *testing.T) {
			tt.method(st, resource)
		})
	}
}

func testProducerWithReconnect(t *testing.T, resource *dockertest.Resource) {
	var wg sync.WaitGroup

	adminClient := getRabbitClient(t, resource)
	rab, err := rabbids.NewProducer("", rabbids.WithConnection(rabbids.Connection{
		DSN:     getDSN(resource),
		Timeout: 100 * time.Millisecond,
		Sleep:   1000 * time.Millisecond,
		Retries: 6,
	}))
	require.NoError(t, err, "could not connect to: ", getDSN(resource))

	ch := rab.GetAMPQChannel()

	_, err = ch.QueueDeclare("testProducerWithReconnect", true, false, false, false, amqp.Table{})
	require.NoError(t, err)

	wg.Add(2)

	go func() {
		defer wg.Done()

		rab.Run()
	}()

	var emitWithErrors int64

	go func() {
		defer wg.Done()

		for pErr := range rab.EmitErr() {
			t.Logf("received a emitErr: %v", pErr)
			atomic.AddInt64(&emitWithErrors, 1)
		}
	}()

	for i := 1; i <= 1000; i++ {
		if i%100 == 0 {
			closeRabbitMQConnections(t, adminClient)
		}
		rab.Emit() <- rabbids.NewPublishing("", "testProducerWithReconnect",
			map[string]int{"test": i},
		)
		time.Sleep(time.Millisecond)
	}

	err = rab.Close()
	require.NoError(t, err, "error closing the connection")
	wg.Wait()

	count := getQueueLength(t, adminClient, "testProducerWithReconnect", 40*time.Second)
	t.Logf("Finished published with %d messages inside the queue and %d messages with error", count, emitWithErrors)
}
