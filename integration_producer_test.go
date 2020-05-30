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

	tests := []struct {
		scenario string
		method   func(*testing.T, *dockertest.Resource)
	}{
		{
			scenario: "test producer with connection problems",
			method:   testProducerWithReconnect,
		},
		{
			scenario: "test send delay messages",
			method:   testPublishWithDelay,
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
	producer, err := rabbids.NewProducer(getDSN(resource))
	require.NoError(t, err, "could not connect to: ", getDSN(resource))

	ch := producer.GetAMPQChannel()

	_, err = ch.QueueDeclare("testProducerWithReconnect", true, false, false, false, amqp.Table{})
	require.NoError(t, err)

	var emitWithErrors int64

	wg.Add(1)

	go func() {
		defer wg.Done()

		for pErr := range producer.EmitErr() {
			t.Logf("received a emitErr: %v", pErr)
			atomic.AddInt64(&emitWithErrors, 1)
		}
	}()

	for i := 1; i <= 1000; i++ {
		if i%100 == 0 {
			closeRabbitMQConnections(t, adminClient)
		}
		producer.Emit() <- rabbids.NewPublishing("", "testProducerWithReconnect",
			map[string]int{"test": i},
		)
		time.Sleep(time.Millisecond)
	}

	err = producer.Close()
	require.NoError(t, err, "error closing the connection")
	wg.Wait()

	count := getQueueLength(t, adminClient, "testProducerWithReconnect", 40*time.Second)
	t.Logf("Finished published with %d messages inside the queue and %d messages with error", count, emitWithErrors)
}

func testPublishWithDelay(t *testing.T, resource *dockertest.Resource) {
	adminClient := getRabbitClient(t, resource)
	producer, err := rabbids.NewProducer(getDSN(resource))
	require.NoError(t, err, "could not connect to: ", getDSN(resource))

	ch := producer.GetAMPQChannel()

	_, err = ch.QueueDeclare("testPublishWithDelay", true, false, false, false, amqp.Table{})
	require.NoError(t, err)

	err = producer.Send(rabbids.NewDelayedPublishing(
		"testPublishWithDelay",
		10*time.Second,
		map[string]string{"test": "fooo"},
	))
	require.NoError(t, err, "error on rab.Send")
	time.Sleep(15 * time.Second)

	err = producer.Close()
	require.NoError(t, err, "error closing the connection")

	count := getQueueLength(t, adminClient, "testPublishWithDelay", 10*time.Second)
	require.Equal(t, 1, count, "expecting the message inside the queue")
}
