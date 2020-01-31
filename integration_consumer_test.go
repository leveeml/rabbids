// +build integration

package rabbids_test

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/empregoligado/rabbids"
	rabbithole "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/ory-am/dockertest.v3"
)

func TestIntegrationSuite(t *testing.T) {
	tests := []struct {
		scenario string
		method   func(*testing.T, *dockertest.Resource)
	}{
		{
			scenario: "validate the behavior when we have connection trouble",
			method:   testFactoryShouldReturnConnectionErrors,
		},
		{
			scenario: "validate the behavior of one healthy consumer",
			method:   testConsumerProcess,
		},
		{
			scenario: "validate that all the consumers will restart without problems",
			method:   testConsumerReconnect,
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

func testFactoryShouldReturnConnectionErrors(t *testing.T, _ *dockertest.Resource) {
	c := getConfigHelper(t, "valid_queue_and_exchange_config.yml")
	t.Run("when we pass an invalid port", func(t *testing.T) {
		conn := c.Connections["default"]
		conn.DSN = "amqp://guest:guest@localhost:80/"
		c.Connections["default"] = conn
		_, err := rabbids.NewFactory(c, logFNHelper(t))
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "error opening the connection \"default\": ")
	})
	t.Run("when we pass an invalid host", func(t *testing.T) {
		conn := c.Connections["default"]
		conn.DSN = "amqp://guest:guest@10.255.255.1:5672/"
		c.Connections["default"] = conn
		_, err := rabbids.NewFactory(c, logFNHelper(t))
		assert.EqualError(t, err, "error opening the connection \"default\": dial tcp 10.255.255.1:5672: i/o timeout")
	})
}

func testConsumerProcess(t *testing.T, resource *dockertest.Resource) {
	config := getConfigHelper(t, "valid_queue_and_exchange_config.yml")
	config.Connections["default"] = setDSN(resource, config.Connections["default"])

	handler := &mockHandler{count: 0, ack: true}
	config.RegisterHandler("messaging_consumer", handler)
	factory, err := rabbids.NewFactory(config, logFNHelper(t))
	require.NoError(t, err, "Failed to create the factory")
	manager, err := rabbids.NewManager(factory, 10*time.Millisecond, logFNHelper(t))
	require.NoError(t, err, "Failed to create the Manager")
	defer manager.Stop()
	ch := getChannelHelper(t, resource)
	for i := 0; i < 5; i++ {
		err = ch.Publish("event_bus", "service.whatssapp.send", false, false, amqp.Publishing{
			Body: []byte(`{"fooo": "bazzz"}`),
		})
		require.NoError(t, err, "error publishing to rabbitMQ")
	}
	<-time.After(400 * time.Millisecond)
	require.EqualValues(t, 5, handler.messagesProcessed())
	for _, cfg := range config.Consumers {
		_, err := ch.QueueDelete(cfg.Queue.Name, false, false, false)
		require.NoError(t, err)
	}
	for name := range config.Exchanges {
		err := ch.ExchangeDelete(name, false, false)
		require.NoError(t, err)
	}
}

func testConsumerReconnect(t *testing.T, resource *dockertest.Resource) {
	config := getConfigHelper(t, "valid_two_connections.yml")
	config.Connections["default"] = setDSN(resource, config.Connections["default"])
	config.Connections["test1"] = setDSN(resource, config.Connections["test1"])
	received := make(chan string, 10)
	handler := rabbids.MessageHandlerFunc(func(m rabbids.Message) {
		received <- string(m.Body)
		m.Ack(false)
	})
	config.RegisterHandler("send_consumer", handler)
	config.RegisterHandler("response_consumer", handler)
	factory, err := rabbids.NewFactory(config, logFNHelper(t))
	require.NoError(t, err, "failed to create the rabbids factory")
	manager, err := rabbids.NewManager(factory, 10*time.Millisecond, logFNHelper(t))
	require.NoError(t, err, "Failed to create the Manager")
	defer manager.Stop()

	sendMessages(t, resource, "event_bus", "service.whatssapp.send", 0, 2)
	sendMessages(t, resource, "event_bus", "service.whatssapp.response", 3, 4)
	time.Sleep(1 * time.Second)
	require.Len(t, received, 5, "consumer should be processed 5 messages before close connections")

	// get the http client and force to close all the connections
	closeRabbitMQConnections(t, resource)

	// send new messages
	sendMessages(t, resource, "event_bus", "service.whatssapp.send", 5, 6)
	sendMessages(t, resource, "event_bus", "service.whatssapp.response", 7, 8)
	time.Sleep(1 * time.Second)

	require.Len(t, received, 9, "consumer should be processed 9 messages")
}

func sendMessages(t *testing.T, resource *dockertest.Resource, ex, key string, start, count int) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://localhost:%s", resource.GetPort("5672/tcp")))
	require.NoError(t, err, "failed to open a new connection for tests")
	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open a channel for tests")

	for i := start; i <= count; i++ {
		err := ch.Publish(ex, key, false, false, amqp.Publishing{
			Body: []byte(fmt.Sprintf("%d ", i)),
		})
		require.NoError(t, err, "error publishing to rabbitMQ")
	}
}

func getConfigHelper(t *testing.T, configFile string) *rabbids.Config {
	config, err := rabbids.ConfigFromFile(filepath.Join("testdata", configFile))
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func setDSN(resource *dockertest.Resource, conn rabbids.Connection) rabbids.Connection {
	conn.DSN = fmt.Sprintf("amqp://localhost:%s", resource.GetPort("5672/tcp"))
	return conn
}

func closeRabbitMQConnections(t *testing.T, resource *dockertest.Resource) {
	client, err := rabbithole.NewClient(
		fmt.Sprintf("http://localhost:%s", resource.GetPort("15672/tcp")),
		"guest", "guest")
	require.NoError(t, err, "Fail to create the rabbithole client")
	conns, err := client.ListConnections()
	require.NoError(t, err, "fail to get all the connections")
	t.Logf("Found %d open connections", len(conns))
	for _, conn := range conns {
		_, err = client.CloseConnection(conn.Name)
		require.NoError(t, err, "fail to close the connection ", conn.Name)
	}
}

func logFNHelper(tb testing.TB) rabbids.LoggerFN {
	return func(message string, fields rabbids.Fields) {
		pattern := message + " fields: "
		values := []interface{}{}
		for k, v := range fields {
			pattern += "%s=%v "
			values = append(values, k, v)
		}
		tb.Logf(pattern, values...)
	}
}

func getChannelHelper(tb testing.TB, resource *dockertest.Resource) *amqp.Channel {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://localhost:%s", resource.GetPort("5672/tcp")))
	if err != nil {
		tb.Fatal("Failed to connect with rabbitMQ: ", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		tb.Fatal("Failed to create a new channel: ", err)
	}
	return ch
}

type mockHandler struct {
	count int64
	ack   bool
}

func (m *mockHandler) Handle(msg rabbids.Message) {
	atomic.AddInt64(&m.count, 1)
	if m.ack {
		msg.Ack(false)
		return
	}
	msg.Nack(false, false)
}

func (m *mockHandler) Close() {}

func (m *mockHandler) messagesProcessed() int64 {
	return atomic.LoadInt64(&m.count)
}