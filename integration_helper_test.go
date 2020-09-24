package rabbids_test

import (
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/leveeml/rabbids"
	rabbithole "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"gopkg.in/ory-am/dockertest.v3"
)

var integration = flag.Bool("integration", false, "run integration tests")

func integrationTest(tb testing.TB) {
	if !*integration {
		tb.SkipNow()
	}
}

func getDSN(resource *dockertest.Resource) string {
	return fmt.Sprintf("amqp://localhost:%s", resource.GetPort("5672/tcp"))
}

func getRabbitClient(t *testing.T, resource *dockertest.Resource) *rabbithole.Client {
	t.Helper()

	client, err := rabbithole.NewClient(
		fmt.Sprintf("http://localhost:%s", resource.GetPort("15672/tcp")),
		"guest", "guest")
	require.NoError(t, err, "Fail to create the rabbithole client")

	return client
}

// close all open connections to the rabbitmq via the management api.
func closeRabbitMQConnections(t *testing.T, client *rabbithole.Client, names ...string) {
	t.Helper()

	timeout := time.After(10 * time.Second)
	namesIdx := make(map[string]struct{})

	for _, name := range names {
		namesIdx[name] = struct{}{}
	}

	for {
		select {
		default:
			connections, err := client.ListConnections()
			require.NoError(t, err, "failed to get the connections")

			if len(connections) >= 1 {
				for _, c := range connections {
					connectionName, _ := c.ClientProperties["connection_name"].(string)
					if _, exists := namesIdx[connectionName]; exists {
						t.Logf("killing connection: (%s) name: %s", c.Name, connectionName)

						_, err := client.CloseConnection(c.Name)
						require.NoError(t, err, "impossible to kill connection", c.Name)
					} else {
						t.Logf("skipping closing the connection with name: %v", c.ClientProperties["connection_name"])
					}
				}

				return
			}

			time.Sleep(time.Second)
		case <-timeout:
			t.Log("timeout for killing connection reached")

			return
		}
	}
}

//nolint:unparam
func sendMessages(t *testing.T, resource *dockertest.Resource, ex, key string, start, count int) {
	t.Helper()

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
	t.Helper()

	config, err := rabbids.ConfigFromFilename(filepath.Join("testdata", configFile))
	require.NoError(t, err)

	return config
}

func setDSN(resource *dockertest.Resource, conn rabbids.Connection) rabbids.Connection {
	conn.DSN = fmt.Sprintf("amqp://localhost:%s", resource.GetPort("5672/tcp"))
	return conn
}

func logFNHelper(tb testing.TB) rabbids.LoggerFN {
	if testing.Short() {
		return rabbids.NoOPLoggerFN
	}

	return func(message string, fields rabbids.Fields) {
		pattern := message + " fields: "
		values := []interface{}{}

		for k, v := range fields {
			pattern += "%s=%v "

			values = append(values, k, v)
		}

		tb.Helper()
		tb.Logf(pattern, values...)
	}
}

func getChannelHelper(tb testing.TB, resource *dockertest.Resource) *amqp.Channel {
	tb.Helper()

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

func getQueueLength(t *testing.T, client *rabbithole.Client, queuename string, duration time.Duration) int {
	t.Helper()

	timeout := time.After(duration)
	equalCounts := 0
	lastCount := 0

	for {
		info, err := client.GetQueue("/", queuename)
		require.NoError(t, err, "error getting the queue info")

		if info.Messages == lastCount {
			equalCounts++
		} else {
			equalCounts = 0
		}

		lastCount = info.Messages

		if equalCounts >= 3 {
			return info.Messages
		}

		select {
		case <-timeout:
			return info.Messages
		default:
			time.Sleep(time.Second)
		}
	}
}
