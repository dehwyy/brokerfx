//go:build integration

package conn_test

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	brokerconn "github.com/dehwyy/brokerfx/pkg/nats/conn"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

var testSeed, testPub = func() (string, string) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		panic(err)
	}
	raw, err := kp.Seed()
	if err != nil {
		panic(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		panic(err)
	}

	return string(raw), pub
}()

func startServer(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()

	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
		Nkeys:     []*server.NkeyUser{{Nkey: testPub}},
	})
	require.NoError(t, err)

	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second))

	return srv
}

func seed(*testing.T) string {
	return testSeed
}

type fakeShutdowner struct {
	calls atomic.Int32
}

func (f *fakeShutdowner) Shutdown(...fx.ShutdownOption) error {
	f.calls.Add(1)
	return nil
}

func TestConnectsBeforeServerIsUp(t *testing.T) {
	port := freePort(t)
	url := "nats://127.0.0.1:" + itoa(port)

	nc, err := brokerconn.New(brokerconn.Opts{
		Servers:       []string{url},
		SeedKey:       seed(t),
		ReconnectWait: 100 * time.Millisecond,
	})()
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	srv := startServer(t, port, t.TempDir())
	t.Cleanup(srv.Shutdown)

	require.Eventually(t, nc.IsConnected, 15*time.Second, 100*time.Millisecond)
}

func TestConsumerSurvivesServerRestart(t *testing.T) {
	port := freePort(t)
	storeDir := t.TempDir()
	srv := startServer(t, port, storeDir)

	nc, err := brokerconn.New(brokerconn.Opts{
		Servers:       []string{"nats://127.0.0.1:" + itoa(port)},
		SeedKey:       seed(t),
		ReconnectWait: 100 * time.Millisecond,
	})()
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	st, err := stream.New(stream.Opts{
		JetStream: js,
		StreamOptsBuilder: streamoptsbuilder.NewDefault().
			WithName("RECONNECT").
			WithSubjects([]string{"reconnect.>"}),
	})
	require.NoError(t, err)

	received := make(chan string, 16)
	_, err = consumer.New(consumer.Opts{
		JetStream: js,
		Stream:    st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().
			WithName("reconnect-consumer", true).
			WithFilterSubject("reconnect.>"),
		HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
			received <- string(msg.Data())
			return msg.Ack()
		},
	})
	require.NoError(t, err)

	_, err = js.Publish(context.Background(), "reconnect.a", []byte("before"))
	require.NoError(t, err)
	require.Equal(t, "before", waitMsg(t, received))

	srv.Shutdown()
	srv.WaitForShutdown()
	require.Eventually(t, func() bool { return !nc.IsConnected() }, 10*time.Second, 50*time.Millisecond)

	srv2 := startServer(t, port, storeDir)
	t.Cleanup(srv2.Shutdown)
	require.Eventually(t, nc.IsConnected, 15*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		_, err := js.Publish(context.Background(), "reconnect.b", []byte("after"))
		return err == nil
	}, 15*time.Second, 200*time.Millisecond)

	require.Equal(t, "after", waitMsg(t, received))
}

func TestClosedConnectionTriggersShutdowner(t *testing.T) {
	port := freePort(t)
	srv := startServer(t, port, t.TempDir())

	sd := &fakeShutdowner{}
	nc, err := brokerconn.New(brokerconn.Opts{
		Servers:       []string{"nats://127.0.0.1:" + itoa(port)},
		SeedKey:       seed(t),
		ReconnectWait: 50 * time.Millisecond,
		MaxReconnects: 2,
		Shutdowner:    sd,
	})()
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	srv.Shutdown()
	srv.WaitForShutdown()

	require.Eventually(t, func() bool { return sd.calls.Load() == 1 }, 15*time.Second, 50*time.Millisecond)
	require.True(t, nc.IsClosed())
}

func TestDeliberateCloseDoesNotTriggerShutdowner(t *testing.T) {
	port := freePort(t)
	srv := startServer(t, port, t.TempDir())
	t.Cleanup(srv.Shutdown)

	sd := &fakeShutdowner{}
	nc, err := brokerconn.New(brokerconn.Opts{
		Servers:    []string{"nats://127.0.0.1:" + itoa(port)},
		SeedKey:    seed(t),
		Shutdowner: sd,
	})()
	require.NoError(t, err)

	nc.Close()
	time.Sleep(500 * time.Millisecond)

	require.Zero(t, sd.calls.Load())
}

func TestDefaultsAreUnlimitedReconnects(t *testing.T) {
	port := freePort(t)
	srv := startServer(t, port, t.TempDir())
	t.Cleanup(srv.Shutdown)

	nc, err := brokerconn.New(brokerconn.Opts{
		Servers: []string{"nats://127.0.0.1:" + itoa(port)},
		SeedKey: seed(t),
	})()
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	require.Equal(t, -1, nc.Opts.MaxReconnect)
	require.Equal(t, brokerconn.DefaultReconnectWait, nc.Opts.ReconnectWait)
	require.True(t, nc.Opts.RetryOnFailedConnect)
}

func waitMsg(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case m := <-ch:
		return m
	case <-time.After(20 * time.Second):
		t.Fatal("no message received")
		return ""
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
