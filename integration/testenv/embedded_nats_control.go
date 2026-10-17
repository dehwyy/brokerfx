package testenv

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type NATSControl struct {
	mu      sync.Mutex
	t       *testing.T
	opts    *server.Options
	current *server.Server
}

func EmbeddedNATSControl(t *testing.T) (jetstream.JetStream, *NATSControl) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}

	control := &NATSControl{
		t:    t,
		opts: opts,
	}
	control.current = startEmbedded(
		t,
		opts,
	)
	opts.Port = control.current.Addr().(*net.TCPAddr).Port
	t.Cleanup(control.Stop)

	conn, err := natsgo.Connect(
		control.current.ClientURL(),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(50*time.Millisecond),
	)
	require.NoError(
		t,
		err,
	)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(
		t,
		err,
	)

	return js, control
}

func (c *NATSControl) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		return
	}
	c.current.Shutdown()
	c.current.WaitForShutdown()
	c.current = nil
}

func (c *NATSControl) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		return
	}
	c.current = startEmbedded(
		c.t,
		c.opts,
	)
}
