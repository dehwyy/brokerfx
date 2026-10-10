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

func EmbeddedNATS(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}

	current := startEmbedded(
		t,
		opts,
	)
	opts.Port = current.Addr().(*net.TCPAddr).Port

	var mu sync.Mutex
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		current.Shutdown()
		current.WaitForShutdown()
	})

	conn, err := natsgo.Connect(
		current.ClientURL(),
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

	restart := func() {
		mu.Lock()
		defer mu.Unlock()
		current.Shutdown()
		current.WaitForShutdown()
		current = startEmbedded(
			t,
			opts,
		)
	}

	return js, restart
}

func startEmbedded(
	t *testing.T,
	opts *server.Options,
) *server.Server {
	t.Helper()

	srv, err := server.NewServer(opts.Clone())
	require.NoError(
		t,
		err,
	)

	go srv.Start()
	require.True(
		t,
		srv.ReadyForConnections(10*time.Second),
	)

	return srv
}
