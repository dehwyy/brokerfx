package testenv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func Postgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(
		ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("brokerfx"),
		tcpostgres.WithUsername("brokerfx"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
		tcpostgres.WithSQLDriver("pgx"),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	var db *gorm.DB
	require.Eventually(t, func() bool {
		var openErr error
		db, openErr = gorm.Open(postgres.Open(dsn), &gorm.Config{})
		return openErr == nil
	}, 30*time.Second, 500*time.Millisecond)

	return db
}

func NATS(t *testing.T) jetstream.JetStream {
	t.Helper()
	ctx := context.Background()

	container, err := tcnats.Run(ctx, "nats:2.10-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	var conn *natsgo.Conn
	require.Eventually(t, func() bool {
		var connErr error
		conn, connErr = natsgo.Connect(url)
		return connErr == nil
	}, 30*time.Second, 500*time.Millisecond)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	return js
}

type RelayHarness struct {
	Store *outbox.OutboxStore
	Relay *outbox.OutboxRelay

	cancel context.CancelFunc
	done   <-chan struct{}
}

type RelayHarnessOption func(*outbox.RelayDeps)

func WithSigner(signer outbox.Signer) RelayHarnessOption {
	return func(deps *outbox.RelayDeps) {
		deps.Signer = signer
	}
}

func NewRelayHarness(t *testing.T, db *gorm.DB, producer outbox.Producer, cfg outbox.Config, opts ...RelayHarnessOption) *RelayHarness {
	t.Helper()

	txm, err := gormtx.New(gormtx.Opts{DB: db})
	require.NoError(t, err)

	store := outbox.NewStore(outbox.StoreDeps{
		DB:        db,
		TxManager: txm,
	})

	deps := outbox.RelayDeps{
		Store:    store,
		Producer: producer,
		Config:   cfg,
	}
	for _, opt := range opts {
		opt(&deps)
	}

	relay := outbox.NewRelay(deps)

	return &RelayHarness{
		Store: store,
		Relay: relay,
	}
}

func (h *RelayHarness) Start(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = h.Relay.Done()

	go h.Relay.Run(ctx)

	t.Cleanup(h.Stop)
}

func (h *RelayHarness) Stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	<-h.done
}

func (h *RelayHarness) Wake() {
	h.Store.WakeupRelay()
}

type FakeProducer struct {
	mu    sync.Mutex
	Fail  func(outbox.ProducerEvent) error
	Calls []outbox.ProducerEvent
}

func (p *FakeProducer) Produce(_ context.Context, event outbox.ProducerEvent) error {
	p.mu.Lock()
	p.Calls = append(p.Calls, event)
	p.mu.Unlock()

	if p.Fail != nil {
		return p.Fail(event)
	}

	return nil
}

func (p *FakeProducer) CallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.Calls)
}
