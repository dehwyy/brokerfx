//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type ackLedger struct {
	mu    sync.Mutex
	acked map[string]int
	lose  atomic.Int64
}

func newAckLedger() *ackLedger {
	return &ackLedger{
		acked: make(map[string]int),
	}
}

func (l *ackLedger) record(event outbox.ProducerEvent, err error) error {
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.acked[event.Headers[jetstream.MsgIDHeader]]++
	l.mu.Unlock()

	if l.lose.Add(-1) >= 0 {
		return context.DeadlineExceeded
	}

	return nil
}

func (l *ackLedger) counts() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	counts := make(
		map[string]int,
		len(l.acked),
	)
	for id, n := range l.acked {
		counts[id] = n
	}

	return counts
}

type ledgerProducer struct {
	inner  *outbox.JetStreamProducer
	ledger *ackLedger
	calls  atomic.Int64
}

func (p *ledgerProducer) Produce(ctx context.Context, event outbox.ProducerEvent) error {
	p.calls.Add(1)
	return p.ledger.record(
		event,
		p.inner.Produce(
			ctx,
			event,
		),
	)
}

func (p *ledgerProducer) ProduceBatch(ctx context.Context, events []outbox.ProducerEvent) []error {
	p.calls.Add(int64(len(events)))
	errs := p.inner.ProduceBatch(
		ctx,
		events,
	)
	for i, event := range events {
		errs[i] = p.ledger.record(
			event,
			errs[i],
		)
	}

	return errs
}

func drainTable(t *testing.T, db *gorm.DB, idType string) {
	t.Helper()

	if idType == "uuid" {
		createUUIDOutboxV2(t, db)
		return
	}
	require.NoError(t, outbox.AutoMigrate(db))
}

func drainConfig(mode outbox.OutboxMode) outbox.Config {
	return outbox.Config{
		Mode:                  mode,
		BatchSize:             50,
		TickInterval:          100 * time.Millisecond,
		DeleteOlderThan:       time.Hour,
		StallThreshold:        5 * time.Minute,
		MaxAttempts:           20,
		RetryBackoffBase:      10 * time.Millisecond,
		RetryBackoffMax:       50 * time.Millisecond,
		TxDrain:               true,
		TxDrainPublishTimeout: 2 * time.Second,
	}
}

func claimConfig(mode outbox.OutboxMode) outbox.Config {
	cfg := drainConfig(mode)
	cfg.TxDrain = false
	cfg.TxDrainPublishTimeout = 0
	cfg.Batched = true
	cfg.Linger = 5 * time.Millisecond
	cfg.ClaimV2 = true

	return cfg
}

func streamMsgs(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()

	stream, err := js.Stream(
		context.Background(),
		name,
	)
	require.NoError(t, err)
	info, err := stream.Info(context.Background())
	require.NoError(t, err)

	return info.State.Msgs
}

func seedDrain(t *testing.T, store *outbox.OutboxStore, prefix string, count int) {
	t.Helper()

	messages := make(
		[]outbox.Message,
		count,
	)
	for i := range messages {
		messages[i] = outbox.Message{
			Subject: "drain.events",
			Payload: []byte(fmt.Sprintf("%s-%04d", prefix, i)),
		}
	}
	require.NoError(t, store.SaveMessages(context.Background(), messages))
}

func activeRows(t *testing.T, db *gorm.DB) int64 {
	t.Helper()

	return countRows(t, db, "select count(*) from outbox_events where state in ('PENDING', 'IN_FLIGHT')")
}

func TestTxDrainWithAnotherRelayOnOneTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		first  outbox.Config
		second outbox.Config
	}{
		{
			name:   "claim update with IN_FLIGHT and tx-drain",
			first:  claimConfig(outbox.ModeUpdateAfterSend),
			second: drainConfig(outbox.ModeDeleteAfterSend),
		},
		{
			name:   "claim delete and tx-drain",
			first:  claimConfig(outbox.ModeDeleteAfterSend),
			second: drainConfig(outbox.ModeDeleteAfterSend),
		},
		{
			name:   "two tx-drain relays",
			first:  drainConfig(outbox.ModeDeleteAfterSend),
			second: drainConfig(outbox.ModeDeleteAfterSend),
		},
		{
			name:   "tx-drain update and tx-drain delete",
			first:  drainConfig(outbox.ModeUpdateAfterSend),
			second: drainConfig(outbox.ModeDeleteAfterSend),
		},
	}

	for _, idType := range []string{"uuid", "text"} {
		for _, tc := range cases {
			idType := idType
			tc := tc
			t.Run(
				idType+"/"+tc.name,
				func(t *testing.T) {
					t.Parallel()

					db := testenv.Postgres(t)
					drainTable(
						t,
						db,
						idType,
					)

					js, _ := testenv.EmbeddedNATS(t)
					newStream(t, js, "DRAIN_MIX", "drain.>")

					ledger := newAckLedger()
					firstProducer := &ledgerProducer{
						inner:  outbox.NewJetStreamProducer(js),
						ledger: ledger,
					}
					secondProducer := &ledgerProducer{
						inner:  outbox.NewJetStreamProducer(js),
						ledger: ledger,
					}
					first := testenv.NewRelayHarness(
						t,
						db,
						firstProducer,
						tc.first,
					)
					second := testenv.NewRelayHarness(
						t,
						db,
						secondProducer,
						tc.second,
					)
					first.Start(t)
					second.Start(t)

					const rounds = 8
					const perRound = 75
					for round := 0; round < rounds; round++ {
						seedDrain(
							t,
							first.Store,
							fmt.Sprintf("r%d", round),
							perRound,
						)
						first.Wake()
						second.Wake()
						time.Sleep(20 * time.Millisecond)
					}

					total := rounds * perRound
					require.Eventually(
						t,
						func() bool {
							return activeRows(t, db) == 0 && streamMsgs(t, js, "DRAIN_MIX") == uint64(total)
						},
						30*time.Second,
						50*time.Millisecond,
					)

					t.Logf("first relay %d, second relay %d", firstProducer.calls.Load(), secondProducer.calls.Load())
					counts := ledger.counts()
					require.Len(t, counts, total)
					for id, n := range counts {
						require.Equal(t, 1, n, "event %s published %d times", id, n)
					}
				},
			)
		}
	}
}

func TestTxDrainLostAckKeepsOneCopy(t *testing.T) {
	t.Parallel()

	for _, idType := range []string{"uuid", "text"} {
		idType := idType
		t.Run(
			idType,
			func(t *testing.T) {
				t.Parallel()

				db := testenv.Postgres(t)
				drainTable(
					t,
					db,
					idType,
				)

				js, _ := testenv.EmbeddedNATS(t)
				newStream(t, js, "DRAIN_LOST", "drain.>")

				ledger := newAckLedger()
				ledger.lose.Store(30)
				harness := testenv.NewRelayHarness(
					t,
					db,
					&ledgerProducer{
						inner:  outbox.NewJetStreamProducer(js),
						ledger: ledger,
					},
					drainConfig(outbox.ModeDeleteAfterSend),
				)
				seedDrain(
					t,
					harness.Store,
					"lost",
					100,
				)
				harness.Start(t)
				harness.Wake()

				require.Eventually(
					t,
					func() bool {
						return countRows(t, db, "select count(*) from outbox_events") == 0
					},
					30*time.Second,
					50*time.Millisecond,
				)
				require.Equal(t, uint64(100), streamMsgs(t, js, "DRAIN_LOST"))
				require.Equal(t, int64(30), countRows(t, db, "select count(*) from outbox_retries"))

				republished := 0
				for _, n := range ledger.counts() {
					if n > 1 {
						republished++
					}
				}
				require.Equal(t, 30, republished)
			},
		)
	}
}

func TestTxDrainNATSDownKeepsRowsAndOneCopy(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js, control := testenv.EmbeddedNATSControl(t)
	newStream(t, js, "DRAIN_DOWN", "drain.>")

	cfg := drainConfig(outbox.ModeDeleteAfterSend)
	cfg.TxDrainPublishTimeout = 300 * time.Millisecond
	harness := testenv.NewRelayHarness(
		t,
		db,
		outbox.NewJetStreamProducer(js),
		cfg,
	)
	harness.Start(t)

	control.Stop()
	seedDrain(
		t,
		harness.Store,
		"down",
		60,
	)
	harness.Wake()

	require.Eventually(
		t,
		func() bool {
			return countRows(t, db, "select count(*) from outbox_retries") >= 60
		},
		20*time.Second,
		50*time.Millisecond,
	)
	require.Equal(t, int64(60), countRows(t, db, "select count(*) from outbox_events where state = 'PENDING'"))
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events where attempts <> 0"))

	control.Start()

	require.Eventually(
		t,
		func() bool {
			return countRows(t, db, "select count(*) from outbox_events") == 0
		},
		30*time.Second,
		100*time.Millisecond,
	)
	require.Eventually(
		t,
		func() bool {
			return streamMsgs(t, js, "DRAIN_DOWN") == 60
		},
		10*time.Second,
		100*time.Millisecond,
	)
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, uint64(60), streamMsgs(t, js, "DRAIN_DOWN"))
}

func TestTxDrainServerRefusalCountsAttempts(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js, _ := testenv.EmbeddedNATS(t)

	cfg := drainConfig(outbox.ModeDeleteAfterSend)
	cfg.MaxAttempts = 2
	observer := &fakeObserver{}
	harness := testenv.NewRelayHarness(
		t,
		db,
		outbox.NewJetStreamProducer(js),
		cfg,
		testenv.WithObserver(observer),
	)
	seedDrain(
		t,
		harness.Store,
		"nostream",
		3,
	)
	harness.Start(t)
	harness.Wake()

	require.Eventually(
		t,
		func() bool {
			return countRows(t, db, "select count(*) from outbox_events where state = 'PARKED' and attempts = 2") == 3
		},
		20*time.Second,
		50*time.Millisecond,
	)
	require.Eventually(
		t,
		func() bool {
			return observer.parkedCount() == 3
		},
		5*time.Second,
		50*time.Millisecond,
	)
}

func TestTxDrainStreamWithoutAckDoesNotStarveOtherSubjects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mode outbox.OutboxMode
	}{
		{
			name: "delete after send",
			mode: outbox.ModeDeleteAfterSend,
		},
		{
			name: "update after send",
			mode: outbox.ModeUpdateAfterSend,
		},
	}

	for _, tc := range cases {
		mode := tc.mode
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()

				db := testenv.Postgres(t)
				require.NoError(t, outbox.AutoMigrate(db))

				js, _ := testenv.EmbeddedNATS(t)
				newStream(t, js, "DRAIN_OK", "drain.ok.>")
				silent, err := js.Conn().Subscribe(
					"drain.stuck.>",
					func(*natsgo.Msg) {},
				)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, silent.Unsubscribe())
				})

				cfg := drainConfig(mode)
				cfg.TxDrainPublishTimeout = 300 * time.Millisecond
				harness := testenv.NewRelayHarness(
					t,
					db,
					outbox.NewJetStreamProducer(js),
					cfg,
				)

				stuck := make(
					[]outbox.Message,
					cfg.BatchSize+10,
				)
				for i := range stuck {
					stuck[i] = outbox.Message{
						Subject: "drain.stuck.result",
						Payload: []byte(fmt.Sprintf("stuck-%04d", i)),
					}
				}
				require.NoError(t, harness.Store.SaveMessages(context.Background(), stuck))
				ok := make(
					[]outbox.Message,
					40,
				)
				for i := range ok {
					ok[i] = outbox.Message{
						Subject: "drain.ok.completed",
						Payload: []byte(fmt.Sprintf("ok-%04d", i)),
					}
				}
				require.NoError(t, harness.Store.SaveMessages(context.Background(), ok))

				harness.Start(t)
				harness.Wake()

				require.Eventually(
					t,
					func() bool {
						return streamMsgs(t, js, "DRAIN_OK") == uint64(len(ok)) &&
							countRows(t, db, "select count(*) from outbox_events where topic = 'drain.ok.completed' and state = 'PENDING'") == 0
					},
					4*time.Second,
					50*time.Millisecond,
				)
				require.Equal(t, int64(len(stuck)), countRows(t, db, "select count(*) from outbox_events where topic = 'drain.stuck.result' and state = 'PENDING' and attempts = 0"))
				require.Equal(t, int64(len(stuck)), countRows(t, db, "select count(*) from outbox_events where topic = 'drain.stuck.result' and next_attempt_at > now()"))
			},
		)
	}
}
