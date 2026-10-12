//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager/gormtx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type claimSeed struct {
	ID            string
	Payload       string
	State         outbox.OutboxState
	CreatedAt     time.Time
	UpdatedAt     time.Time
	NextAttemptAt *time.Time
}

type claimRun struct {
	Published []string
	States    map[string]outbox.OutboxState
}

func claimV2Config(mode outbox.OutboxMode, claimV2 bool) outbox.Config {
	cfg := batchedConfig(mode)
	cfg.BatchSize = 10
	cfg.ClaimV2 = claimV2

	return cfg
}

func claimSeeds() []claimSeed {
	base := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	future := time.Now().Add(time.Hour)
	seeds := make(
		[]claimSeed,
		0,
		64,
	)
	for i := 0; i < 57; i++ {
		seeds = append(
			seeds,
			claimSeed{
				ID:        fmt.Sprintf("00000000-0000-0000-0000-%012d", i),
				Payload:   fmt.Sprintf("p-%02d", i),
				State:     outbox.StatePending,
				CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
				UpdatedAt: base,
			},
		)
	}
	seeds = append(
		seeds,
		claimSeed{
			ID:            "00000000-0000-0000-0001-000000000001",
			Payload:       "backoff",
			State:         outbox.StatePending,
			CreatedAt:     base.Add(-time.Minute),
			UpdatedAt:     base,
			NextAttemptAt: &future,
		},
		claimSeed{
			ID:        "00000000-0000-0000-0001-000000000002",
			Payload:   "parked",
			State:     outbox.StateParked,
			CreatedAt: base.Add(-time.Minute),
			UpdatedAt: base,
		},
		claimSeed{
			ID:        "00000000-0000-0000-0001-000000000003",
			Payload:   "done",
			State:     outbox.StateDone,
			CreatedAt: base.Add(-time.Minute),
			UpdatedAt: time.Now(),
		},
		claimSeed{
			ID:        "00000000-0000-0000-0001-000000000004",
			Payload:   "stalled",
			State:     outbox.StateInFlight,
			CreatedAt: base.Add(-2 * time.Minute),
			UpdatedAt: time.Now().Add(-time.Hour),
		},
		claimSeed{
			ID:        "00000000-0000-0000-0001-000000000005",
			Payload:   "in-flight-fresh",
			State:     outbox.StateInFlight,
			CreatedAt: base.Add(-2 * time.Minute),
			UpdatedAt: time.Now().Add(time.Hour),
		},
	)

	return seeds
}

func seedClaimRows(t *testing.T, db *gorm.DB, seeds []claimSeed) {
	t.Helper()

	require.NoError(
		t,
		db.Exec("truncate outbox_events, outbox_retries").Error,
	)
	for _, seed := range seeds {
		require.NoError(
			t,
			db.Exec(
				"insert into outbox_events (id, topic, payload, state, created_at, updated_at, next_attempt_at) values (?, ?, ?, ?, ?, ?, ?)",
				seed.ID,
				"claim.v2",
				[]byte(seed.Payload),
				seed.State,
				seed.CreatedAt,
				seed.UpdatedAt,
				seed.NextAttemptAt,
			).Error,
		)
	}
}

func outboxStates(t *testing.T, db *gorm.DB) map[string]outbox.OutboxState {
	t.Helper()

	var rows []struct {
		ID    string
		State outbox.OutboxState
	}
	require.NoError(
		t,
		db.Raw("select id, state from outbox_events").Scan(&rows).Error,
	)

	states := make(
		map[string]outbox.OutboxState,
		len(rows),
	)
	for _, row := range rows {
		states[row.ID] = row.State
	}

	return states
}

func runClaim(t *testing.T, db *gorm.DB, cfg outbox.Config) claimRun {
	t.Helper()

	seeds := claimSeeds()
	seedClaimRows(
		t,
		db,
		seeds,
	)

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(
		t,
		db,
		producer,
		cfg,
	)
	harness.Start(t)
	harness.Wake()

	require.Eventually(
		t,
		func() bool {
			harness.Wake()
			return countRows(
				t,
				db,
				"select count(*) from outbox_events where state = ?",
				outbox.StatePending,
			) == 1 && producer.CallCount() == 58
		},
		20*time.Second,
		50*time.Millisecond,
	)
	harness.Stop()

	published := make(
		[]string,
		0,
		producer.CallCount(),
	)
	for _, call := range producer.Calls {
		published = append(
			published,
			string(call.Payload),
		)
	}

	return claimRun{
		Published: published,
		States:    outboxStates(t, db),
	}
}

func TestClaimV2BatchedOffEqualsOn(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	off := runClaim(
		t,
		db,
		claimV2Config(
			outbox.ModeUpdateAfterSend,
			false,
		),
	)
	on := runClaim(
		t,
		db,
		claimV2Config(
			outbox.ModeUpdateAfterSend,
			true,
		),
	)

	require.Equal(t, off.Published, on.Published)
	require.Equal(t, off.States, on.States)
	require.Equal(t, "stalled", on.Published[0])
	require.Equal(t, outbox.StatePending, on.States["00000000-0000-0000-0001-000000000001"])
	require.Equal(t, outbox.StateParked, on.States["00000000-0000-0000-0001-000000000002"])
	require.Equal(t, outbox.StateInFlight, on.States["00000000-0000-0000-0001-000000000005"])
	require.NotContains(t, on.Published, "backoff")
	require.NotContains(t, on.Published, "parked")
	require.NotContains(t, on.Published, "done")
	require.NotContains(t, on.Published, "in-flight-fresh")
}

func TestClaimV2UnbatchedOffEqualsOn(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	offConfig := claimV2Config(
		outbox.ModeUpdateAfterSend,
		false,
	)
	offConfig.Batched = false
	onConfig := claimV2Config(
		outbox.ModeUpdateAfterSend,
		true,
	)
	onConfig.Batched = false

	off := runClaim(
		t,
		db,
		offConfig,
	)
	on := runClaim(
		t,
		db,
		onConfig,
	)

	sort.Strings(off.Published)
	sort.Strings(on.Published)
	require.Equal(t, off.Published, on.Published)
	require.Equal(t, off.States, on.States)
}

func TestClaimV2ConcurrentRelaysPublishEachEventOnce(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	cfg := claimV2Config(
		outbox.ModeDeleteAfterSend,
		true,
	)
	cfg.BatchSize = 20
	cfg.Linger = 5 * time.Millisecond

	producers := make(
		[]*testenv.FakeBatchProducer,
		3,
	)
	harnesses := make(
		[]*testenv.RelayHarness,
		3,
	)
	for i := range producers {
		producers[i] = &testenv.FakeBatchProducer{}
		harnesses[i] = testenv.NewRelayHarness(
			t,
			db,
			producers[i],
			cfg,
		)
		harnesses[i].Start(t)
	}

	const total = 600
	for chunk := 0; chunk < total/50; chunk++ {
		messages := make(
			[]outbox.Message,
			50,
		)
		for i := range messages {
			messages[i] = outbox.Message{
				Subject: "claim.v2.concurrent",
				Payload: []byte(fmt.Sprintf("c-%04d", chunk*50+i)),
			}
		}
		require.NoError(
			t,
			harnesses[chunk%3].Store.SaveMessages(
				context.Background(),
				messages,
			),
		)
		for _, harness := range harnesses {
			harness.Wake()
		}
	}

	require.Eventually(
		t,
		func() bool {
			for _, harness := range harnesses {
				harness.Wake()
			}
			return countRows(
				t,
				db,
				"select count(*) from outbox_events",
			) == 0
		},
		30*time.Second,
		50*time.Millisecond,
	)
	for _, harness := range harnesses {
		harness.Stop()
	}

	seen := make(
		map[string]int,
		total,
	)
	payloads := make(
		map[string]struct{},
		total,
	)
	for _, producer := range producers {
		for _, call := range producer.Calls {
			seen[call.Headers["Nats-Msg-Id"]]++
			payloads[string(call.Payload)] = struct{}{}
		}
	}
	require.Len(t, payloads, total)
	require.Len(t, seen, total)
	for id, count := range seen {
		require.Equal(t, 1, count, "event %s published %d times", id, count)
	}
}

func TestClaimV2ReclaimsStalledOnce(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	stalledID := uuid.NewString()
	freshID := uuid.NewString()
	for _, id := range []string{
		stalledID,
		freshID,
	} {
		insertEvent(
			t,
			db,
			&outbox.OutboxEvent{
				ID:      id,
				Topic:   "claim.v2.stalled",
				Payload: []byte(id),
			},
		)
	}
	require.NoError(
		t,
		db.Exec(
			"update outbox_events set state = ?, updated_at = ? where id = ?",
			outbox.StateInFlight,
			time.Now().Add(-time.Hour),
			stalledID,
		).Error,
	)
	require.NoError(
		t,
		db.Exec(
			"update outbox_events set state = ?, updated_at = ? where id = ?",
			outbox.StateInFlight,
			time.Now(),
			freshID,
		).Error,
	)

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(
		t,
		db,
		producer,
		claimV2Config(
			outbox.ModeDeleteAfterSend,
			true,
		),
	)
	harness.Start(t)
	harness.Wake()

	require.Eventually(
		t,
		func() bool {
			return producer.CallCount() == 1 && !eventExists(t, db, stalledID)
		},
		10*time.Second,
		50*time.Millisecond,
	)

	harness.Wake()
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, producer.CallCount())
	require.Equal(t, stalledID, producer.Calls[0].Headers["Nats-Msg-Id"])
	require.Equal(t, outbox.StateInFlight, loadEvent(t, db, freshID).State)
}

type statementCounter struct {
	count atomic.Int64
}

func (c *statementCounter) register(t *testing.T, db *gorm.DB) {
	t.Helper()

	count := func(*gorm.DB) {
		c.count.Add(1)
	}
	callbacks := db.Callback()
	require.NoError(t, callbacks.Create().After("gorm:create").Register("budget:create", count))
	require.NoError(t, callbacks.Query().After("gorm:query").Register("budget:query", count))
	require.NoError(t, callbacks.Update().After("gorm:update").Register("budget:update", count))
	require.NoError(t, callbacks.Delete().After("gorm:delete").Register("budget:delete", count))
	require.NoError(t, callbacks.Row().After("gorm:row").Register("budget:row", count))
	require.NoError(t, callbacks.Raw().After("gorm:raw").Register("budget:raw", count))
}

func TestClaimV2QueryBudget(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	counter := &statementCounter{}
	counter.register(
		t,
		db,
	)

	cfg := claimV2Config(
		outbox.ModeDeleteAfterSend,
		true,
	)
	cfg.BatchSize = 100
	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(
		t,
		db,
		producer,
		cfg,
	)

	warmup := []outbox.Message{
		{
			Subject: "claim.v2.budget",
			Payload: []byte("warmup"),
		},
	}
	require.NoError(t, harness.Store.SaveMessages(context.Background(), warmup))
	harness.Start(t)
	harness.Wake()
	require.Eventually(
		t,
		func() bool {
			return producer.CallCount() == 1
		},
		10*time.Second,
		20*time.Millisecond,
	)
	time.Sleep(300 * time.Millisecond)

	messages := make(
		[]outbox.Message,
		40,
	)
	for i := range messages {
		messages[i] = outbox.Message{
			Subject: "claim.v2.budget",
			Payload: []byte(fmt.Sprintf("b-%02d", i)),
		}
	}

	counter.count.Store(0)
	require.NoError(t, harness.Store.SaveMessages(context.Background(), messages))
	require.Equal(t, int64(1), counter.count.Load(), "SaveMessages of 40 rows must be one INSERT")

	counter.count.Store(0)
	harness.Wake()
	time.Sleep(time.Second)
	require.Equal(t, 41, producer.CallCount())
	require.Equal(t, int64(2), counter.count.Load(), "one claim and one delete per relay pass")
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events"))
}

func TestSaveMessagesKeepsOrderAndHeaders(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	producer := &testenv.FakeBatchProducer{}
	cfg := claimV2Config(
		outbox.ModeDeleteAfterSend,
		true,
	)
	cfg.BatchSize = 7
	harness := testenv.NewRelayHarness(
		t,
		db,
		producer,
		cfg,
	)

	messages := make(
		[]outbox.Message,
		120,
	)
	want := make(
		[]string,
		len(messages),
	)
	for i := range messages {
		want[i] = fmt.Sprintf("o-%03d", i)
		messages[i] = outbox.Message{
			Subject: "claim.v2.order",
			Payload: []byte(want[i]),
		}
	}
	require.NoError(
		t,
		harness.Store.SaveMessages(
			context.Background(),
			messages,
			outbox.WithHeaders(
				map[string]string{
					"Paylonium-Envelope": "v1",
				},
			),
		),
	)

	harness.Start(t)
	harness.Wake()
	require.Eventually(
		t,
		func() bool {
			return producer.CallCount() == len(messages)
		},
		20*time.Second,
		50*time.Millisecond,
	)

	got := make(
		[]string,
		0,
		len(messages),
	)
	for _, call := range producer.Calls {
		got = append(
			got,
			string(call.Payload),
		)
		require.Equal(t, "v1", call.Headers["Paylonium-Envelope"])
	}
	require.Equal(t, want, got)
}

func TestSaveMessagesRollsBackWithTransaction(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	txm, err := gormtx.New(
		gormtx.Opts{
			DB: db,
		},
	)
	require.NoError(t, err)
	store := outbox.NewStore(
		outbox.StoreDeps{
			DB:        db,
			TxManager: txm,
		},
	)

	errAbort := errors.New("abort")
	err = txm.Do(
		context.Background(),
		"claim.v2.rollback",
		func(ctx context.Context) error {
			if err := store.SaveMessages(
				ctx,
				[]outbox.Message{
					{
						Subject: "claim.v2.tx",
						Payload: []byte("a"),
					},
					{
						Subject: "claim.v2.tx",
						Payload: []byte("b"),
					},
				},
			); err != nil {
				return err
			}

			return errAbort
		},
	)
	require.ErrorIs(t, err, errAbort)
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events"))

	require.NoError(
		t,
		store.SaveMessages(
			context.Background(),
			nil,
		),
	)
	require.ErrorIs(
		t,
		store.SaveMessages(
			context.Background(),
			[]outbox.Message{
				{
					Subject: "ok",
				},
				outbox.KVPut(
					"bad bucket",
					"key",
					nil,
				),
			},
		),
		outbox.ErrInvalidKVBucket,
	)
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events"))
}

func TestSaveMessagesLegacySchema(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)
	store := newStoreForTest(
		t,
		db,
	)

	require.ErrorIs(
		t,
		store.SaveMessages(
			context.Background(),
			[]outbox.Message{
				{
					Subject: "legacy",
					Payload: []byte("a"),
				},
			},
			outbox.WithHeaders(
				map[string]string{
					"X-Trace": "1",
				},
			),
		),
		outbox.ErrSchemaOutdated,
	)
	require.Equal(t, int64(0), countRows(t, db, "select count(*) from outbox_events"))

	require.NoError(
		t,
		store.SaveMessages(
			context.Background(),
			[]outbox.Message{
				{
					Subject: "legacy",
					Payload: []byte("a"),
				},
				{
					Subject: "legacy",
					Payload: []byte("b"),
				},
			},
		),
	)
	require.Equal(t, int64(2), countRows(t, db, "select count(*) from outbox_events where state = ?", outbox.StatePending))
}

func explainClaimV2(t *testing.T, db *gorm.DB) string {
	t.Helper()

	rows, err := db.Raw("explain select id from outbox_events where state <> 'DONE' and state = 'PENDING' and (next_attempt_at is null or next_attempt_at <= now()) order by created_at limit 100 for update skip locked").Rows()
	require.NoError(t, err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(
			lines,
			line,
		)
	}
	require.NoError(t, rows.Err())

	return strings.Join(
		lines,
		"\n",
	)
}

func TestEnsurePendingIndex(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.EnsurePendingIndex(context.Background(), db))

	require.NoError(t, outbox.AutoMigrate(db))
	require.NoError(t, outbox.EnsurePendingIndex(context.Background(), db))
	require.NoError(t, outbox.EnsurePendingIndex(context.Background(), db))

	var valid bool
	require.NoError(
		t,
		db.Raw("select i.indisvalid from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'idx_outbox_events_pending_created_at'").Scan(&valid).Error,
	)
	require.True(t, valid)

	require.NoError(
		t,
		db.Exec("update pg_index set indisvalid = false where indexrelid = 'idx_outbox_events_pending_created_at'::regclass").Error,
	)
	require.NoError(t, outbox.EnsurePendingIndex(context.Background(), db))
	require.NoError(
		t,
		db.Raw("select i.indisvalid from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'idx_outbox_events_pending_created_at'").Scan(&valid).Error,
	)
	require.True(t, valid)

	require.NoError(
		t,
		db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'DONE', now() - interval '1 hour' + g * interval '1 millisecond', now() from generate_series(1, 20000) g").Error,
	)
	require.NoError(
		t,
		db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'PENDING', now() + g * interval '1 millisecond', now() from generate_series(1, 20) g").Error,
	)
	require.NoError(t, db.Exec("drop index if exists idx_outbox_events_state, idx_outbox_events_state_updated_at").Error)
	require.NoError(t, db.Exec("analyze outbox_events").Error)

	plan := explainClaimV2(
		t,
		db,
	)
	require.Contains(t, plan, "idx_outbox_events_pending_created_at", plan)
}
