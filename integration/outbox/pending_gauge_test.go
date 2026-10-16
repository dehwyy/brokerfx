//go:build integration

package outbox_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type pendingRecorder struct {
	fakeObserver

	mu      sync.Mutex
	pending []outbox.PendingStats
}

func (p *pendingRecorder) OnPending(stats outbox.PendingStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = append(
		p.pending,
		stats,
	)
}

func (p *pendingRecorder) last() (outbox.PendingStats, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) == 0 {
		return outbox.PendingStats{}, false
	}

	return p.pending[len(p.pending)-1], true
}

func explainLines(t *testing.T, db *gorm.DB, query string) string {
	t.Helper()

	rows, err := db.Raw(query).Rows()
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

func TestPendingGaugeCapsCountAndReportsOldest(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	require.NoError(t, outbox.EnsurePendingIndex(context.Background(), db))

	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'DONE', now() - interval '2 hour', now() from generate_series(1, 30000) g").Error)
	require.NoError(t, db.Exec("insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\\x00'::bytea, 'PENDING', now() - interval '10 minute' + g * interval '1 millisecond', now() from generate_series(1, 12000) g").Error)
	require.NoError(t, db.Exec("drop index if exists idx_outbox_events_state, idx_outbox_events_state_updated_at").Error)
	require.NoError(t, db.Exec("analyze outbox_events").Error)

	plan := explainLines(
		t,
		db,
		"explain select (select count(*) from (select 1 from outbox_events where state <> 'DONE' and state = 'PENDING' limit 10001) p), (select created_at from outbox_events where state <> 'DONE' and state = 'PENDING' order by created_at limit 1), now()",
	)
	require.Contains(t, plan, "idx_outbox_events_pending_created_at", plan)

	observer := &pendingRecorder{}
	cfg := batchedConfig(outbox.ModeDeleteAfterSend)
	cfg.PendingGaugeInterval = 100 * time.Millisecond
	harness := testenv.NewRelayHarness(
		t,
		db,
		&testenv.FakeBatchProducer{},
		cfg,
		testenv.WithObserver(observer),
	)
	harness.Relay.Pause()
	harness.Start(t)

	require.Eventually(
		t,
		func() bool {
			_, ok := observer.last()
			return ok
		},
		10*time.Second,
		50*time.Millisecond,
	)
	stats, _ := observer.last()
	require.True(t, stats.Capped)
	require.Equal(t, int64(10000), stats.Pending)
	require.Greater(t, stats.OldestPendingAge, 9*time.Minute)
	require.Less(t, stats.OldestPendingAge, 11*time.Minute)
}
