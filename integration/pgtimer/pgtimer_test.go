//go:build integration

package pgtimer_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/pgtimer"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const timeoutName = "TIMEOUT"

type fireLedger struct {
	mu    sync.Mutex
	fired map[string]int
}

func newFireLedger() *fireLedger {
	return &fireLedger{
		fired: map[string]int{},
	}
}

func (l *fireLedger) record(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fired[key]++

	return l.fired[key]
}

func (l *fireLedger) snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	copied := make(
		map[string]int,
		len(l.fired),
	)
	for key, count := range l.fired {
		copied[key] = count
	}

	return copied
}

type countingObserver struct {
	claimed atomic.Int64
	fired   atomic.Int64
	deleted atomic.Int64
	failed  atomic.Int64
}

func (o *countingObserver) OnClaim(event pgtimer.ClaimEvent) {
	o.claimed.Add(int64(event.Claimed))
}

func (o *countingObserver) OnFire(event pgtimer.FireEvent) {
	o.fired.Add(1)
	if event.Deleted {
		o.deleted.Add(1)
	}
	if event.Err != nil {
		o.failed.Add(1)
	}
}

func createTimers(
	t *testing.T,
	db *gorm.DB,
	table string,
	keyType string,
) pgtimer.Table {
	t.Helper()
	require.NoError(
		t,
		db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (
	order_id   %s PRIMARY KEY,
	name       VARCHAR(64) NOT NULL,
	attempt_id TEXT NOT NULL DEFAULT '',
	due_at     TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
			table,
			keyType,
		)).Error,
	)
	require.NoError(
		t,
		db.Exec(fmt.Sprintf(
			`CREATE INDEX ON %s (due_at)`,
			table,
		)).Error,
	)

	return pgtimer.Table{
		Name:      table,
		Key:       "order_id",
		TimerName: "name",
		Attempt:   "attempt_id",
		DueAt:     "due_at",
		CreatedAt: "created_at",
		UpdatedAt: "updated_at",
	}
}

func seedDue(
	t *testing.T,
	db *gorm.DB,
	table string,
	name string,
	count int,
) {
	t.Helper()
	require.NoError(
		t,
		db.Exec(
			fmt.Sprintf(
				`INSERT INTO %s (order_id, name, attempt_id, due_at)
SELECT gen_random_uuid()::text, ?, 'a1', now() - interval '1 second' FROM generate_series(1, ?)`,
				table,
			),
			name,
			count,
		).Error,
	)
}

func rowCount(
	t *testing.T,
	db *gorm.DB,
	table string,
) int64 {
	t.Helper()
	var count int64
	require.NoError(
		t,
		db.Raw(fmt.Sprintf(
			`SELECT count(*) FROM %s`,
			table,
		)).Scan(&count).Error,
	)

	return count
}

func newPoller(
	t *testing.T,
	db *gorm.DB,
	cfg pgtimer.Config,
	handler pgtimer.HandlerFunc,
) *pgtimer.Poller {
	t.Helper()
	poller, err := pgtimer.New(
		db,
		cfg,
	)
	require.NoError(
		t,
		err,
	)
	require.NoError(
		t,
		poller.Handle(
			timeoutName,
			handler,
		),
	)
	require.NoError(
		t,
		poller.Start(),
	)

	return poller
}

func stopPoller(
	t *testing.T,
	poller *pgtimer.Poller,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()
	require.NoError(
		t,
		poller.Stop(ctx),
	)
}

func TestPgTimer(t *testing.T) {
	db := testenv.Postgres(t)

	t.Run(
		"two pollers fire every timer once",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_once",
				"VARCHAR(36)",
			)
			seedDue(
				t,
				db,
				"timers_once",
				timeoutName,
				300,
			)
			ledger := newFireLedger()
			handler := func(_ context.Context, timer pgtimer.Timer) error {
				ledger.record(timer.Key)
				time.Sleep(2 * time.Millisecond)
				return nil
			}
			first := &countingObserver{}
			second := &countingObserver{}
			cfg := pgtimer.Config{
				Table:          table,
				Lease:          20 * time.Second,
				HandlerTimeout: 5 * time.Second,
				Poll:           50 * time.Millisecond,
				Batch:          16,
				Concurrency:    8,
			}
			cfg.Observer = first
			a := newPoller(
				t,
				db,
				cfg,
				handler,
			)
			cfg.Observer = second
			b := newPoller(
				t,
				db,
				cfg,
				handler,
			)

			require.Eventually(
				t,
				func() bool {
					return rowCount(
						t,
						db,
						"timers_once",
					) == 0
				},
				30*time.Second,
				50*time.Millisecond,
			)
			stopPoller(
				t,
				a,
			)
			stopPoller(
				t,
				b,
			)

			fired := ledger.snapshot()
			require.Len(
				t,
				fired,
				300,
			)
			for key, count := range fired {
				require.Equal(
					t,
					1,
					count,
					key,
				)
			}
			require.Positive(
				t,
				first.deleted.Load(),
			)
			require.Positive(
				t,
				second.deleted.Load(),
			)
			require.Equal(
				t,
				int64(300),
				first.deleted.Load()+second.deleted.Load(),
			)
		},
	)

	t.Run(
		"failed handler fires again after the lease",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_retry",
				"VARCHAR(36)",
			)
			seedDue(
				t,
				db,
				"timers_retry",
				timeoutName,
				5,
			)
			ledger := newFireLedger()
			poller := newPoller(
				t,
				db,
				pgtimer.Config{
					Table:          table,
					Lease:          time.Second,
					HandlerTimeout: 500 * time.Millisecond,
					Poll:           50 * time.Millisecond,
				},
				func(_ context.Context, timer pgtimer.Timer) error {
					if ledger.record(timer.Key) == 1 {
						return errors.New("transient")
					}
					return nil
				},
			)
			defer stopPoller(
				t,
				poller,
			)

			require.Eventually(
				t,
				func() bool {
					return rowCount(
						t,
						db,
						"timers_retry",
					) == 0
				},
				15*time.Second,
				50*time.Millisecond,
			)
			for key, count := range ledger.snapshot() {
				require.Equal(
					t,
					2,
					count,
					key,
				)
			}
		},
	)

	t.Run(
		"timer rescheduled during handling is kept",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_resched",
				"VARCHAR(36)",
			)
			seedDue(
				t,
				db,
				"timers_resched",
				timeoutName,
				3,
			)
			done := make(
				chan string,
				3,
			)
			poller := newPoller(
				t,
				db,
				pgtimer.Config{
					Table:          table,
					Lease:          10 * time.Second,
					HandlerTimeout: 5 * time.Second,
					Poll:           50 * time.Millisecond,
				},
				func(ctx context.Context, timer pgtimer.Timer) error {
					err := db.WithContext(ctx).
						Exec(
							`UPDATE timers_resched SET due_at = now() + interval '1 hour', attempt_id = 'a2', updated_at = now() WHERE order_id = ?`,
							timer.Key,
						).
						Error
					done <- timer.Key
					return err
				},
			)

			for range 3 {
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("timer did not fire")
				}
			}
			stopPoller(
				t,
				poller,
			)

			var kept int64
			require.NoError(
				t,
				db.Raw(`SELECT count(*) FROM timers_resched WHERE attempt_id = 'a2' AND due_at > now() + interval '50 minutes'`).
					Scan(&kept).
					Error,
			)
			require.Equal(
				t,
				int64(3),
				kept,
			)
		},
	)

	t.Run(
		"stop in the middle of a batch loses nothing",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_stop",
				"VARCHAR(36)",
			)
			seedDue(
				t,
				db,
				"timers_stop",
				timeoutName,
				40,
			)
			ledger := newFireLedger()
			entered := make(
				chan struct{},
				40,
			)
			blocking := func(ctx context.Context, timer pgtimer.Timer) error {
				entered <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			cfg := pgtimer.Config{
				Table:          table,
				Lease:          time.Second,
				HandlerTimeout: 800 * time.Millisecond,
				Poll:           50 * time.Millisecond,
				Batch:          8,
				Concurrency:    4,
			}
			first := newPoller(
				t,
				db,
				cfg,
				blocking,
			)
			for range 4 {
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("handlers did not start")
				}
			}
			stopCtx, cancel := context.WithTimeout(
				context.Background(),
				50*time.Millisecond,
			)
			stopErr := first.Stop(stopCtx)
			cancel()
			require.ErrorIs(
				t,
				stopErr,
				context.DeadlineExceeded,
			)
			require.Equal(
				t,
				int64(40),
				rowCount(
					t,
					db,
					"timers_stop",
				),
			)

			observer := &countingObserver{}
			cfg.Observer = observer
			second := newPoller(
				t,
				db,
				cfg,
				func(_ context.Context, timer pgtimer.Timer) error {
					ledger.record(timer.Key)
					return nil
				},
			)
			defer stopPoller(
				t,
				second,
			)

			require.Eventually(
				t,
				func() bool {
					return rowCount(
						t,
						db,
						"timers_stop",
					) == 0
				},
				15*time.Second,
				50*time.Millisecond,
			)
			require.Len(
				t,
				ledger.snapshot(),
				40,
			)
			require.Equal(
				t,
				int64(40),
				observer.deleted.Load(),
			)
		},
	)

	t.Run(
		"graceful stop finishes claimed timers",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_drain",
				"VARCHAR(36)",
			)
			seedDue(
				t,
				db,
				"timers_drain",
				timeoutName,
				20,
			)
			ledger := newFireLedger()
			entered := make(
				chan struct{},
				20,
			)
			poller := newPoller(
				t,
				db,
				pgtimer.Config{
					Table:          table,
					Lease:          10 * time.Second,
					HandlerTimeout: 5 * time.Second,
					Poll:           time.Hour,
					Batch:          4,
					Concurrency:    4,
				},
				func(_ context.Context, timer pgtimer.Timer) error {
					entered <- struct{}{}
					time.Sleep(200 * time.Millisecond)
					ledger.record(timer.Key)
					return nil
				},
			)
			<-entered
			stopPoller(
				t,
				poller,
			)

			fired := len(ledger.snapshot())
			require.Positive(
				t,
				fired,
			)
			require.Equal(
				t,
				int64(20-fired),
				rowCount(
					t,
					db,
					"timers_drain",
				),
			)
			var leased int64
			require.NoError(
				t,
				db.Raw(`SELECT count(*) FROM timers_drain WHERE due_at > now()`).
					Scan(&leased).
					Error,
			)
			require.Zero(
				t,
				leased,
			)
		},
	)

	t.Run(
		"uuid keys and foreign names",
		func(t *testing.T) {
			table := createTimers(
				t,
				db,
				"timers_uuid",
				"UUID",
			)
			require.NoError(
				t,
				db.Exec(`INSERT INTO timers_uuid (order_id, name, attempt_id, due_at) VALUES (gen_random_uuid(), ?, 'a1', now() - interval '1 second'), (gen_random_uuid(), 'FOREIGN', 'a1', now() - interval '1 second')`,
					timeoutName,
				).Error,
			)
			fired := make(
				chan pgtimer.Timer,
				2,
			)
			poller := newPoller(
				t,
				db,
				pgtimer.Config{
					Table:          table,
					Lease:          10 * time.Second,
					HandlerTimeout: 5 * time.Second,
					Poll:           50 * time.Millisecond,
				},
				func(_ context.Context, timer pgtimer.Timer) error {
					fired <- timer
					return nil
				},
			)

			var timer pgtimer.Timer
			select {
			case timer = <-fired:
			case <-time.After(10 * time.Second):
				t.Fatal("uuid timer did not fire")
			}
			require.Eventually(
				t,
				func() bool {
					return rowCount(
						t,
						db,
						"timers_uuid",
					) == 1
				},
				5*time.Second,
				20*time.Millisecond,
			)
			stopPoller(
				t,
				poller,
			)

			require.Len(
				t,
				timer.Key,
				36,
			)
			require.True(
				t,
				timer.LeaseDueAt.After(timer.DueAt),
			)
			require.GreaterOrEqual(
				t,
				timer.ClaimedAt.Sub(timer.DueAt),
				time.Second,
			)
			var foreign int64
			require.NoError(
				t,
				db.Raw(`SELECT count(*) FROM timers_uuid WHERE name = 'FOREIGN' AND due_at < now()`).
					Scan(&foreign).
					Error,
			)
			require.Equal(
				t,
				int64(1),
				foreign,
			)
		},
	)
}
