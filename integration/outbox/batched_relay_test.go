//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func batchedConfig(mode outbox.OutboxMode) outbox.Config {
	return outbox.Config{
		Mode:            mode,
		BatchSize:       100,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Batched:         true,
		Linger:          50 * time.Millisecond,
	}
}

func countRows(t *testing.T, db *gorm.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Raw(query, args...).Scan(&n).Error)
	return n
}

func TestBatchedRelayLingerCoalescesWakeups(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	producer := &testenv.FakeBatchProducer{}
	cfg := batchedConfig(outbox.ModeDeleteAfterSend)
	cfg.Linger = 200 * time.Millisecond
	harness := testenv.NewRelayHarness(t, db, producer, cfg)
	harness.Start(t)

	for i := 0; i < 50; i++ {
		require.NoError(t, harness.Store.SaveMessage(context.Background(), outbox.Message{
			Subject: "batched.coalesce",
			Payload: []byte(fmt.Sprintf("p-%d", i)),
		}))
		harness.Wake()
	}

	require.Eventually(t, func() bool {
		return producer.CallCount() == 50 && countRows(t, db, "select count(*) from outbox_events") == 0
	}, 10*time.Second, 50*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 50, producer.CallCount())
	seen := map[string]int{}
	for _, call := range producer.Calls {
		seen[string(call.Payload)]++
	}
	require.Len(t, seen, 50)
	require.LessOrEqual(t, len(producer.BatchSizes()), 5)
}

func TestBatchedRelayUpdateMode(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	ids := make([]string, 3)
	for i := range ids {
		ids[i] = uuid.NewString()
		insertEvent(t, db, &outbox.OutboxEvent{ID: ids[i], Topic: "batched.update", Payload: []byte("p")})
	}

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, batchedConfig(outbox.ModeUpdateAfterSend))
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return countRows(t, db, "select count(*) from outbox_events where state = ?", outbox.StateDone) == 3
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, 3, producer.CallCount())
}

func TestBatchedRelayReclaimsStalled(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{ID: id, Topic: "batched.stalled", Payload: []byte("p")})
	require.NoError(t, db.Exec(
		"update outbox_events set state = ?, updated_at = ? where id = ?",
		outbox.StateInFlight,
		time.Now().Add(-time.Hour),
		id,
	).Error)

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, batchedConfig(outbox.ModeDeleteAfterSend))
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return producer.CallCount() == 1 && countRows(t, db, "select count(*) from outbox_events") == 0
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, id, producer.Calls[0].Headers["Nats-Msg-Id"])
}

func TestBatchedRelayFailureKeepsPending(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))
	failID := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{ID: failID, Topic: "batched.fail", Payload: []byte("bad")})
	for i := 0; i < 3; i++ {
		insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "batched.fail", Payload: []byte("ok")})
	}

	producer := &testenv.FakeBatchProducer{
		Fail: func(event outbox.ProducerEvent) error {
			if string(event.Payload) == "bad" {
				return errors.New("stream rejected")
			}
			return nil
		},
	}
	cfg := batchedConfig(outbox.ModeDeleteAfterSend)
	cfg.RetryBackoffBase = time.Minute
	cfg.RetryBackoffMax = time.Hour
	harness := testenv.NewRelayHarness(t, db, producer, cfg)
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return countRows(t, db, "select count(*) from outbox_events") == 1 &&
			countRows(t, db, "select count(*) from outbox_retries where event_id = ?", failID) == 1
	}, 10*time.Second, 50*time.Millisecond)

	var row struct {
		State         string
		Attempts      int
		NextAttemptAt *time.Time
	}
	require.NoError(t, db.Raw("select state, attempts, next_attempt_at from outbox_events where id = ?", failID).Scan(&row).Error)
	require.Equal(t, string(outbox.StatePending), row.State)
	require.Equal(t, 1, row.Attempts)
	require.NotNil(t, row.NextAttemptAt)
}

func TestBatchedRelayLegacySchemaFallsBack(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	createV1OutboxEventsTable(t, db)
	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{ID: id, Topic: "batched.legacy", Payload: []byte("p")})

	producer := &testenv.FakeBatchProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, batchedConfig(outbox.ModeUpdateAfterSend))
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, 1, producer.CallCount())
	require.Equal(t, []int{1}, producer.BatchSizes())
}
