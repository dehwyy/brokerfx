//go:build integration

package outbox_test

import (
	"strings"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func backdateUpdatedAt(t *testing.T, db *gorm.DB, id string, offset time.Duration) {
	t.Helper()
	require.NoError(
		t,
		db.Exec("update outbox_events set updated_at = ? where id = ?", time.Now().Add(offset), id).Error,
	)
}

func eventExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&outbox.OutboxEvent{}).Where("id = ?", id).Count(&count).Error)
	return count > 0
}

func TestCleanupDeletesExpiredDoneAndParkedButKeepsRecentAndActive(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	oldDoneID := uuid.NewString()
	recentDoneID := uuid.NewString()
	oldParkedID := uuid.NewString()
	recentParkedID := uuid.NewString()
	pendingID := uuid.NewString()
	inFlightID := uuid.NewString()

	insertEvent(t, db, &outbox.OutboxEvent{ID: oldDoneID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StateDone})
	insertEvent(t, db, &outbox.OutboxEvent{ID: recentDoneID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StateDone})
	insertEvent(t, db, &outbox.OutboxEvent{ID: oldParkedID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StateParked})
	insertEvent(t, db, &outbox.OutboxEvent{ID: recentParkedID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StateParked})
	insertEvent(t, db, &outbox.OutboxEvent{ID: pendingID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StatePending})
	insertEvent(t, db, &outbox.OutboxEvent{ID: inFlightID, Topic: "cleanup.noop", Payload: []byte("p"), State: outbox.StateInFlight})

	backdateUpdatedAt(t, db, oldDoneID, -2*time.Hour)
	backdateUpdatedAt(t, db, oldParkedID, -2*time.Hour)
	backdateUpdatedAt(t, db, pendingID, -2*time.Hour)
	backdateUpdatedAt(t, db, inFlightID, -2*time.Hour)

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  time.Hour,
		CleanupInterval: 50 * time.Millisecond,
		RetainParked:    time.Hour,
	})
	harness.Start(t)

	require.Eventually(t, func() bool {
		return !eventExists(t, db, oldDoneID)
	}, 5*time.Second, 50*time.Millisecond, "DONE row older than DeleteOlderThan should be cleaned up")

	require.Eventually(t, func() bool {
		return !eventExists(t, db, oldParkedID)
	}, 5*time.Second, 50*time.Millisecond, "PARKED row older than RetainParked should be cleaned up")

	require.True(t, eventExists(t, db, recentDoneID), "recent DONE row must not be cleaned up")
	require.True(t, eventExists(t, db, recentParkedID), "recent PARKED row must not be cleaned up")
	require.True(t, eventExists(t, db, pendingID), "PENDING row must never be cleaned up regardless of age")
	require.True(t, eventExists(t, db, inFlightID), "IN_FLIGHT row must never be cleaned up regardless of age")
}

func TestCleanupNeverDeletesParkedWhenRetainParkedIsZero(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	oldParkedID := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{ID: oldParkedID, Topic: "cleanup.retain-zero", Payload: []byte("p"), State: outbox.StateParked})
	backdateUpdatedAt(t, db, oldParkedID, -30*24*time.Hour)

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  time.Hour,
		CleanupInterval: 50 * time.Millisecond,
		RetainParked:    0,
	})
	harness.Start(t)

	require.Never(t, func() bool {
		return !eventExists(t, db, oldParkedID)
	}, 500*time.Millisecond, 50*time.Millisecond, "RetainParked=0 must never delete PARKED rows")
}

func TestCleanupQueryUsesStateUpdatedAtIndex(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	require.NoError(t, db.Exec("set enable_seqscan = off").Error)

	rows, err := db.Raw(
		"explain select * from outbox_events where state = ? and updated_at < now()",
		outbox.StateDone,
	).Rows()
	require.NoError(t, err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}

	plan := strings.Join(lines, "\n")
	require.Contains(t, plan, "idx_outbox_events_state_updated_at")
}
