//go:build integration

package outbox_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestPausedConfigKeepsEventsPendingAndWakeupIsNoop(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.config",
		Payload: []byte("payload-pause"),
	})

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    50 * time.Millisecond,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Paused:          true,
	})
	harness.Start(t)
	harness.Wake()

	require.Never(t, func() bool {
		return loadEvent(t, db, id).State != outbox.StatePending
	}, time.Second, 50*time.Millisecond, "a paused relay must never pick up a PENDING row")

	row := loadAttemptsRow(t, db, id)
	require.Equal(t, 0, row.Attempts)
	require.Equal(t, 0, producer.CallCount())
}

func TestPausedByEnvKeepsEventsPending(t *testing.T) {
	t.Setenv("BROKERFX_OUTBOX_RELAY_PAUSED", "true")

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.env",
		Payload: []byte("payload-pause-env"),
	})

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    50 * time.Millisecond,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})
	require.True(t, harness.Relay.Paused())
	harness.Start(t)
	harness.Wake()

	require.Never(t, func() bool {
		return loadEvent(t, db, id).State != outbox.StatePending
	}, time.Second, 50*time.Millisecond, "env-paused relay must never pick up a PENDING row")
}

func TestResumePublishesQueuedEvents(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.resume",
		Payload: []byte("payload-resume"),
	})

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    50 * time.Millisecond,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Paused:          true,
	})
	harness.Start(t)
	harness.Wake()

	require.Never(t, func() bool {
		return loadEvent(t, db, id).State != outbox.StatePending
	}, 300*time.Millisecond, 50*time.Millisecond, "row must stay PENDING while paused")

	harness.Relay.Resume()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 50*time.Millisecond, "row should be published once resumed")

	require.Equal(t, 1, producer.CallCount())
}

func TestStopWhilePausedSkipsFinalBatch(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.stop",
		Payload: []byte("payload-stop"),
	})

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Paused:          true,
	})
	harness.Start(t)

	harness.Stop()

	require.Equal(t, outbox.StatePending, loadEvent(t, db, id).State, "a paused relay must not process the final batch on shutdown")
	require.Equal(t, 0, producer.CallCount())
}

func TestPausedRelayLogsPendingCountOnStart(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.log",
		Payload: []byte("payload-log"),
	})

	var logs bytes.Buffer
	previousLogger := log.Logger
	log.Logger = zerolog.New(&logs).With().Timestamp().Logger()
	t.Cleanup(func() { log.Logger = previousLogger })

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    50 * time.Millisecond,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		Paused:          true,
	})
	harness.Start(t)

	require.Eventually(t, func() bool {
		return bytes.Contains(logs.Bytes(), []byte("outbox relay paused")) &&
			bytes.Contains(logs.Bytes(), []byte(`"pending":1`))
	}, 5*time.Second, 50*time.Millisecond, "a relay that starts paused should log the PENDING count")
}

func TestCleanupRunsWhilePaused(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "pause.cleanup",
		Payload: []byte("payload-cleanup"),
		State:   outbox.StateDone,
	})
	require.NoError(
		t,
		db.Model(&outbox.OutboxEvent{}).
			Where("id = ?", id).
			UpdateColumn("updated_at", time.Now().Add(-2*time.Hour)).Error,
	)

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
		CleanupInterval: 50 * time.Millisecond,
		Paused:          true,
	})
	harness.Start(t)

	require.Eventually(t, func() bool {
		return !eventExists(t, db, id)
	}, 5*time.Second, 50*time.Millisecond, "cleanup must keep running while the relay is paused")
}
