//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newStream(t *testing.T, js jetstream.JetStream, name, subject string) {
	t.Helper()

	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
	})
	require.NoError(t, err)
}

func insertEvent(t *testing.T, db *gorm.DB, ev *outbox.OutboxEvent) {
	t.Helper()
	require.NoError(t, db.Create(ev).Error)
}

func loadEvent(t *testing.T, db *gorm.DB, id string) *outbox.OutboxEvent {
	t.Helper()
	var ev outbox.OutboxEvent
	require.NoError(t, db.First(&ev, "id = ?", id).Error)
	return &ev
}

func TestLegacyPendingEventPublishedAndMarkedDone(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "LEGACY_PENDING", "legacy.pending")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "legacy.pending",
		Payload: []byte("payload-pending"),
	})

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 100*time.Millisecond, "event should transition to DONE after publish")

	stream, err := js.Stream(context.Background(), "LEGACY_PENDING")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "legacy.pending")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "published message should be visible in the stream")

	require.Equal(t, id, raw.Header.Get(jetstream.MsgIDHeader))
	require.Equal(t, []byte("payload-pending"), raw.Data)
}

func TestLegacyProducerFailureRevertsToPendingAndRecordsRetry(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "legacy.failure",
		Payload: []byte("payload-failure"),
	})

	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			return errors.New("simulated publish failure")
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  5 * time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StatePending
	}, 10*time.Second, 100*time.Millisecond, "event should revert to PENDING after a failed publish")

	var retries []outbox.OutboxRetry
	require.Eventually(t, func() bool {
		require.NoError(t, db.Where("event_id = ?", id).Find(&retries).Error)
		return len(retries) == 1
	}, 10*time.Second, 100*time.Millisecond, "exactly one retry row should be recorded")

	require.Equal(t, "simulated publish failure", retries[0].Error)
	require.Equal(t, 1, producer.CallCount())
	require.Equal(t, outbox.StatePending, loadEvent(t, db, id).State, "event should be PENDING again after the retry row was recorded")
}

func TestLegacyStalledInFlightEventIsRepublished(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "LEGACY_STALLED", "legacy.stalled")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "legacy.stalled",
		Payload: []byte("payload-stalled"),
		State:   outbox.StateInFlight,
	})
	require.NoError(
		t,
		db.Model(&outbox.OutboxEvent{}).
			Where("id = ?", id).
			UpdateColumn("updated_at", time.Now().Add(-time.Hour)).Error,
	)

	harness := testenv.NewRelayHarness(t, db, outbox.NewJetStreamProducer(js), outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: time.Hour,
		StallThreshold:  500 * time.Millisecond,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 100*time.Millisecond, "stalled IN_FLIGHT event should be re-picked and republished")

	stream, err := js.Stream(context.Background(), "LEGACY_STALLED")
	require.NoError(t, err)

	var raw *jetstream.RawStreamMsg
	require.Eventually(t, func() bool {
		var getErr error
		raw, getErr = stream.GetLastMsgForSubject(context.Background(), "legacy.stalled")
		return getErr == nil
	}, 10*time.Second, 100*time.Millisecond, "republished message should be visible in the stream")

	require.Equal(t, id, raw.Header.Get(jetstream.MsgIDHeader))
}

func TestLegacyDoneEventCleanedUpAfterRetention(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "legacy.done",
		Payload: []byte("payload-done"),
		State:   outbox.StateDone,
	})
	require.NoError(
		t,
		db.Model(&outbox.OutboxEvent{}).
			Where("id = ?", id).
			UpdateColumn("updated_at", time.Now().Add(-2*time.Second)).Error,
	)

	producer := &testenv.FakeProducer{}
	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:            outbox.ModeUpdateAfterSend,
		BatchSize:       10,
		TickInterval:    time.Hour,
		DeleteOlderThan: 1 * time.Second,
		StallThreshold:  5 * time.Minute,
	})
	harness.Start(t)

	require.Eventually(t, func() bool {
		var ev outbox.OutboxEvent
		err := db.First(&ev, "id = ?", id).Error
		return errors.Is(err, gorm.ErrRecordNotFound)
	}, 5*time.Minute+30*time.Second, 5*time.Second,
		"DONE event older than DeleteOlderThan should be removed by the fixed 5m cleanup ticker (v0.1.9 has no configurable CleanupInterval)")

	require.Equal(t, 0, producer.CallCount(), "cleanup must not touch already-DONE rows via the producer")
}
