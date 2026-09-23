//go:build integration

package outbox_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type outboxAttemptsRow struct {
	Attempts      int
	LastError     sql.NullString
	NextAttemptAt sql.NullTime
	State         outbox.OutboxState
}

func loadAttemptsRow(t *testing.T, db *gorm.DB, id string) outboxAttemptsRow {
	t.Helper()

	var row outboxAttemptsRow
	require.NoError(
		t,
		db.Raw(`select attempts, last_error, next_attempt_at, state from outbox_events where id = ?`, id).
			Row().
			Scan(&row.Attempts, &row.LastError, &row.NextAttemptAt, &row.State),
	)

	return row
}

func TestAttemptsIncrementOnOrdinaryFailureAndBackoffDelaysNextPick(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "ATTEMPTS_BACKOFF", "attempts.backoff")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "attempts.backoff",
		Payload: []byte("payload-backoff"),
	})

	backoffBase := 3 * time.Second
	failedOnce := false
	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			if failedOnce {
				return nil
			}
			failedOnce = true
			return &jetStreamRejected{}
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:             outbox.ModeUpdateAfterSend,
		BatchSize:        10,
		TickInterval:     200 * time.Millisecond,
		DeleteOlderThan:  time.Hour,
		StallThreshold:   5 * time.Minute,
		RetryBackoffBase: backoffBase,
		RetryBackoffMax:  time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadAttemptsRow(t, db, id).Attempts == 1
	}, 10*time.Second, 50*time.Millisecond, "attempts should be incremented after an ordinary publish failure")

	afterFirstFailure := loadAttemptsRow(t, db, id)
	require.Equal(t, outbox.StatePending, afterFirstFailure.State)
	require.True(t, afterFirstFailure.LastError.Valid)
	require.True(t, afterFirstFailure.NextAttemptAt.Valid)
	require.True(t, afterFirstFailure.NextAttemptAt.Time.After(time.Now()))

	require.Never(t, func() bool {
		return producer.CallCount() >= 2
	}, backoffBase-time.Second, 200*time.Millisecond, "row must not be re-picked before next_attempt_at")

	require.Eventually(t, func() bool {
		return loadAttemptsRow(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 100*time.Millisecond, "row should publish successfully once next_attempt_at has passed")

	require.Equal(t, 2, producer.CallCount())
}

func TestTransportErrorDoesNotIncrementAttempts(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "ATTEMPTS_TRANSPORT", "attempts.transport")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "attempts.transport",
		Payload: []byte("payload-transport"),
	})

	failedOnce := false
	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			if failedOnce {
				return nil
			}
			failedOnce = true
			return natsgo.ErrNoServers
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:             outbox.ModeUpdateAfterSend,
		BatchSize:        10,
		TickInterval:     200 * time.Millisecond,
		DeleteOlderThan:  time.Hour,
		StallThreshold:   5 * time.Minute,
		RetryBackoffBase: 3 * time.Second,
		RetryBackoffMax:  time.Minute,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return producer.CallCount() >= 1
	}, 10*time.Second, 50*time.Millisecond, "producer should have been called at least once")

	require.Eventually(t, func() bool {
		return loadAttemptsRow(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 100*time.Millisecond, "transport failure must not delay the retry, row should publish on the very next tick")

	row := loadAttemptsRow(t, db, id)
	require.Equal(t, 0, row.Attempts, "a transport-level failure must not increment attempts")
}

type jetStreamRejected struct{}

func (*jetStreamRejected) Error() string {
	return "simulated ordinary publish rejection"
}
