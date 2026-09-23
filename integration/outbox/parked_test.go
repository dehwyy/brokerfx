//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type parkedCall struct {
	eventID   string
	subject   string
	lastError string
}

type fakeObserver struct {
	mu        sync.Mutex
	published int
	failed    int
	parked    []parkedCall
	stats     []outbox.Stats
}

func (f *fakeObserver) OnPublished(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published += n
}

func (f *fakeObserver) OnFailed(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed += n
}

func (f *fakeObserver) OnParked(eventID, subject, lastError string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.parked = append(f.parked, parkedCall{eventID, subject, lastError})
}

func (f *fakeObserver) OnStats(stats outbox.Stats) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = append(f.stats, stats)
}

func (f *fakeObserver) parkedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.parked)
}

func (f *fakeObserver) lastParked() parkedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.parked[len(f.parked)-1]
}

func (f *fakeObserver) counts() (published, failed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published, f.failed
}

func TestExhaustedAttemptsParkEventAndNotifyObserverOnce(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "PARKED_EXHAUSTED", "parked.exhausted")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "parked.exhausted",
		Payload: []byte("payload-parked"),
	})

	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			return errors.New("simulated permanent publish failure")
		},
	}

	observer := &fakeObserver{}

	var logs bytes.Buffer
	previousLogger := log.Logger
	log.Logger = zerolog.New(&logs).With().Timestamp().Logger()
	t.Cleanup(func() { log.Logger = previousLogger })

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:             outbox.ModeUpdateAfterSend,
		BatchSize:        10,
		TickInterval:     50 * time.Millisecond,
		DeleteOlderThan:  time.Hour,
		StallThreshold:   5 * time.Minute,
		MaxAttempts:      3,
		RetryBackoffBase: 10 * time.Millisecond,
		RetryBackoffMax:  50 * time.Millisecond,
	}, testenv.WithObserver(observer))
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateParked
	}, 10*time.Second, 50*time.Millisecond, "event should be parked once attempts reach MaxAttempts")

	parkedRow := loadAttemptsRow(t, db, id)
	require.Equal(t, 3, parkedRow.Attempts)
	require.False(t, parkedRow.NextAttemptAt.Valid, "a parked row must not carry a pending next_attempt_at")

	require.Eventually(t, func() bool {
		return observer.parkedCount() == 1
	}, time.Second, 10*time.Millisecond, "observer should be notified exactly once")

	call := observer.lastParked()
	require.Equal(t, id, call.eventID)
	require.Equal(t, "parked.exhausted", call.subject)
	require.Contains(t, call.lastError, "simulated permanent publish failure")

	callsAtPark := producer.CallCount()
	require.Never(t, func() bool {
		return producer.CallCount() > callsAtPark
	}, 300*time.Millisecond, 50*time.Millisecond, "a parked row must not be picked by the relay")

	require.Equal(t, 1, observer.parkedCount(), "observer must not be notified again while the row stays parked")

	published, failed := observer.counts()
	require.Equal(t, 0, published, "a never-succeeding event must never report OnPublished")
	require.GreaterOrEqual(t, failed, 3, "each failed batch round must report OnFailed")

	harness.Stop()

	output := logs.String()
	require.Contains(t, output, "outbox event parked")
	require.Contains(t, output, id)
	require.Contains(t, output, "parked.exhausted")
}

func TestMaxAttemptsZeroNeverParks(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "PARKED_UNLIMITED", "parked.unlimited")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "parked.unlimited",
		Payload: []byte("payload-unlimited"),
	})

	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			return errors.New("simulated repeated publish failure")
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:             outbox.ModeUpdateAfterSend,
		BatchSize:        10,
		TickInterval:     30 * time.Millisecond,
		DeleteOlderThan:  time.Hour,
		StallThreshold:   5 * time.Minute,
		MaxAttempts:      0,
		RetryBackoffBase: 5 * time.Millisecond,
		RetryBackoffMax:  20 * time.Millisecond,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadAttemptsRow(t, db, id).Attempts >= 5
	}, 10*time.Second, 20*time.Millisecond, "event should keep retrying past what would be a MaxAttempts threshold")

	require.Equal(t, outbox.StatePending, loadEvent(t, db, id).State, "MaxAttempts=0 must never park an event")
}

func TestRequeueParkedRepublishesEvent(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	js := testenv.NATS(t)
	newStream(t, js, "PARKED_REQUEUE", "parked.requeue")

	id := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{
		ID:      id,
		Topic:   "parked.requeue",
		Payload: []byte("payload-requeue"),
	})

	var shouldFail atomic.Bool
	shouldFail.Store(true)

	producer := &testenv.FakeProducer{
		Fail: func(outbox.ProducerEvent) error {
			if shouldFail.Load() {
				return errors.New("simulated publish failure before requeue")
			}
			return nil
		},
	}

	harness := testenv.NewRelayHarness(t, db, producer, outbox.Config{
		Mode:             outbox.ModeUpdateAfterSend,
		BatchSize:        10,
		TickInterval:     50 * time.Millisecond,
		DeleteOlderThan:  time.Hour,
		StallThreshold:   5 * time.Minute,
		MaxAttempts:      1,
		RetryBackoffBase: 10 * time.Millisecond,
		RetryBackoffMax:  50 * time.Millisecond,
	})
	harness.Start(t)
	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateParked
	}, 10*time.Second, 50*time.Millisecond, "event with MaxAttempts=1 should park after its single failure")

	shouldFail.Store(false)

	n, err := harness.Store.RequeueParked(context.Background(), []string{id})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	requeued := loadAttemptsRow(t, db, id)
	require.Equal(t, outbox.StatePending, requeued.State, "requeue must reset state to PENDING immediately")
	require.Equal(t, 0, requeued.Attempts, "requeue must reset attempts")

	harness.Wake()

	require.Eventually(t, func() bool {
		return loadEvent(t, db, id).State == outbox.StateDone
	}, 10*time.Second, 50*time.Millisecond, "requeued event should publish successfully")
}

func TestRequeueParkedWithEmptyIDsRequeuesAll(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	idA := uuid.NewString()
	idB := uuid.NewString()
	insertEvent(t, db, &outbox.OutboxEvent{ID: idA, Topic: "parked.a", Payload: []byte("a"), State: outbox.StateParked})
	insertEvent(t, db, &outbox.OutboxEvent{ID: idB, Topic: "parked.b", Payload: []byte("b"), State: outbox.StateParked})

	store := newStoreForTest(t, db)

	n, err := store.RequeueParked(context.Background(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)

	require.Equal(t, outbox.StatePending, loadEvent(t, db, idA).State)
	require.Equal(t, outbox.StatePending, loadEvent(t, db, idB).State)
}

func TestStatsCountsRowsByStateAndOldestPendingAge(t *testing.T) {
	db := testenv.Postgres(t)
	require.NoError(t, outbox.AutoMigrate(db))

	store := newStoreForTest(t, db)

	older := time.Now().Add(-time.Hour)
	insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "stats.pending.old", Payload: []byte("p"), State: outbox.StatePending, CreatedAt: older})
	insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "stats.pending.new", Payload: []byte("p"), State: outbox.StatePending})
	insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "stats.inflight", Payload: []byte("p"), State: outbox.StateInFlight})
	insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "stats.done", Payload: []byte("p"), State: outbox.StateDone})
	insertEvent(t, db, &outbox.OutboxEvent{ID: uuid.NewString(), Topic: "stats.parked", Payload: []byte("p"), State: outbox.StateParked})

	stats, err := store.Stats(context.Background())
	require.NoError(t, err)

	require.EqualValues(t, 2, stats.Pending)
	require.EqualValues(t, 1, stats.InFlight)
	require.EqualValues(t, 1, stats.Done)
	require.EqualValues(t, 1, stats.Parked)
	require.GreaterOrEqual(t, stats.OldestPendingAge, 55*time.Minute, "oldest pending age should reflect the older row, not the newer one")
}
