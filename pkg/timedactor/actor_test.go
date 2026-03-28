package timedactor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

// ===========================================================================
// Mocks
// ===========================================================================

// mockJetStream satisfies jetstream.JetStream for constructor tests.
// Only CreateOrUpdateKeyValue is implemented; all other methods panic via
// the embedded nil interface.
type mockJetStream struct {
	jetstream.JetStream
	createOrUpdateKVFn func(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error)
}

func (m *mockJetStream) CreateOrUpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return m.createOrUpdateKVFn(ctx, cfg)
}

// mockKV satisfies jetstream.KeyValue. Each method delegates to its function
// field; unset fields will nil-panic, signalling a missing mock setup.
type mockKV struct {
	jetstream.KeyValue
	putFn      func(ctx context.Context, key string, value []byte) (uint64, error)
	getFn      func(ctx context.Context, key string) (jetstream.KeyValueEntry, error)
	updateFn   func(ctx context.Context, key string, value []byte, last uint64) (uint64, error)
	deleteFn   func(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error
	watchFn    func(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error)
	listKeysFn func(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyLister, error)
}

func (m *mockKV) Put(ctx context.Context, key string, val []byte) (uint64, error) {
	return m.putFn(ctx, key, val)
}
func (m *mockKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	return m.getFn(ctx, key)
}
func (m *mockKV) Update(ctx context.Context, key string, value []byte, last uint64) (uint64, error) {
	return m.updateFn(ctx, key, value, last)
}
func (m *mockKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	return m.deleteFn(ctx, key, opts...)
}
func (m *mockKV) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return m.watchFn(ctx, keys, opts...)
}
func (m *mockKV) ListKeys(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
	return m.listKeysFn(ctx, opts...)
}

// mockEntry satisfies jetstream.KeyValueEntry.
type mockEntry struct {
	jetstream.KeyValueEntry
	key       string
	value     []byte
	revision  uint64
	operation jetstream.KeyValueOp
}

func (e *mockEntry) Bucket() string                  { return "test" }
func (e *mockEntry) Key() string                     { return e.key }
func (e *mockEntry) Value() []byte                   { return e.value }
func (e *mockEntry) Revision() uint64                { return e.revision }
func (e *mockEntry) Created() time.Time              { return time.Now() }
func (e *mockEntry) Delta() uint64                   { return 0 }
func (e *mockEntry) Operation() jetstream.KeyValueOp { return e.operation }

// mockWatcher satisfies jetstream.KeyWatcher.
type mockWatcher struct {
	jetstream.KeyWatcher
	ch chan jetstream.KeyValueEntry
}

func (w *mockWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.ch }
func (w *mockWatcher) Stop() error                             { return nil }

// mockKeyLister satisfies jetstream.KeyLister. Channel must be closed after
// all keys are sent so that `range lister.Keys()` terminates.
type mockKeyLister struct {
	jetstream.KeyLister
	ch chan string
}

func (l *mockKeyLister) Keys() <-chan string { return l.ch }

// ===========================================================================
// Helpers
// ===========================================================================

func newTestActor(kv jetstream.KeyValue) (*TimedActor, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &TimedActor{
		kv:     kv,
		logger: zerolog.Nop(),
		config: Config{
			BucketName:      "test-bucket",
			CheckInterval:   1 * time.Hour, // effectively disable rescan ticker
			MaxHoldDuration: 10 * time.Minute,
		},
		timers: make(map[string]*time.Timer),
		ctx:    ctx,
		cancel: cancel,
	}, cancel
}

func closedKeyLister(keys ...string) *mockKeyLister {
	ch := make(chan string, len(keys))
	for _, k := range keys {
		ch <- k
	}
	close(ch)
	return &mockKeyLister{ch: ch}
}

// ===========================================================================
// Tests: marshalTime / unmarshalTime
// ===========================================================================

func TestMarshalUnmarshalTime_RoundTrip(t *testing.T) {
	now := time.Now()
	data := marshalTime(now)
	got, err := unmarshalTime(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(now) {
		t.Errorf("got %v, want %v", got, now)
	}
}

func TestMarshalUnmarshalTime_UnixEpoch(t *testing.T) {
	epoch := time.Unix(0, 0)
	data := marshalTime(epoch)
	got, err := unmarshalTime(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(epoch) {
		t.Errorf("got %v, want Unix epoch", got)
	}
}

func TestUnmarshalTime_InvalidData(t *testing.T) {
	_, err := unmarshalTime([]byte("not-a-number"))
	if err == nil {
		t.Fatal("expected error for invalid data")
	}
}

// ===========================================================================
// Tests: isRevisionMismatch
// ===========================================================================

func TestIsRevisionMismatch_APIError(t *testing.T) {
	err := &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
	if !isRevisionMismatch(err) {
		t.Error("expected true for APIError with code 10071")
	}
}

func TestIsRevisionMismatch_StringMatch(t *testing.T) {
	err := errors.New("some wrapper: wrong last sequence: 5")
	if !isRevisionMismatch(err) {
		t.Error("expected true for error containing 'wrong last sequence'")
	}
}

func TestIsRevisionMismatch_UnrelatedError(t *testing.T) {
	err := errors.New("connection refused")
	if isRevisionMismatch(err) {
		t.Error("expected false for unrelated error")
	}
}

// ===========================================================================
// Tests: New
// ===========================================================================

func TestNew_DefaultConfig(t *testing.T) {
	var gotBucket string
	js := &mockJetStream{
		createOrUpdateKVFn: func(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
			gotBucket = cfg.Bucket
			return &mockKV{}, nil
		},
	}
	actor, err := New(Deps{JS: js})
	if err != nil {
		t.Fatal(err)
	}
	if actor == nil {
		t.Fatal("expected non-nil actor")
	}
	if gotBucket != "TimedActorBucket" {
		t.Errorf("bucket = %q, want TimedActorBucket", gotBucket)
	}
}

func TestNew_CustomConfig(t *testing.T) {
	var gotBucket string
	js := &mockJetStream{
		createOrUpdateKVFn: func(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
			gotBucket = cfg.Bucket
			return &mockKV{}, nil
		},
	}
	actor, err := New(Deps{
		JS:     js,
		Config: Config{BucketName: "custom-bucket", CheckInterval: 10 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if actor == nil {
		t.Fatal("expected non-nil actor")
	}
	if gotBucket != "custom-bucket" {
		t.Errorf("bucket = %q, want custom-bucket", gotBucket)
	}
}

func TestNew_CreateBucketError(t *testing.T) {
	js := &mockJetStream{
		createOrUpdateKVFn: func(_ context.Context, _ jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
			return nil, errors.New("nats: connection closed")
		},
	}
	actor, err := New(Deps{JS: js})
	if err == nil {
		t.Fatal("expected error")
	}
	if actor != nil {
		t.Fatal("expected nil actor on error")
	}
}

// ===========================================================================
// Tests: Add
// ===========================================================================

func TestAdd_Success(t *testing.T) {
	var gotKey string
	var gotValue []byte
	kv := &mockKV{
		putFn: func(_ context.Context, key string, value []byte) (uint64, error) {
			gotKey = key
			gotValue = value
			return 1, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	before := time.Now()
	err := actor.Add(context.Background(), "order.123", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if gotKey != "order.123" {
		t.Errorf("key = %q, want order.123", gotKey)
	}
	expiresAt, err := unmarshalTime(gotValue)
	if err != nil {
		t.Fatal(err)
	}
	expected := before.Add(5 * time.Minute)
	if expiresAt.Before(expected.Add(-1*time.Second)) || expiresAt.After(expected.Add(1*time.Second)) {
		t.Errorf("expires_at = %v, want ~%v", expiresAt, expected)
	}
}

func TestAdd_PutError(t *testing.T) {
	kv := &mockKV{
		putFn: func(_ context.Context, _ string, _ []byte) (uint64, error) {
			return 0, errors.New("nats: timeout")
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	err := actor.Add(context.Background(), "key", time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdd_OverwriteExisting(t *testing.T) {
	var callCount int
	kv := &mockKV{
		putFn: func(_ context.Context, _ string, _ []byte) (uint64, error) {
			callCount++
			return uint64(callCount), nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	_ = actor.Add(context.Background(), "key", time.Minute)
	_ = actor.Add(context.Background(), "key", 2*time.Minute)
	if callCount != 2 {
		t.Errorf("put called %d times, want 2", callCount)
	}
}

// ===========================================================================
// Tests: Clear
// ===========================================================================

func TestClear_Success(t *testing.T) {
	var deletedKey string
	kv := &mockKV{
		deleteFn: func(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
			deletedKey = key
			return nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	err := actor.Clear(context.Background(), "order.456")
	if err != nil {
		t.Fatal(err)
	}
	if deletedKey != "order.456" {
		t.Errorf("deleted key = %q, want order.456", deletedKey)
	}
}

func TestClear_KeyNotFound(t *testing.T) {
	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return jetstream.ErrKeyNotFound
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	err := actor.Clear(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("expected nil error for key-not-found, got %v", err)
	}
}

func TestClear_DeleteError(t *testing.T) {
	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return errors.New("nats: connection closed")
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	err := actor.Clear(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ===========================================================================
// Tests: List
// ===========================================================================

func TestList_AllKeysEmpty(t *testing.T) {
	kv := &mockKV{
		listKeysFn: func(_ context.Context, _ ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
			return nil, jetstream.ErrNoKeysFound
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	result, err := actor.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestList_AllKeysWithEntries(t *testing.T) {
	now := time.Now()
	entries := map[string]time.Time{
		"a": now.Add(1 * time.Minute),
		"b": now.Add(2 * time.Minute),
	}
	kv := &mockKV{
		listKeysFn: func(_ context.Context, _ ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
			return closedKeyLister("a", "b"), nil
		},
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			t, ok := entries[key]
			if !ok {
				return nil, jetstream.ErrKeyNotFound
			}
			return &mockEntry{key: key, value: marshalTime(t), revision: 1, operation: jetstream.KeyValuePut}, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	result, err := actor.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	for k, expected := range entries {
		got, ok := result[k]
		if !ok {
			t.Errorf("missing key %q", k)
			continue
		}
		if !got.Equal(expected) {
			t.Errorf("key %q: got %v, want %v", k, got, expected)
		}
	}
}

func TestList_SpecificKeys(t *testing.T) {
	now := time.Now().Add(10 * time.Minute)
	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			if key == "exists" {
				return &mockEntry{key: key, value: marshalTime(now), revision: 1, operation: jetstream.KeyValuePut}, nil
			}
			return nil, jetstream.ErrKeyNotFound
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	result, err := actor.List(context.Background(), "exists", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if _, ok := result["exists"]; !ok {
		t.Error("expected 'exists' key in result")
	}
}

// ===========================================================================
// Tests: tryFireCallback (core exactly-once logic)
// ===========================================================================

func TestTryFireCallback_WinsRace(t *testing.T) {
	var called atomic.Bool
	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return nil // success — this replica wins
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	actor.tryFireCallback("key", 1, func(_ context.Context, key string) {
		called.Store(true)
		if key != "key" {
			t.Errorf("callback key = %q, want 'key'", key)
		}
	})
	if !called.Load() {
		t.Error("expected callback to be called")
	}
}

func TestTryFireCallback_LosesRace(t *testing.T) {
	var called atomic.Bool
	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	actor.tryFireCallback("key", 1, func(_ context.Context, _ string) {
		called.Store(true)
	})
	if called.Load() {
		t.Error("callback should NOT be called on revision mismatch")
	}
}

func TestTryFireCallback_KeyAlreadyDeleted(t *testing.T) {
	var called atomic.Bool
	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return jetstream.ErrKeyNotFound
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	actor.tryFireCallback("key", 1, func(_ context.Context, _ string) {
		called.Store(true)
	})
	if called.Load() {
		t.Error("callback should NOT be called when key is already deleted")
	}
}

// ===========================================================================
// Tests: Subscribe
// ===========================================================================

func TestSubscribe_CallbackFiresOnExpiry(t *testing.T) {
	watchCh := make(chan jetstream.KeyValueEntry, 10)
	callbackCh := make(chan string, 1)

	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return nil // success
		},
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{ch: watchCh}, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	defer actor.Stop()

	actor.Subscribe(context.Background(), "test.*", func(_ context.Context, key string) {
		callbackCh <- key
	})

	// Send an entry expiring in 50ms.
	watchCh <- &mockEntry{
		key: "test.1", value: marshalTime(time.Now().Add(50 * time.Millisecond)),
		revision: 1, operation: jetstream.KeyValuePut,
	}

	select {
	case key := <-callbackCh:
		if key != "test.1" {
			t.Errorf("callback key = %q, want test.1", key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for callback")
	}
}

func TestSubscribe_RevisionMismatchSkipsCallback(t *testing.T) {
	watchCh := make(chan jetstream.KeyValueEntry, 10)
	var called atomic.Bool

	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
		},
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{ch: watchCh}, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	defer actor.Stop()

	actor.Subscribe(context.Background(), "test.*", func(_ context.Context, _ string) {
		called.Store(true)
	})

	// Expires immediately.
	watchCh <- &mockEntry{
		key: "test.1", value: marshalTime(time.Now()),
		revision: 1, operation: jetstream.KeyValuePut,
	}

	time.Sleep(300 * time.Millisecond)
	if called.Load() {
		t.Error("callback should NOT fire on revision mismatch")
	}
}

func TestSubscribe_DeleteMarkerClearsTimer(t *testing.T) {
	watchCh := make(chan jetstream.KeyValueEntry, 10)
	var called atomic.Bool

	kv := &mockKV{
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			return nil
		},
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{ch: watchCh}, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	defer actor.Stop()

	actor.Subscribe(context.Background(), "test.*", func(_ context.Context, _ string) {
		called.Store(true)
	})

	// Add entry expiring in 500ms.
	watchCh <- &mockEntry{
		key: "test.1", value: marshalTime(time.Now().Add(500 * time.Millisecond)),
		revision: 1, operation: jetstream.KeyValuePut,
	}
	time.Sleep(50 * time.Millisecond) // let watcher goroutine schedule the timer

	// Send delete marker before the timer fires.
	watchCh <- &mockEntry{
		key: "test.1", operation: jetstream.KeyValueDelete,
	}
	time.Sleep(600 * time.Millisecond) // wait past original expiry

	if called.Load() {
		t.Error("callback should NOT fire after key was deleted")
	}
}

// ===========================================================================
// Tests: Stop
// ===========================================================================

func TestStop_CancelsTimers(t *testing.T) {
	kv := &mockKV{}
	actor, _ := newTestActor(kv)

	// Manually schedule a timer.
	actor.scheduleTimer("key", 1, time.Hour, func(_ context.Context, _ string) {
		t.Error("timer should have been cancelled")
	})

	actor.Stop()

	actor.mu.Lock()
	remaining := len(actor.timers)
	actor.mu.Unlock()
	if remaining != 0 {
		t.Errorf("expected 0 timers after Stop, got %d", remaining)
	}
}

func TestStop_StopsWatchers(t *testing.T) {
	watchCh := make(chan jetstream.KeyValueEntry, 1)
	watcher := &mockWatcher{ch: watchCh}

	kv := &mockKV{
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}
	actor, _ := newTestActor(kv)
	actor.Subscribe(context.Background(), ">", func(_ context.Context, _ string) {})

	time.Sleep(50 * time.Millisecond) // let goroutine start
	actor.Stop()

	actor.mu.Lock()
	remainingWatchers := len(actor.watchers)
	actor.mu.Unlock()
	if remainingWatchers != 0 {
		t.Errorf("expected 0 watchers after Stop, got %d", remainingWatchers)
	}
}

func TestStop_WaitsForGoroutines(t *testing.T) {
	watchCh := make(chan jetstream.KeyValueEntry, 1)
	kv := &mockKV{
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{ch: watchCh}, nil
		},
	}
	actor, _ := newTestActor(kv)
	actor.Subscribe(context.Background(), ">", func(_ context.Context, _ string) {})

	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		actor.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Stop returned — goroutines drained successfully.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return in time — goroutines not drained")
	}
}

// ===========================================================================
// Tests: Hold (distributed via KV CAS)
// ===========================================================================

func TestHold_CASUpdateBumpsRevision(t *testing.T) {
	// Hold should do a CAS update that pushes expiry far into the future.
	originalExpiry := time.Now().Add(5 * time.Minute)
	var updatedValue []byte
	var updatedWithRev uint64

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			return &mockEntry{
				key: key, value: marshalTime(originalExpiry),
				revision: 1, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, value []byte, last uint64) (uint64, error) {
			updatedValue = value
			updatedWithRev = last
			return 2, nil // new rev after CAS update
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()

	err := actor.Hold(holdCtx, "key")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}

	// Verify CAS was called with the original revision.
	if updatedWithRev != 1 {
		t.Errorf("CAS update used rev=%d, want 1", updatedWithRev)
	}

	// Verify the new expiry is far in the future (close to MaxHoldDuration).
	newExpiry, err := unmarshalTime(updatedValue)
	if err != nil {
		t.Fatal(err)
	}
	expectedHoldExpiry := time.Now().Add(10 * time.Minute)
	if newExpiry.Before(expectedHoldExpiry.Add(-5 * time.Second)) {
		t.Errorf("hold expiry %v is too early, expected ~%v", newExpiry, expectedHoldExpiry)
	}
}

func TestHold_InvalidatesAllTimers(t *testing.T) {
	// When Hold bumps the revision, any existing timer's tryFireCallback
	// should fail with revision mismatch — simulating multi-replica protection.
	originalExpiry := time.Now().Add(50 * time.Millisecond)
	var callbackCalled atomic.Bool

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			return &mockEntry{
				key: key, value: marshalTime(originalExpiry),
				revision: 1, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, _ []byte, _ uint64) (uint64, error) {
			return 2, nil // bumped to rev 2
		},
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			// Simulate: the timer tries Delete(key, LastRevision(1)),
			// but the current revision is 2 (bumped by Hold).
			return &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	// Schedule a timer with rev=1 that expires in 50ms.
	actor.scheduleTimer("key", 1, 50*time.Millisecond, func(_ context.Context, _ string) {
		callbackCalled.Store(true)
	})

	// Hold the key — this bumps revision to 2.
	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()
	err := actor.Hold(holdCtx, "key")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}

	// Wait for the timer to fire (it should fail due to revision mismatch).
	time.Sleep(200 * time.Millisecond)

	if callbackCalled.Load() {
		t.Error("callback should NOT fire — Hold bumped the revision, making the timer's Delete fail")
	}
}

func TestHold_SeamlessTransition(t *testing.T) {
	// When Add() is called during Hold, revision changes.
	// On hold release, the goroutine detects this and skips restore.
	originalExpiry := time.Now().Add(5 * time.Minute)

	var currentRev atomic.Uint64
	currentRev.Store(1)

	var restoreCalled atomic.Bool

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			rev := currentRev.Load()
			return &mockEntry{
				key: key, value: marshalTime(originalExpiry),
				revision: rev, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, _ []byte, last uint64) (uint64, error) {
			newRev := last + 1
			currentRev.Store(newRev)
			return newRev, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())

	err := actor.Hold(holdCtx, "key")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}
	// After Hold: rev = 2 (original was 1, Hold CAS bumped to 2).
	// holdRev captured inside Hold = 2.

	// Simulate Add() during the hold — this bumps revision further.
	currentRev.Store(3)

	// Override updateFn to detect restore attempts.
	kv.updateFn = func(_ context.Context, _ string, _ []byte, _ uint64) (uint64, error) {
		restoreCalled.Store(true)
		return 0, errors.New("should not be called")
	}

	// Release hold — should detect revision changed (3 ≠ 2) and skip restore.
	holdCancel()
	time.Sleep(200 * time.Millisecond) // let goroutine run

	if restoreCalled.Load() {
		t.Error("restore should NOT be called when revision changed during hold (seamless transition)")
	}
}

func TestHold_RestoresOnFailure(t *testing.T) {
	// When hold releases without Add() (transaction failed), the original
	// expiry should be restored.
	originalExpiry := time.Now().Add(5 * time.Minute)

	var currentRev atomic.Uint64
	currentRev.Store(1)

	var restoredValue []byte
	var restoredWithRev uint64

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			rev := currentRev.Load()
			return &mockEntry{
				key: key, value: marshalTime(originalExpiry),
				revision: rev, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, value []byte, last uint64) (uint64, error) {
			currentRev.Store(last + 1)
			restoredValue = value
			restoredWithRev = last
			return last + 1, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())

	err := actor.Hold(holdCtx, "key")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}
	// After Hold: rev = 2 (CAS bumped 1→2). holdRev = 2.

	// Release hold WITHOUT calling Add() — simulate transaction failure.
	holdCancel()
	time.Sleep(200 * time.Millisecond) // let goroutine run

	// The restore CAS should have been called with holdRev=2.
	if restoredWithRev != 2 {
		t.Errorf("restore CAS used rev=%d, want 2", restoredWithRev)
	}

	// Verify original expiry was restored.
	restored, err := unmarshalTime(restoredValue)
	if err != nil {
		t.Fatal(err)
	}
	if !restored.Equal(originalExpiry) {
		t.Errorf("restored expiry = %v, want %v", restored, originalExpiry)
	}
}

func TestHold_GetError(t *testing.T) {
	kv := &mockKV{
		getFn: func(_ context.Context, _ string) (jetstream.KeyValueEntry, error) {
			return nil, jetstream.ErrKeyNotFound
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()

	err := actor.Hold(holdCtx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

func TestHold_CASConflict(t *testing.T) {
	// If another replica modifies the key between Get and Update, Hold fails.
	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			return &mockEntry{
				key: key, value: marshalTime(time.Now().Add(time.Minute)),
				revision: 1, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, _ []byte, _ uint64) (uint64, error) {
			return 0, &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()

	err := actor.Hold(holdCtx, "key")
	if err == nil {
		t.Fatal("expected CAS conflict error")
	}
}

func TestHold_KeyDeletedDuringHold(t *testing.T) {
	// If the key is deleted while held, restore goroutine should handle
	// gracefully (no panic, no error).
	originalExpiry := time.Now().Add(5 * time.Minute)

	getCallCount := 0
	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			getCallCount++
			if getCallCount == 1 {
				// First call: Hold reads the current entry.
				return &mockEntry{
					key: key, value: marshalTime(originalExpiry),
					revision: 1, operation: jetstream.KeyValuePut,
				}, nil
			}
			// Subsequent calls: key was deleted during hold.
			return nil, jetstream.ErrKeyNotFound
		},
		updateFn: func(_ context.Context, _ string, _ []byte, _ uint64) (uint64, error) {
			return 2, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()

	holdCtx, holdCancel := context.WithCancel(context.Background())

	err := actor.Hold(holdCtx, "key")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}

	// Release — goroutine tries Get, gets ErrKeyNotFound, exits gracefully.
	holdCancel()
	time.Sleep(200 * time.Millisecond)
	// No panic = success.
}

func TestHold_WithSubscribe_FullFlow(t *testing.T) {
	// Full integration test: Subscribe + Hold + Add = seamless transition.
	// The old timer's callback should NOT fire (revision mismatch).
	// A new timer should be scheduled for the new expiry.
	watchCh := make(chan jetstream.KeyValueEntry, 10)
	callbackCh := make(chan string, 10)

	originalExpiry := time.Now().Add(50 * time.Millisecond)
	var currentRev atomic.Uint64
	currentRev.Store(1)

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			rev := currentRev.Load()
			return &mockEntry{
				key: key, value: marshalTime(originalExpiry),
				revision: rev, operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, _ []byte, last uint64) (uint64, error) {
			newRev := last + 1
			currentRev.Store(newRev)
			return newRev, nil
		},
		deleteFn: func(_ context.Context, _ string, _ ...jetstream.KVDeleteOpt) error {
			// All deletes fail with revision mismatch (Hold bumped the rev).
			return &jetstream.APIError{Code: 400, ErrorCode: 10071, Description: "wrong last sequence"}
		},
		watchFn: func(_ context.Context, _ string, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return &mockWatcher{ch: watchCh}, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	defer actor.Stop()

	actor.Subscribe(context.Background(), "test.*", func(_ context.Context, key string) {
		callbackCh <- key
	})

	// Step 1: Send an entry expiring in 50ms (rev=1).
	watchCh <- &mockEntry{
		key: "test.1", value: marshalTime(originalExpiry),
		revision: 1, operation: jetstream.KeyValuePut,
	}
	time.Sleep(20 * time.Millisecond) // let watcher schedule the timer

	// Step 2: Hold the key — CAS bumps rev to 2.
	holdCtx, holdCancel := context.WithCancel(context.Background())
	err := actor.Hold(holdCtx, "test.1")
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}

	// Step 3: The timer fires at 50ms, but Delete fails (rev 1 ≠ current 2).
	time.Sleep(200 * time.Millisecond)

	// Step 4: Simulate Add() during hold (rev bumps further).
	currentRev.Store(3)

	// Step 5: Release hold.
	holdCancel()
	time.Sleep(200 * time.Millisecond)

	// Verify no callback was fired.
	select {
	case key := <-callbackCh:
		t.Errorf("expected no callback, but got %q", key)
	default:
		// Good — no callback fired.
	}
}
