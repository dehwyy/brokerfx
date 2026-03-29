package timedactor

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Callback is the function signature for timeout callbacks.
// It receives the key (e.g. "order.123") and the metadata that was stored
// alongside the expiration timestamp.
type Callback[T any] func(ctx context.Context, key string, metadata T)

// TimedActor is a distributed timer/scheduler backed by NATS JetStream KV Store.
//
// It stores expiration timestamps and arbitrary metadata as key values in a KV
// bucket and watches for changes in real-time. When a key's expires_at time is
// reached, the actor attempts an atomic revision-guarded delete to guarantee
// exactly-once callback execution across multiple Kubernetes replicas.
//
// The type parameter T defines the metadata stored alongside each timer entry.
// This can be any JSON-serializable type (e.g. a timeout type enum, a struct
// with additional fields, etc.). The library itself does not interpret T — it
// simply marshals/unmarshals it and passes it to the registered callbacks.
type TimedActor[T any] struct {
	kv     jetstream.KeyValue
	logger zerolog.Logger
	config Config

	// mu guards the timers map and the subscribers slice.
	mu     sync.Mutex
	timers map[string]*time.Timer

	// subscribers holds all registered callback subscriptions (per key pattern).
	subscribers []subscriber[T]

	// watchers tracks all opened KV watchers so they can be stopped on shutdown.
	watchers []jetstream.KeyWatcher

	// wg tracks background goroutines started by Subscribe and Hold for
	// graceful shutdown.
	wg sync.WaitGroup

	// cancel cancels the root context shared by all background goroutines.
	cancel context.CancelFunc

	// ctx is the root context for all background goroutines.
	ctx context.Context
}

// subscriber represents a single callback registration bound to a metadata
// matcher function. When a timer fires, all subscribers whose Match function
// returns true for the entry's metadata are invoked.
type subscriber[T any] struct {
	// Match returns true if this subscriber should handle the given metadata.
	// A nil Match function means "match everything" (wildcard subscriber).
	Match    func(T) bool
	Callback Callback[T]
}

// payload is the internal structure stored as KV value.
// It wraps the expiration timestamp with arbitrary metadata.
type payload[T any] struct {
	ExpiresAt int64 `json:"e"` // Unix nanoseconds
	Metadata  T     `json:"m"`
}

// New creates a new TimedActor instance. It ensures the KV bucket exists,
// creating it if necessary. This function is intended to be called by Uber fx.
func New[T any](deps Deps) (*TimedActor[T], error) {
	cfg := deps.Config
	if cfg.BucketName == "" {
		cfg = DefaultConfig()
	}

	ctx := context.Background()

	// CreateOrUpdateKeyValue is idempotent: creates the bucket if it doesn't
	// exist, or returns the existing one with updated config.
	kv, err := deps.JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  cfg.BucketName,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		return nil, err
	}

	rootCtx, cancel := context.WithCancel(context.Background())

	return &TimedActor[T]{
		kv:     kv,
		logger: log.With().Str("component", "timed-actor").Logger(),
		config: cfg,
		timers: make(map[string]*time.Timer),
		cancel: cancel,
		ctx:    rootCtx,
	}, nil
}

// Add creates or updates a timer key in the KV bucket.
// It computes expires_at = now + timeout and stores the Unix-nano timestamp
// along with the provided metadata as JSON. If a Subscribe goroutine is running,
// it will pick up the change via the KV watcher and schedule the in-memory
// timer automatically.
func (ta *TimedActor[T]) Add(ctx context.Context, key string, metadata T, timeout time.Duration) error {
	expiresAt := time.Now().Add(timeout)
	data, err := marshalPayload(payload[T]{
		ExpiresAt: expiresAt.UnixNano(),
		Metadata:  metadata,
	})
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).Msg("failed to marshal payload")
		return err
	}

	_, err = ta.kv.Put(ctx, key, data)
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).Msg("failed to put timer key")
		return err
	}

	ta.logger.Debug().
		Str("key", key).
		Time("expires_at", expiresAt).
		Msg("timer key added/updated")

	return nil
}

// Hold temporarily suppresses callback execution for the given key across ALL
// replicas by performing a CAS (Compare-And-Swap) update in the NATS KV store.
//
// How it works:
//  1. Hold reads the current key value and its revision.
//  2. It atomically updates the expiration to a far-future time using
//     kv.Update (revision-guarded CAS). This bumps the revision, which
//     invalidates all in-flight timers on ALL replicas — their
//     kv.Delete(key, LastRevision(oldRev)) calls will fail with a
//     revision mismatch.
//  3. The KV watchers on all replicas pick up the new (far-future) value
//     and reschedule their timers accordingly — no callback fires.
//  4. When the hold's ctx is cancelled (transaction completes):
//     - If Add() was called during the hold (revision changed),
//     the new timeout takes over seamlessly — nothing to restore.
//     - If no Add() was called (revision unchanged — transaction failed),
//     the original expiration is restored via another CAS update.
//
// The metadata stored in the entry is preserved during Hold — only the
// expiration timestamp is modified.
//
// Usage:
//
//	holdCtx, holdCancel := context.WithCancel(ctx)
//	err := actor.Hold(holdCtx, "order.123")
//	if err != nil { ... }
//	// ... perform long transaction ...
//	actor.Add(ctx, "order.123", metadata, newTimeout) // set new timeout
//	holdCancel()                                       // release hold; old expiry discarded
//
// If the transaction fails and you don't call Add(), simply cancel the
// holdCtx — the original expiration will be automatically restored.
func (ta *TimedActor[T]) Hold(ctx context.Context, key string) error {
	// 1. Read the current entry to capture the original payload and revision.
	entry, err := ta.kv.Get(ctx, key)
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).Msg("hold: failed to get current entry")
		return err
	}

	originalPayload, err := unmarshalPayload[T](entry.Value())
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).Msg("hold: failed to parse current payload")
		return err
	}

	originalRev := entry.Revision()
	originalExpiry := time.Unix(0, originalPayload.ExpiresAt)

	// 2. CAS update: push expiration far into the future, preserving metadata.
	//    This bumps the revision, invalidating ALL replicas' pending timers.
	holdExpiry := time.Now().Add(ta.config.MaxHoldDuration)
	holdPayload := payload[T]{
		ExpiresAt: holdExpiry.UnixNano(),
		Metadata:  originalPayload.Metadata,
	}
	holdData, err := marshalPayload(holdPayload)
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).Msg("hold: failed to marshal hold payload")
		return err
	}

	holdRev, err := ta.kv.Update(ctx, key, holdData, originalRev)
	if err != nil {
		ta.logger.Error().Err(err).Str("key", key).
			Uint64("expected_rev", originalRev).
			Msg("hold: CAS update failed (key may have been modified concurrently)")
		return err
	}

	ta.logger.Debug().
		Str("key", key).
		Time("original_expiry", originalExpiry).
		Time("hold_expiry", holdExpiry).
		Uint64("original_rev", originalRev).
		Uint64("hold_rev", holdRev).
		Msg("hold acquired (distributed)")

	// 3. Background goroutine: wait for hold release, then restore or skip.
	ta.wg.Add(1)
	go func() {
		defer ta.wg.Done()

		select {
		case <-ctx.Done():
			// Hold released by caller — proceed to check/restore.
		case <-ta.ctx.Done():
			// Actor stopping — exit without restore.
			ta.logger.Debug().Str("key", key).Msg("actor stopping, hold abandoned")
			return
		}

		// Re-check the key's current state in KV.
		current, err := ta.kv.Get(ta.ctx, key)
		if err != nil {
			// Key was deleted or KV error — nothing to restore.
			ta.logger.Debug().Err(err).Str("key", key).
				Msg("hold released, key not found — skipping restore")
			return
		}

		if current.Revision() != holdRev {
			// Revision changed during hold — Add() or Clear() was called.
			// The new value takes over seamlessly. No restore needed.
			ta.logger.Info().
				Str("key", key).
				Uint64("hold_rev", holdRev).
				Uint64("current_rev", current.Revision()).
				Msg("hold released, revision changed — seamless transition")
			return
		}

		// Revision unchanged — transaction failed without calling Add().
		// Restore the original payload (with original expiry).
		originalData, err := marshalPayload(originalPayload)
		if err != nil {
			ta.logger.Error().Err(err).Str("key", key).
				Msg("hold released, failed to marshal original payload for restore")
			return
		}

		_, err = ta.kv.Update(ta.ctx, key, originalData, holdRev)
		if err != nil {
			ta.logger.Error().Err(err).Str("key", key).
				Time("original_expiry", originalExpiry).
				Msg("hold released, failed to restore original expiry")
			return
		}

		ta.logger.Info().
			Str("key", key).
			Time("original_expiry", originalExpiry).
			Msg("hold released, original expiry restored")
	}()

	return nil
}

// Clear removes a timer key from the KV bucket, effectively cancelling
// the scheduled timeout. The in-memory timer (if any) will be cleaned up
// by the watcher goroutine when it receives the delete marker.
func (ta *TimedActor[T]) Clear(ctx context.Context, key string) error {
	err := ta.kv.Delete(ctx, key)
	if err != nil {
		// Key might already be deleted; treat as non-fatal.
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		ta.logger.Error().Err(err).Str("key", key).Msg("failed to delete timer key")
		return err
	}

	// Eagerly remove the in-memory timer so the callback won't fire locally.
	ta.stopTimer(key)

	ta.logger.Debug().Str("key", key).Msg("timer key cleared")
	return nil
}

// List returns the expiration times and metadata for the requested keys.
// If no keys are provided, it returns all active keys in the bucket.
func (ta *TimedActor[T]) List(ctx context.Context, keys ...string) (map[string]Entry[T], error) {
	result := make(map[string]Entry[T])

	if len(keys) == 0 {
		// List all keys in the bucket.
		lister, err := ta.kv.ListKeys(ctx)
		if err != nil {
			// If bucket is empty, ListKeys may return an error.
			if errors.Is(err, jetstream.ErrNoKeysFound) {
				return result, nil
			}
			return nil, err
		}

		for key := range lister.Keys() {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		entry, err := ta.kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}

		p, err := unmarshalPayload[T](entry.Value())
		if err != nil {
			ta.logger.Warn().Err(err).Str("key", key).Msg("failed to parse payload, skipping")
			continue
		}

		result[key] = Entry[T]{
			ExpiresAt: time.Unix(0, p.ExpiresAt),
			Metadata:  p.Metadata,
		}
	}

	return result, nil
}

// Entry represents a timer entry returned by List.
type Entry[T any] struct {
	ExpiresAt time.Time
	Metadata  T
}

// Subscribe starts a background watcher that monitors keys matching eventKey
// (e.g. "order.*" or ">") and fires the callback exactly once (across all
// replicas) when a key's expiration time is reached.
//
// The match function filters entries by their metadata. Only entries whose
// metadata satisfies the match function will trigger this callback. Pass nil
// as match to subscribe to ALL entries regardless of metadata (wildcard).
//
// This method is intended to be called once at application startup. It launches
// goroutines that are tracked via sync.WaitGroup and cancelled via the actor's
// root context. Multiple calls with different eventKey patterns / match
// functions are supported.
func (ta *TimedActor[T]) Subscribe(ctx context.Context, eventKey string, match func(T) bool, callback Callback[T]) {
	ta.mu.Lock()
	ta.subscribers = append(ta.subscribers, subscriber[T]{
		Match:    match,
		Callback: callback,
	})
	ta.mu.Unlock()

	ta.wg.Add(1)

	go func() {
		defer ta.wg.Done()

		ta.logger.Info().Str("event_key", eventKey).Msg("starting subscribe watcher")

		// Open a KV watcher for the given key pattern.
		// The watcher will first send all existing values, then a nil sentinel,
		// then real-time updates.
		watcher, err := ta.kv.Watch(ta.ctx, eventKey)
		if err != nil {
			ta.logger.Error().Err(err).Str("event_key", eventKey).Msg("failed to create KV watcher")
			return
		}

		ta.mu.Lock()
		ta.watchers = append(ta.watchers, watcher)
		ta.mu.Unlock()

		// Safety-net ticker: periodically re-scan all tracked timers to catch
		// any keys that might have been missed due to reconnection or race.
		ticker := time.NewTicker(ta.config.CheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ta.ctx.Done():
				ta.logger.Info().Str("event_key", eventKey).Msg("subscribe watcher stopping")
				return

			case entry, ok := <-watcher.Updates():
				if !ok {
					ta.logger.Warn().Str("event_key", eventKey).Msg("watcher channel closed")
					return
				}

				// nil entry is the sentinel indicating all initial values have
				// been delivered; ignore it.
				if entry == nil {
					continue
				}

				ta.handleWatchEntry(entry)

			case <-ticker.C:
				ta.rescanOverdueTimers()
			}
		}
	}()
}

// Stop cancels all background goroutines spawned by Subscribe and waits for
// them to finish. It also stops all opened KV watchers. This method is called
// by the fx.Lifecycle OnStop hook.
func (ta *TimedActor[T]) Stop() {
	ta.logger.Info().Msg("timed-actor stopping")

	// Signal all goroutines to exit.
	ta.cancel()

	// Stop all KV watchers.
	ta.mu.Lock()
	for _, w := range ta.watchers {
		_ = w.Stop()
	}
	ta.watchers = nil

	// Stop all in-memory timers.
	for key, t := range ta.timers {
		t.Stop()
		delete(ta.timers, key)
	}
	ta.mu.Unlock()

	// Wait for all goroutines to finish.
	ta.wg.Wait()

	ta.logger.Info().Msg("timed-actor stopped")
}

// handleWatchEntry processes a single KV watcher entry. For put/update
// operations it schedules (or reschedules) an in-memory timer. For delete
// operations it clears the timer.
func (ta *TimedActor[T]) handleWatchEntry(entry jetstream.KeyValueEntry) {
	key := entry.Key()

	// A delete marker means the key was removed (either by Clear or by a
	// successful atomic delete from another replica). Remove the local timer.
	if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
		ta.stopTimer(key)
		return
	}

	p, err := unmarshalPayload[T](entry.Value())
	if err != nil {
		ta.logger.Warn().Err(err).Str("key", key).Msg("invalid payload, skipping")
		return
	}

	expiresAt := time.Unix(0, p.ExpiresAt)
	rev := entry.Revision()
	delay := time.Until(expiresAt)
	if delay < 0 {
		delay = 0
	}

	ta.scheduleTimer(key, rev, delay, p.Metadata)
}

// scheduleTimer creates or replaces an in-memory timer for the given key.
// When the timer fires, it attempts an atomic revision-guarded delete.
func (ta *TimedActor[T]) scheduleTimer(key string, rev uint64, delay time.Duration, metadata T) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// If there's an existing timer for this key, stop it before replacing.
	if existing, ok := ta.timers[key]; ok {
		existing.Stop()
	}

	ta.timers[key] = time.AfterFunc(delay, func() {
		ta.tryFireCallback(key, rev, metadata)
	})

	ta.logger.Debug().
		Str("key", key).
		Uint64("rev", rev).
		Dur("delay", delay).
		Msg("timer scheduled")
}

// tryFireCallback attempts to atomically delete the key with a revision guard.
// This is the core mechanism for exactly-once guarantees in a multi-replica
// environment.
//
// How it works:
//  1. The timer fires on ALL replicas that have this key in their watch set.
//  2. Each replica calls kv.Delete(key, LastRevision(rev)).
//  3. Only the FIRST replica to execute this call succeeds — NATS JetStream
//     will accept the delete only if the current revision matches `rev`.
//  4. All subsequent replicas receive ErrKeyWrongLastRevision because the
//     revision was already bumped by the successful delete. They silently
//     skip the callback.
//
// This provides an exactly-once guarantee: only one replica fires the callback,
// even though all replicas attempt the delete concurrently.
//
// When a key is held (via Hold), the revision has already been bumped by the
// CAS update, so this function's kv.Delete with the old revision will fail
// with a revision mismatch — the callback is silently skipped.
func (ta *TimedActor[T]) tryFireCallback(key string, rev uint64, metadata T) {
	// Clean up the in-memory timer reference regardless of outcome.
	ta.mu.Lock()
	delete(ta.timers, key)
	ta.mu.Unlock()

	// Attempt the atomic, revision-guarded delete.
	// If the key's current revision no longer matches `rev`, another replica
	// has already claimed this timer — we silently skip.
	err := ta.kv.Delete(ta.ctx, key, jetstream.LastRevision(rev))
	if err != nil {
		// ErrKeyWrongLastRevision (or key already deleted) means another pod
		// won the race. This is the expected path for losing replicas.
		if isRevisionMismatch(err) || errors.Is(err, jetstream.ErrKeyNotFound) {
			ta.logger.Debug().
				Str("key", key).
				Uint64("rev", rev).
				Msg("lost race for key deletion, another replica handled it")
			return
		}

		// Unexpected error (network issue, etc). Log and do not fire callback
		// to avoid potential double-execution.
		ta.logger.Error().Err(err).
			Str("key", key).
			Uint64("rev", rev).
			Msg("unexpected error during atomic delete, skipping callback")
		return
	}

	// This replica successfully deleted the key — it "won" the race.
	// Execute matching callbacks.
	ta.logger.Info().Str("key", key).Uint64("rev", rev).Msg("won race, firing callbacks")
	ta.fireMatchingCallbacks(key, metadata)
}

// fireMatchingCallbacks invokes all registered subscribers whose Match function
// accepts the given metadata. If a subscriber has no Match function (nil), it
// is treated as a wildcard and always invoked.
func (ta *TimedActor[T]) fireMatchingCallbacks(key string, metadata T) {
	ta.mu.Lock()
	subs := make([]subscriber[T], len(ta.subscribers))
	copy(subs, ta.subscribers)
	ta.mu.Unlock()

	for _, sub := range subs {
		if sub.Match == nil || sub.Match(metadata) {
			sub.Callback(ta.ctx, key, metadata)
		}
	}
}

// rescanOverdueTimers is a safety-net that re-checks all tracked keys.
// If any key's timer has already expired but no timer is scheduled (e.g. due
// to a missed watch event), it will attempt to fire the callback.
func (ta *TimedActor[T]) rescanOverdueTimers() {
	ta.mu.Lock()
	// Collect keys that currently have scheduled timers — these are already
	// being handled, so we skip them.
	tracked := make(map[string]struct{}, len(ta.timers))
	for k := range ta.timers {
		tracked[k] = struct{}{}
	}
	ta.mu.Unlock()

	// If there are tracked timers, we don't need to rescan — the timers will
	// fire on their own. This ticker is only for keys that somehow slipped
	// through (e.g. watch reconnect).
	//
	// We list all keys and check if any are overdue but not tracked.
	keys, err := ta.kv.ListKeys(ta.ctx)
	if err != nil {
		// No keys or error — nothing to do.
		return
	}

	now := time.Now()

	for key := range keys.Keys() {
		if _, ok := tracked[key]; ok {
			continue
		}

		entry, err := ta.kv.Get(ta.ctx, key)
		if err != nil {
			continue
		}

		p, err := unmarshalPayload[T](entry.Value())
		if err != nil {
			continue
		}

		expiresAt := time.Unix(0, p.ExpiresAt)

		if now.After(expiresAt) {
			// Key is overdue and not tracked — attempt to fire.
			ta.logger.Warn().
				Str("key", key).
				Time("expires_at", expiresAt).
				Msg("rescan found overdue key, attempting callback")
			ta.tryFireCallback(key, entry.Revision(), p.Metadata)
		} else {
			// Key is not yet expired but not tracked — re-schedule.
			ta.scheduleTimer(key, entry.Revision(), time.Until(expiresAt), p.Metadata)
		}
	}
}

// stopTimer stops and removes the in-memory timer for the given key.
func (ta *TimedActor[T]) stopTimer(key string) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	if t, ok := ta.timers[key]; ok {
		t.Stop()
		delete(ta.timers, key)
	}
}

// marshalPayload serializes a payload to JSON bytes.
func marshalPayload[T any](p payload[T]) ([]byte, error) {
	return json.Marshal(p)
}

// unmarshalPayload deserializes a payload from JSON bytes.
// For backward compatibility, if JSON unmarshal fails, it attempts to parse
// the data as a legacy plain Unix-nano timestamp (without metadata).
func unmarshalPayload[T any](data []byte) (payload[T], error) {
	var p payload[T]
	if err := json.Unmarshal(data, &p); err != nil {
		// Backward compatibility: try parsing as legacy plain timestamp.
		ns, parseErr := strconv.ParseInt(string(data), 10, 64)
		if parseErr != nil {
			return p, err // return original JSON error
		}
		p.ExpiresAt = ns
		// Metadata remains zero value of T.
		return p, nil
	}
	return p, nil
}

// isRevisionMismatch checks whether the error indicates that the key's revision
// did not match the expected value (i.e., another replica modified/deleted the
// key first). The new jetstream package surfaces this as a specific API error.
func isRevisionMismatch(err error) bool {
	// The new nats.go/jetstream package returns jetstream.ErrKeyWrongLastRevision
	// (or wraps the API error with "wrong last sequence"). We check both the
	// sentinel and the error message for robustness.
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		// NATS server error code 10071 = "wrong last sequence"
		return apiErr.ErrorCode == 10071
	}
	return strings.Contains(err.Error(), "wrong last sequence")
}
