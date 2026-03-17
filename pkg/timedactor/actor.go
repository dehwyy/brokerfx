package timedactor

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TimedActor is a distributed timer/scheduler backed by NATS JetStream KV Store.
//
// It stores expiration timestamps as key values in a KV bucket and watches for
// changes in real-time. When a key's expires_at time is reached, the actor
// attempts an atomic revision-guarded delete to guarantee exactly-once callback
// execution across multiple Kubernetes replicas.
type TimedActor struct {
	kv     jetstream.KeyValue
	logger zerolog.Logger
	config Config

	// mu guards the timers map.
	mu     sync.Mutex
	timers map[string]*time.Timer

	// watchers tracks all opened KV watchers so they can be stopped on shutdown.
	watchers []jetstream.KeyWatcher

	// wg tracks background goroutines started by Subscribe for graceful shutdown.
	wg sync.WaitGroup

	// cancel cancels the root context shared by all background goroutines.
	cancel context.CancelFunc

	// ctx is the root context for all background goroutines.
	ctx context.Context
}

// New creates a new TimedActor instance. It ensures the KV bucket exists,
// creating it if necessary. This function is intended to be called by Uber fx.
func New(deps Deps) (*TimedActor, error) {
	cfg := deps.Config
	if cfg.BucketName == "" {
		cfg = DefaultConfig()
	}

	ctx := context.Background()

	// CreateOrUpdateKeyValue is idempotent: creates the bucket if it doesn't
	// exist, or returns the existing one with updated config.
	kv, err := deps.JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: cfg.BucketName,
	})
	if err != nil {
		return nil, err
	}

	rootCtx, cancel := context.WithCancel(context.Background())

	return &TimedActor{
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
// as the key's payload. If a Subscribe goroutine is running, it will pick up
// the change via the KV watcher and schedule the in-memory timer automatically.
func (ta *TimedActor) Add(ctx context.Context, key string, timeout time.Duration) error {
	expiresAt := time.Now().Add(timeout)
	payload := marshalTime(expiresAt)

	_, err := ta.kv.Put(ctx, key, payload)
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

// Clear removes a timer key from the KV bucket, effectively cancelling
// the scheduled timeout. The in-memory timer (if any) will be cleaned up
// by the watcher goroutine when it receives the delete marker.
func (ta *TimedActor) Clear(ctx context.Context, key string) error {
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

// List returns the expiration times for the requested keys. If no keys are
// provided, it returns all active keys in the bucket.
func (ta *TimedActor) List(ctx context.Context, keys ...string) (map[string]time.Time, error) {
	result := make(map[string]time.Time)

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

		t, err := unmarshalTime(entry.Value())
		if err != nil {
			ta.logger.Warn().Err(err).Str("key", key).Msg("failed to parse expires_at, skipping")
			continue
		}

		result[key] = t
	}

	return result, nil
}

// Subscribe starts a background watcher that monitors keys matching eventKey
// (e.g. "order.*" or ">") and fires the callback exactly once (across all
// replicas) when a key's expiration time is reached.
//
// This method is intended to be called once at application startup. It launches
// goroutines that are tracked via sync.WaitGroup and cancelled via the actor's
// root context. Multiple calls with different eventKey patterns are supported.
func (ta *TimedActor) Subscribe(ctx context.Context, eventKey string, callback func(ctx context.Context, key string)) {
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

				ta.handleWatchEntry(entry, callback)

			case <-ticker.C:
				ta.rescanOverdueTimers(callback)
			}
		}
	}()
}

// Stop cancels all background goroutines spawned by Subscribe and waits for
// them to finish. It also stops all opened KV watchers. This method is called
// by the fx.Lifecycle OnStop hook.
func (ta *TimedActor) Stop() {
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
func (ta *TimedActor) handleWatchEntry(entry jetstream.KeyValueEntry, callback func(ctx context.Context, key string)) {
	key := entry.Key()

	// A delete marker means the key was removed (either by Clear or by a
	// successful atomic delete from another replica). Remove the local timer.
	if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
		ta.stopTimer(key)
		return
	}

	expiresAt, err := unmarshalTime(entry.Value())
	if err != nil {
		ta.logger.Warn().Err(err).Str("key", key).Msg("invalid expires_at payload, skipping")
		return
	}

	rev := entry.Revision()
	delay := time.Until(expiresAt)
	if delay < 0 {
		delay = 0
	}

	ta.scheduleTimer(key, rev, delay, callback)
}

// scheduleTimer creates or replaces an in-memory timer for the given key.
// When the timer fires, it attempts an atomic revision-guarded delete.
func (ta *TimedActor) scheduleTimer(key string, rev uint64, delay time.Duration, callback func(ctx context.Context, key string)) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// If there's an existing timer for this key, stop it before replacing.
	if existing, ok := ta.timers[key]; ok {
		existing.Stop()
	}

	ta.timers[key] = time.AfterFunc(delay, func() {
		ta.tryFireCallback(key, rev, callback)
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
func (ta *TimedActor) tryFireCallback(key string, rev uint64, callback func(ctx context.Context, key string)) {
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
	// Execute the callback.
	ta.logger.Info().Str("key", key).Uint64("rev", rev).Msg("won race, firing callback")
	callback(ta.ctx, key)
}

// rescanOverdueTimers is a safety-net that re-checks all tracked keys.
// If any key's timer has already expired but no timer is scheduled (e.g. due
// to a missed watch event), it will attempt to fire the callback.
func (ta *TimedActor) rescanOverdueTimers(callback func(ctx context.Context, key string)) {
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

		expiresAt, err := unmarshalTime(entry.Value())
		if err != nil {
			continue
		}

		if now.After(expiresAt) {
			// Key is overdue and not tracked — attempt to fire.
			ta.logger.Warn().
				Str("key", key).
				Time("expires_at", expiresAt).
				Msg("rescan found overdue key, attempting callback")
			ta.tryFireCallback(key, entry.Revision(), callback)
		} else {
			// Key is not yet expired but not tracked — re-schedule.
			ta.scheduleTimer(key, entry.Revision(), time.Until(expiresAt), callback)
		}
	}
}

// stopTimer stops and removes the in-memory timer for the given key.
func (ta *TimedActor) stopTimer(key string) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	if t, ok := ta.timers[key]; ok {
		t.Stop()
		delete(ta.timers, key)
	}
}

// marshalTime converts a time.Time to a byte slice by encoding its Unix
// nanosecond timestamp as a decimal string. This avoids JSON overhead while
// being deterministic across platforms.
func marshalTime(t time.Time) []byte {
	return []byte(strconv.FormatInt(t.UnixNano(), 10))
}

// unmarshalTime parses a byte slice produced by marshalTime back into time.Time.
func unmarshalTime(data []byte) (time.Time, error) {
	ns, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ns), nil
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
