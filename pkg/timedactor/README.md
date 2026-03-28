# TimedActor

Distributed timer/scheduler backed by NATS JetStream KV Store with exactly-once callback guarantees across multiple Kubernetes replicas.

## Key Features

- **Exactly-once callback execution** — only one replica fires the callback, even with N pods running
- **Distributed Hold** — temporarily suppress timeouts during long transactions without losing them
- **Seamless timeout transitions** — set a new timeout mid-transaction; the old one is silently discarded
- **Crash-safe** — timeouts persist in NATS KV and survive pod restarts
- **Safety-net rescan** — periodic re-check catches any missed events due to reconnection

## Tech Stack

- **Language**: Go 1.21+
- **Storage**: NATS JetStream KV Store
- **DI Framework**: Uber fx
- **Logging**: zerolog

## Architecture

### How the Distributed Timer Works

```
                    NATS JetStream KV
                   ┌──────────────────┐
                   │ order.123 = T+5m │ ← expiration timestamp
                   │ rev = 1          │
                   └────────┬─────────┘
                            │ Watch events
               ┌────────────┼────────────┐
               ▼            ▼            ▼
          ┌─────────┐  ┌─────────┐  ┌─────────┐
          │Replica A│  │Replica B│  │Replica C│
          │timer 5m │  │timer 5m │  │timer 5m │
          └────┬────┘  └────┬────┘  └────┬────┘
               │            │            │
         Timer fires   Timer fires  Timer fires
               │            │            │
         Delete(rev=1) Delete(rev=1) Delete(rev=1)
               │            │            │
            SUCCESS      FAIL ✗       FAIL ✗
          (rev match)  (rev changed) (rev changed)
               │
          Fire callback ✓  (exactly once)
```

### Exactly-Once Guarantee

When a timer expires, ALL replicas attempt to atomically delete the key using a **revision-guarded CAS** operation:

```go
kv.Delete(key, jetstream.LastRevision(rev))
```

Only the **first** replica to execute this succeeds — NATS JetStream rejects subsequent attempts because the revision has already changed. This provides exactly-once callback execution without any external coordination.

### Distributed Hold

The `Hold()` mechanism allows temporarily suppressing timeout callbacks during long, multi-step transactions. It works **across all replicas** by modifying the shared KV state.

```
Timeline:
────────────────────────────────────────────────────────────

T=0    Add("order.123", 5min)     → KV: expires=T+5m, rev=1
       All replicas schedule timer for T+5m

T=3m   Hold(ctx, "order.123")     → KV: expires=T+10m, rev=2 (CAS)
       ↑ Push expiry far into future via CAS update
       ↑ Bumps revision 1→2
       ↑ All replicas' existing timers become INVALID
       ↑ Watchers reschedule timers for T+10m

T=5m   Original timer would have fired...
       But rev=1 timers already cancelled ✓
       No callback fires on ANY replica ✓

T=4m   Add("order.123", 5min)     → KV: expires=T+9m, rev=3
       Set new timeout during transaction

T=4m   holdCancel()               → Hold goroutine wakes up
       Checks: current rev=3 ≠ hold rev=2
       → Seamless transition, no restore needed ✓
       Watchers already scheduled timer for rev=3 ✓
```

#### Hold Scenarios

| Scenario | What Happens |
|---|---|
| **Normal flow** (Add + cancel) | New timeout takes over seamlessly (revision changed) |
| **Transaction fails** (cancel without Add) | Original expiry is restored via CAS |
| **Process crashes during Hold** | Timeout fires after MaxHoldDuration (safety net) |
| **Concurrent Hold from 2 replicas** | Second Hold fails with CAS error (only one can hold) |
| **Key deleted during Hold** | Restore goroutine sees ErrKeyNotFound, exits gracefully |

## API

### `Add(ctx, key, timeout) error`

Creates or updates a timer. Computes `expires_at = now + timeout` and stores it in the KV bucket.

```go
err := actor.Add(ctx, "order.123", 5*time.Minute)
```

### `Hold(ctx, key) error`

Temporarily suppresses the timeout callback across all replicas. The hold is released when `ctx` is cancelled.

```go
holdCtx, holdCancel := context.WithCancel(ctx)

// Acquire the hold — pushes expiry far into future in KV.
err := actor.Hold(holdCtx, "order.123")
if err != nil {
    return err // CAS failed or key not found
}

// Perform long transaction...
err = doMultiStepTransaction(ctx)
if err != nil {
    // Transaction failed — cancel hold, original expiry is restored automatically.
    holdCancel()
    return err
}

// Transaction succeeded — set new timeout, then release hold.
actor.Add(ctx, "order.123", 10*time.Minute)
holdCancel() // old expiry discarded (seamless transition)
```

### `Clear(ctx, key) error`

Removes a timer key from the KV bucket, cancelling the timeout.

```go
err := actor.Clear(ctx, "order.123")
```

### `List(ctx, keys...) (map[string]time.Time, error)`

Returns expiration times. Without arguments, returns all active keys.

```go
// List all
all, err := actor.List(ctx)

// List specific keys
subset, err := actor.List(ctx, "order.123", "order.456")
```

### `Subscribe(ctx, eventKey, callback)`

Starts a background watcher that fires the callback exactly once when a key expires.

```go
actor.Subscribe(ctx, "order.*", func(ctx context.Context, key string) {
    log.Info().Str("key", key).Msg("order timed out!")
    // Handle timeout — this runs on exactly ONE replica.
})
```

### `Stop()`

Cancels all background goroutines and waits for graceful shutdown. Called automatically by the fx lifecycle.

## Usage with Uber fx

```go
import "github.com/dehwyy/brokerfx/pkg/timedactor"

func main() {
    fx.New(
        // Provide JetStream connection
        fx.Provide(newJetStream),

        // Optionally provide custom config
        fx.Provide(func() timedactor.Config {
            return timedactor.Config{
                BucketName:      "my-timers",
                CheckInterval:   15 * time.Second,
                MaxHoldDuration: 5 * time.Minute,
            }
        }),

        // Register TimedActor module
        timedactor.Module,

        // Use TimedActor
        fx.Invoke(func(actor *timedactor.TimedActor) {
            actor.Subscribe(context.Background(), "order.*", handleOrderTimeout)
        }),
    ).Run()
}
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `BucketName` | `string` | `"TimedActorBucket"` | NATS KV bucket name for timer entries |
| `CheckInterval` | `time.Duration` | `15s` | Safety-net polling interval for missed events |
| `MaxHoldDuration` | `time.Duration` | `30s` | Max time a Hold suppresses a timeout. Acts as a safety net if the holding process crashes |

## File Structure

```
pkg/timedactor/
├── actor.go       # Core TimedActor implementation (Add, Hold, Clear, List, Subscribe)
├── actor_test.go  # Unit tests with full mock coverage
├── config.go      # Config struct, Deps, and defaults
├── module.go      # Uber fx module and lifecycle hooks
└── README.md      # This file
```

## Testing

```bash
go test -v ./pkg/timedactor/...
```

All tests use in-memory mocks — no NATS server required.

### Test Coverage

| Area | Tests |
|---|---|
| `marshalTime/unmarshalTime` | Round-trip, epoch, invalid data |
| `isRevisionMismatch` | API error, string match, unrelated |
| `New` | Default config, custom config, bucket error |
| `Add` | Success, put error, overwrite |
| `Clear` | Success, key not found, delete error |
| `List` | Empty, all keys, specific keys |
| `tryFireCallback` | Wins race, loses race, key already deleted |
| `Subscribe` | Callback on expiry, revision mismatch, delete marker |
| `Stop` | Cancels timers, stops watchers, waits for goroutines |
| `Hold` | CAS update, invalidates timers, seamless transition, restore on failure, get error, CAS conflict, key deleted during hold, full flow with Subscribe |

## Edge Cases

### What happens if the process crashes during Hold?

The key in KV has its expiry pushed forward by `MaxHoldDuration` (default: 10 minutes). After this time, the timer fires normally. The timeout is delayed but **not lost**.

### Can two replicas Hold the same key?

No. `Hold()` uses a CAS (Compare-And-Swap) update. The second replica's `kv.Update(key, value, expectedRev)` will fail because the first Hold already bumped the revision. The second Hold returns an error.

### What if Add() is called without Hold?

It works normally — `kv.Put` updates the value and bumps the revision. All replicas' watchers pick up the new value and reschedule their timers.

### What if Hold() is called on a non-existent key?

`Hold()` returns an error (`jetstream.ErrKeyNotFound`). You must call `Add()` first.
