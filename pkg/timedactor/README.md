# TimedActor

Distributed timer/scheduler backed by NATS JetStream KV Store with exactly-once callback guarantees across multiple Kubernetes replicas.

## Key Features

- **Exactly-once callback execution** — only one replica fires the callback, even with N pods running
- **Generic metadata** — store arbitrary data alongside each timer (type parameter `T`)
- **Per-metadata routing** — register multiple subscribers with match functions for selective callback dispatch
- **Distributed Hold** — temporarily suppress timeouts during long transactions without losing them
- **Seamless timeout transitions** — set a new timeout mid-transaction; the old one is silently discarded
- **Crash-safe** — timeouts persist in NATS KV and survive pod restarts
- **Safety-net rescan** — periodic re-check catches any missed events due to reconnection
- **Backward compatible** — legacy plain-timestamp entries are parsed automatically

## Tech Stack

- **Language**: Go 1.21+ (with generics)
- **Storage**: NATS JetStream KV Store
- **DI Framework**: Uber fx
- **Logging**: zerolog

## Architecture

### How the Distributed Timer Works

```
                    NATS JetStream KV
                   ┌──────────────────────────────────────────┐
                   │ order.123 = {"e":T+5m, "m":{...}}       │
                   │ rev = 1                                  │
                   └────────┬─────────────────────────────────┘
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
          Match metadata → Fire subscriber callbacks ✓
```

### Exactly-Once Guarantee

When a timer expires, ALL replicas attempt to atomically delete the key using a **revision-guarded CAS** operation:

```go
kv.Delete(key, jetstream.LastRevision(rev))
```

Only the **first** replica to execute this succeeds — NATS JetStream rejects subsequent attempts because the revision has already changed. This provides exactly-once callback execution without any external coordination.

### Metadata-Based Routing

Each timer entry stores arbitrary metadata of type `T` alongside the expiration timestamp. When subscribing, you provide a **match function** that filters which entries trigger your callback:

```go
// Only fires for entries where metadata.Type == "VISIT_RESOURCE"
actor.Subscribe(ctx, "order.*",
    func(m MyMeta) bool { return m.Type == "VISIT_RESOURCE" },
    handleVisitTimeout,
)

// Only fires for entries where metadata.Type == "ATTACH_RECEIPT"
actor.Subscribe(ctx, "order.*",
    func(m MyMeta) bool { return m.Type == "ATTACH_RECEIPT" },
    handleReceiptTimeout,
)

// Wildcard: fires for ALL entries (e.g. logging/monitoring)
actor.Subscribe(ctx, "order.*", nil, logAllTimeouts)
```

Since there's only **one KV key per order** (the metadata is overwritten atomically via `kv.Put`), switching timeout types is a single, atomic operation with no race conditions.

### Distributed Hold

The `Hold()` mechanism allows temporarily suppressing timeout callbacks during long, multi-step transactions. It works **across all replicas** by modifying the shared KV state. The metadata is preserved during hold.

```
Timeline:
────────────────────────────────────────────────────────────

T=0    Add("order.123", meta, 5min)  → KV: {e:T+5m, m:meta}, rev=1
       All replicas schedule timer for T+5m

T=3m   Hold(ctx, "order.123")        → KV: {e:T+10m, m:meta}, rev=2 (CAS)
       ↑ Push expiry far into future via CAS update
       ↑ Bumps revision 1→2
       ↑ All replicas' existing timers become INVALID
       ↑ Watchers reschedule timers for T+10m
       ↑ Metadata preserved

T=5m   Original timer would have fired...
       But rev=1 timers already cancelled ✓
       No callback fires on ANY replica ✓

T=4m   Add("order.123", newMeta, 5m) → KV: {e:T+9m, m:newMeta}, rev=3
       Set new timeout (with new metadata) during transaction

T=4m   holdCancel()                  → Hold goroutine wakes up
       Checks: current rev=3 ≠ hold rev=2
       → Seamless transition, no restore needed ✓
       Watchers already scheduled timer for rev=3 ✓
```

#### Hold Scenarios

| Scenario | What Happens |
|---|---|
| **Normal flow** (Add + cancel) | New timeout takes over seamlessly (revision changed) |
| **Transaction fails** (cancel without Add) | Original expiry AND metadata restored via CAS |
| **Process crashes during Hold** | Timeout fires after MaxHoldDuration (safety net) |
| **Concurrent Hold from 2 replicas** | Second Hold fails with CAS error (only one can hold) |
| **Key deleted during Hold** | Restore goroutine sees ErrKeyNotFound, exits gracefully |

## API

### `Add(ctx, key, metadata, timeout) error`

Creates or updates a timer. Computes `expires_at = now + timeout` and stores it alongside the metadata in the KV bucket.

```go
err := actor.Add(ctx, "order.123", MyMeta{Type: "VISIT_RESOURCE"}, 5*time.Minute)
```

### `Hold(ctx, key) error`

Temporarily suppresses the timeout callback across all replicas. The hold is released when `ctx` is cancelled. Metadata is preserved.

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
    // Transaction failed — cancel hold, original expiry + metadata restored automatically.
    holdCancel()
    return err
}

// Transaction succeeded — set new timeout with new metadata, then release hold.
actor.Add(ctx, "order.123", MyMeta{Type: "SELECT_REQUISITE"}, 10*time.Minute)
holdCancel() // old expiry discarded (seamless transition)
```

### `Clear(ctx, key) error`

Removes a timer key from the KV bucket, cancelling the timeout.

```go
err := actor.Clear(ctx, "order.123")
```

### `List(ctx, keys...) (map[string]Entry[T], error)`

Returns expiration times and metadata. Without arguments, returns all active keys.

```go
// List all
all, err := actor.List(ctx)
for key, entry := range all {
    fmt.Printf("key=%s expires=%v meta=%+v\n", key, entry.ExpiresAt, entry.Metadata)
}

// List specific keys
subset, err := actor.List(ctx, "order.123", "order.456")
```

### `Subscribe(ctx, eventKey, match, callback)`

Starts a background watcher that fires the callback exactly once when a key expires and its metadata matches the match function.

```go
// Per-type subscription:
actor.Subscribe(ctx, "order.*",
    func(m MyMeta) bool { return m.Type == "VISIT_RESOURCE" },
    func(ctx context.Context, key string, meta MyMeta) {
        log.Info().Str("key", key).Msg("visit resource timed out!")
    },
)

// Wildcard subscription (match = nil):
actor.Subscribe(ctx, "order.*", nil, func(ctx context.Context, key string, meta MyMeta) {
    log.Info().Str("key", key).Interface("meta", meta).Msg("any timeout fired!")
})
```

### `Stop()`

Cancels all background goroutines and waits for graceful shutdown. Called automatically by the fx lifecycle.

## Usage with Uber fx

```go
import "github.com/dehwyy/brokerfx/pkg/timedactor"

// Define your metadata type
type TimeoutMeta struct {
    Type string `json:"type"`
}

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

        // Register TimedActor module (generic!)
        timedactor.Module[TimeoutMeta](),

        // Use TimedActor
        fx.Invoke(func(actor *timedactor.TimedActor[TimeoutMeta]) {
            actor.Subscribe(context.Background(), "order.*",
                func(m TimeoutMeta) bool { return m.Type == "VISIT_RESOURCE" },
                handleVisitTimeout,
            )
            actor.Subscribe(context.Background(), "order.*",
                func(m TimeoutMeta) bool { return m.Type == "SELECT_REQUISITE" },
                handleSelectTimeout,
            )
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

## KV Payload Format

Each KV entry stores a JSON payload:

```json
{"e": 1743200000000000000, "m": {"type": "VISIT_RESOURCE"}}
```

| Field | Description |
|---|---|
| `e` | Expiration timestamp (Unix nanoseconds) |
| `m` | Arbitrary metadata of type `T` |

**Backward compatibility**: If the value is a plain integer string (legacy format), it's parsed as a Unix-nano timestamp with zero-value metadata.

## File Structure

```
pkg/timedactor/
├── actor.go       # Core TimedActor[T] implementation (Add, Hold, Clear, List, Subscribe)
├── actor_test.go  # Unit tests with full mock coverage
├── config.go      # Config struct, Deps, and defaults
├── module.go      # Uber fx module (generic function) and lifecycle hooks
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
| `marshalPayload/unmarshalPayload` | Round-trip, legacy timestamp, invalid data |
| `isRevisionMismatch` | API error, string match, unrelated |
| `New` | Default config, custom config, bucket error |
| `Add` | Success with metadata, put error, overwrite |
| `Clear` | Success, key not found, delete error |
| `List` | Empty, all keys with metadata, specific keys |
| `tryFireCallback` | Wins race, loses race, key already deleted |
| `Subscribe` | Callback on expiry, revision mismatch, delete marker |
| `Subscribe routing` | Routing by metadata, metadata overwrite, wildcard match |
| `Stop` | Cancels timers, stops watchers, waits for goroutines |
| `Hold` | CAS update (preserves metadata), invalidates timers, seamless transition, restore on failure (restores metadata), get error, CAS conflict, key deleted during hold, full flow with Subscribe |

## Edge Cases

### What happens if the process crashes during Hold?

The key in KV has its expiry pushed forward by `MaxHoldDuration` (default: 30 seconds). After this time, the timer fires normally with the original metadata. The timeout is delayed but **not lost**.

### Can two replicas Hold the same key?

No. `Hold()` uses a CAS (Compare-And-Swap) update. The second replica's `kv.Update(key, value, expectedRev)` will fail because the first Hold already bumped the revision. The second Hold returns an error.

### What if Add() is called without Hold?

It works normally — `kv.Put` updates the value (including metadata) and bumps the revision. All replicas' watchers pick up the new value and reschedule their timers.

### What if Hold() is called on a non-existent key?

`Hold()` returns an error (`jetstream.ErrKeyNotFound`). You must call `Add()` first.

### Does Hold preserve the metadata?

Yes. When `Hold()` updates the KV entry, it only modifies the expiration timestamp. The metadata is copied unchanged into the new payload. If the transaction fails and the original expiry is restored, the original metadata is also restored.
