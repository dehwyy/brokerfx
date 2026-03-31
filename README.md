# brokerfx

NATS JetStream abstraction library for Go with Uber fx integration. Provides typed consumers, producers, transactional Outbox pattern, and a distributed timer scheduler with exactly-once execution guarantees.

## Key Features

- **Typed consumers & producers** — generic message handling with automatic msgpack encoding
- **Outbox pattern** — transactional guaranteed delivery to NATS (write to DB + publish in one tx)
- **TimedActor** — distributed timer scheduler backed by NATS KV with exactly-once callback execution across N replicas
- **Distributed Hold** — suppress timeouts during long transactions without losing them
- **Middleware chain** — composable before/after handler middleware (decode, trace, etc.)
- **Fluent builders** — for connections, streams, and consumers
- **Uber fx native** — every component ships as an `fx.Module` with lifecycle hooks

## Table of Contents

- [Tech Stack](#tech-stack)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Packages](#packages)
  - [pkg/crypto/v1](#pkgcryptov1)
  - [pkg/nats/conn](#pkgnatsconn)
  - [pkg/nats/jetstream](#pkgnatsjetstream)
    - [Producer](#producer)
    - [Consumer](#consumer)
    - [Stream](#stream)
  - [pkg/outbox](#pkgoutbox)
  - [pkg/timedactor](#pkgtimedactor)
- [Full FX Wiring Example](#full-fx-wiring-example)
- [Configuration Reference](#configuration-reference)
- [Testing](#testing)
- [CI/CD](#cicd)
- [License](#license)

---

## Tech Stack

| Component | Library |
|---|---|
| **Messaging** | `nats.io/nats.go v1.48.0` |
| **Serialization** | `vmihailenco/msgpack/v5` |
| **DI Framework** | `uber/fx v1.24.0` |
| **ORM** | `gorm.io/gorm v1.31.1` |
| **Transactions** | `dehwyy/txmanagerfx v0.0.3` |
| **Logging** | `rs/zerolog v1.34.0` |
| **Utilities** | `samber/lo v1.53.0` |
| **Go Version** | `1.25+` (generics required) |

---

## Installation

```bash
go get github.com/dehwyy/brokerfx
```

In a monorepo with `go.work`, local changes are visible immediately without publishing:

```go
// go.work
use (
    ./brokerfx
    ./payin-processing
    ./widgetapi
)
```

---

## Project Structure

```
brokerfx/
└── pkg/
    ├── crypto/v1/                         # msgpack encode/decode helpers
    ├── nats/
    │   ├── conn/                          # NATS connection factory
    │   │   └── builder/                   # Fluent options builder
    │   └── jetstream/
    │       ├── conn.go                    # JetStream instance factory
    │       ├── producer/                  # Async JetStream publisher
    │       ├── stream/                    # Stream create-or-update wrapper
    │       │   └── stream-opts-builder/   # Fluent stream config builder
    │       └── consumer/                  # Typed consumer with middleware
    │           ├── consumer-opts-builder/ # Fluent consumer config builder
    │           └── middleware/            # Before/after handler middleware
    ├── outbox/                            # Transactional Outbox pattern
    └── timedactor/                        # Distributed timer scheduler
```

---

## Packages

### pkg/crypto/v1

Thin wrappers around `msgpack.Marshal` / `msgpack.Unmarshal`. Used internally by the producer, consumer middleware, and outbox relay for all payload serialization.

```go
import cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"

// Encode
data, err := cryptov1.Encode(myStruct)

// Decode (generic)
value, err := cryptov1.Decode[MyStruct](data)
```

---

### pkg/nats/conn

NATS connection factory with Uber fx lifecycle integration. Validates required configuration, sets up NKey authentication, TLS, and reconnect behavior.

#### Opts

| Field | Type | Required | Description |
|---|---|---|---|
| `Servers` | `[]string` | ✓ | NATS server addresses |
| `SeedKey` | `string` | ✓ | NKey seed for authentication |
| `TLSCertFile` | `string` | | Path to client TLS certificate |
| `TLSKeyFile` | `string` | | Path to client TLS key |
| `TLSCAFile` | `string` | | Path to CA certificate |
| `ConnName` | `string` | | Display name in NATS monitoring |
| `MaxReconnects` | `int` | | Max reconnect attempts (-1 = unlimited) |
| `ReconnectWait` | `time.Duration` | | Wait between reconnects |

#### Usage

```go
import (
    natsconn "github.com/dehwyy/brokerfx/pkg/nats/conn"
)

fx.Provide(
    natsconn.New(natsconn.Opts{
        Servers: []string{"nats://localhost:4222"},
        SeedKey: os.Getenv("NATS_SEED_KEY"),
        ConnName: "payin-processing",
    }),
)
```

**Default connection settings:** PingInterval=20s, MaxPingsOutstanding=3.

---

### pkg/nats/jetstream

#### Producer

Publishes events asynchronously. Messages are msgpack-encoded. ACK is awaited in a goroutine; failures are logged but not propagated (fire-and-forget).

**Event interface:**

```go
type Event interface {
    Subject() string
    Data() any
}
```

**Usage:**

```go
import (
    jsprod "github.com/dehwyy/brokerfx/pkg/nats/jetstream/producer"
)

// Implement Event
type OrderCreatedEvent struct {
    orderID string
    payload OrderPayload
}
func (e *OrderCreatedEvent) Subject() string { return "orders.created." + e.orderID }
func (e *OrderCreatedEvent) Data() any       { return e.payload }

// In an FX service:
type Opts struct {
    fx.In
    Producer *jsprod.Producer
}

producer.Produce(&OrderCreatedEvent{orderID: "123", payload: p})
```

#### Consumer

Creates a durable JetStream consumer and starts a subscription loop. Messages are ack'd **before** the handler runs (safe for exactly-once processing). Panics are recovered per-message.

**Middleware execution order:**

```
BeforeMiddleware → ACK → Handler → AfterMiddleware
```

**Usage:**

```go
import (
    jscons "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
    consmw "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
    consbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
)

type MyMessage struct {
    OrderID string `msgpack:"order_id"`
}

// Typed handler
handler := jscons.NewHandlerFunc(func(ctx context.Context, msg jscons.Message[MyMessage]) error {
    decoded, err := msg.Decode()
    if err != nil {
        return err
    }
    return processOrder(ctx, decoded)
})

// Consumer options
opts := consbuilder.NewDefault().
    WithName("payin-order-consumer", "payin-order-consumer"). // name, durable
    WithFilterSubject("orders.>").
    WithAckWait(30 * time.Second).
    WithMaxDeliver(5).
    Build()

// Register via FX
fx.Provide(
    fx.Annotate(
        func(js jetstream.JetStream, stream *jsstream.Stream) *jscons.Consumer {
            return jscons.New(jscons.Opts{
                JetStream:            js,
                ConsumerOptsBuilder:  opts,
                Stream:               stream,
                HandlerFunc:          handler,
                BeforeHandlerMiddleware: []consmw.Middleware{
                    consmw.NewDecodeMiddleware[MyMessage]("my-message-key"),
                },
            })
        },
    ),
)
```

**Message[T] — type-safe wrapper:**

```go
type Message[T any] struct {
    jetstream.Msg
}

func (m Message[T]) Decode() (T, error) // deserializes msgpack payload
```

**Drainer — graceful shutdown:**

```go
drainer := jscons.NewDrainer()
drainer.Append(consumer1.Context(), consumer2.Context())

// On shutdown:
drainer.Drain() // waits for all consumers to finish in-flight messages
```

#### Stream

Creates or updates a JetStream stream at startup. Wraps `CreateOrUpdateStream` with a fluent builder.

```go
import (
    jsstream "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
    streambuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
)

streamOpts := streambuilder.NewDefault().
    WithName("paylonium-orders").
    WithSubjects("orders.>", "payments.>").
    WithMaxBytes(2 * 1024 * 1024 * 1024). // 2 GB
    WithMaxAge(12 * time.Hour).
    WithReplicas(3).
    Build()

fx.Provide(
    func(js jetstream.JetStream) *jsstream.Stream {
        return jsstream.New(jsstream.Opts{
            JetStream:          js,
            StreamOptsBuilder:  streamOpts,
        })
    },
)
```

**Stream defaults:**

| Setting | Default |
|---|---|
| Storage | `FileStorage` |
| MaxBytes | `2 GB` |
| MaxAge | `12 hours` |
| Retention | `WorkQueuePolicy` |
| MaxMsgsPerSubject | `1,000` |
| Compression | `S2Compression` |
| Replicas | `1` |

---

### pkg/outbox

Transactional guaranteed delivery to NATS. Stores events in a PostgreSQL table within the same business transaction, then a background relay publishes them to NATS asynchronously.

#### Database schema

```sql
-- outbox_events
CREATE TABLE outbox_events (
    id         UUID PRIMARY KEY,
    topic      VARCHAR(255) NOT NULL,
    payload    BYTEA NOT NULL,
    state      VARCHAR(50) NOT NULL DEFAULT 'PENDING',  -- PENDING | IN_FLIGHT | DONE
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
CREATE INDEX ON outbox_events (state);

-- outbox_retries
CREATE TABLE outbox_retries (
    id         UUID PRIMARY KEY,
    event_id   UUID REFERENCES outbox_events(id),
    error      TEXT,
    created_at TIMESTAMPTZ
);
CREATE INDEX ON outbox_retries (event_id);
```

#### Event lifecycle

```
Business Transaction
┌─────────────────────────────────────┐
│  1. DB write (order created)        │
│  2. outbox_events INSERT (PENDING)  │
│  ← same GORM tx via txmanager       │
└─────────────────────────────────────┘
         │
         ▼ (WakeupRelay or ticker)
Relay goroutine
┌─────────────────────────────────────────────────────┐
│  3. SELECT FOR UPDATE SKIP LOCKED (batch of 100)    │
│  4. UPDATE state → IN_FLIGHT                         │
│  5. PublishAsync to NATS (parallel per event)        │
│  6a. ACK received → mark DONE or DELETE              │
│  6b. Error → revert to PENDING, insert into retries  │
└─────────────────────────────────────────────────────┘
```

#### Outbox modes

| Mode | Behavior |
|---|---|
| `ModeDeleteAfterSend` | Delete row immediately after NATS ACK |
| `ModeUpdateAfterSend` (default) | Mark as DONE, periodic cleanup of records older than `DeleteOlderThan` |

#### Usage

```go
import "github.com/dehwyy/brokerfx/pkg/outbox"

// 1. Register module in FX app
fx.Options(
    outbox.Module,
)

// 2. Inject OutboxStore into your service
type Opts struct {
    fx.In
    Store *outbox.OutboxStore
}

// 3. Save event inside a business transaction (same GORM tx)
func (s *Service) CreateOrder(ctx context.Context, req *CreateOrderRequest) error {
    return s.txManager.Do(ctx, func(ctx context.Context) error {
        // Business logic
        if err := s.repo.CreateOrder(ctx, order); err != nil {
            return err
        }

        // Write to outbox in the same transaction
        if err := s.outboxStore.Save(ctx, &OrderCreatedEvent{order}); err != nil {
            return err
        }

        // Wake relay immediately (non-blocking)
        s.outboxStore.WakeupRelay()

        return nil
    })
}
```

#### Outbox configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `Mode` | `OutboxMode` | `UpdateAfterSend` | Deletion strategy |
| `BatchSize` | `int` | `100` | Max events per relay cycle |
| `TickInterval` | `time.Duration` | `2s` | Relay polling interval |
| `DeleteOlderThan` | `time.Duration` | `1h` | Cleanup threshold (UpdateAfterSend only) |

**Stall recovery:** Events stuck in `IN_FLIGHT` for more than 5 minutes are automatically reverted to `PENDING` on the next relay cycle (handles relay crashes mid-batch).

---

### pkg/timedactor

Distributed timer scheduler backed by NATS JetStream KV Store. Fires callbacks with **exactly-once execution** across all running replicas (Kubernetes pods).

#### How it works

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

Each timer expiry triggers a **revision-guarded CAS delete** — only the first replica to execute it wins. All others silently skip. No external coordination required.

#### API

```go
// Add or replace a timer
err := actor.Add(ctx, "order.123", MyMeta{Type: "VISIT_RESOURCE"}, 5*time.Minute)

// Suppress timeout during a long transaction
holdCtx, holdCancel := context.WithCancel(ctx)
err = actor.Hold(holdCtx, "order.123")
defer holdCancel() // release hold on success or failure

// Cancel a timer
err = actor.Clear(ctx, "order.123")

// List active timers
entries, err := actor.List(ctx)          // all keys
entries, err := actor.List(ctx, "order.123") // specific keys

// Subscribe with metadata-based routing
actor.Subscribe(ctx, "order.*",
    func(m MyMeta) bool { return m.Type == "VISIT_RESOURCE" },
    func(ctx context.Context, key string, meta MyMeta) {
        // fires exactly once, on exactly one replica
    },
)
```

#### Distributed Hold

Temporarily pushes the expiry forward via CAS, protecting an in-progress transaction. On hold release:

- If a new `Add()` was called during the hold → seamless transition, old expiry discarded
- If transaction failed and no `Add()` was called → original expiry and metadata are restored

```go
holdCtx, holdCancel := context.WithCancel(ctx)

if err := actor.Hold(holdCtx, "order.123"); err != nil {
    return err
}

if err := doMultiStepTransaction(ctx); err != nil {
    holdCancel() // original timeout restored automatically
    return err
}

// Set new timeout type before releasing hold
actor.Add(ctx, "order.123", MyMeta{Type: "SELECT_REQUISITE"}, 10*time.Minute)
holdCancel() // old timer invalidated, new one takes over
```

| Hold scenario | Result |
|---|---|
| `Add()` called before `holdCancel()` | Seamless transition, no restore |
| Transaction failed, `holdCancel()` only | Original expiry + metadata restored |
| Process crashes during Hold | Timer fires after `MaxHoldDuration` (default 30s) |
| Two replicas call `Hold()` simultaneously | Second call fails (CAS conflict) |

#### FX integration

```go
import "github.com/dehwyy/brokerfx/pkg/timedactor"

type TimeoutMeta struct {
    TimeoutName string `json:"timeout_name"`
}

fx.Options(
    timedactor.Module[TimeoutMeta](),

    // Optional: custom config
    fx.Provide(func() timedactor.Config {
        return timedactor.Config{
            BucketName:      "paylonium-timers",
            CheckInterval:   15 * time.Second,
            MaxHoldDuration: 30 * time.Second,
            BucketTTL:       48 * time.Hour,
        }
    }),

    fx.Invoke(func(actor *timedactor.TimedActor[TimeoutMeta], lc fx.Lifecycle) {
        lc.Append(fx.Hook{
            OnStart: func(ctx context.Context) error {
                actor.Subscribe(ctx, ">",
                    func(m TimeoutMeta) bool { return m.TimeoutName == "TIMEOUT_VISIT_RESOURCE" },
                    handleVisitResourceTimeout,
                )
                actor.Subscribe(ctx, ">",
                    func(m TimeoutMeta) bool { return m.TimeoutName == "TIMEOUT_CONFIRM_PAYMENT" },
                    handleConfirmPaymentTimeout,
                )
                return nil
            },
        })
    }),
)
```

#### TimedActor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `BucketName` | `string` | `"TimedActorBucket"` | NATS KV bucket name |
| `CheckInterval` | `time.Duration` | `15s` | Safety-net rescan interval (catches missed watch events) |
| `MaxHoldDuration` | `time.Duration` | `30s` | Hold timeout — timer fires after this if holder crashes |
| `BucketTTL` | `time.Duration` | `48h` | KV bucket TTL for automatic GC |

#### KV payload format

```json
{"e": 1743200000000000000, "m": {"timeout_name": "TIMEOUT_VISIT_RESOURCE"}}
```

| Field | Description |
|---|---|
| `e` | Expiration Unix nanoseconds |
| `m` | Arbitrary metadata `T` (JSON) |

Backward compatible: plain integer strings (legacy format) are parsed as Unix-nano timestamps with zero-value metadata.

---

## Full FX Wiring Example

```go
package main

import (
    natsconn    "github.com/dehwyy/brokerfx/pkg/nats/conn"
    natsjs      "github.com/dehwyy/brokerfx/pkg/nats/jetstream"
    jsstream    "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
    streambldr  "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
    jscons      "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
    consbldr    "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
    jsprod      "github.com/dehwyy/brokerfx/pkg/nats/jetstream/producer"
    "github.com/dehwyy/brokerfx/pkg/outbox"
    "github.com/dehwyy/brokerfx/pkg/timedactor"
    "go.uber.org/fx"
)

type TimerMeta struct {
    TimeoutName string `json:"timeout_name"`
}

func main() {
    fx.New(
        // 1. NATS connection
        fx.Provide(
            natsconn.New(natsconn.Opts{
                Servers: []string{"nats://localhost:4222"},
                SeedKey: os.Getenv("NATS_SEED_KEY"),
            }),
        ),

        // 2. JetStream
        fx.Provide(natsjs.New),

        // 3. Stream
        fx.Provide(func(js jetstream.JetStream) *jsstream.Stream {
            return jsstream.New(jsstream.Opts{
                JetStream: js,
                StreamOptsBuilder: streambldr.NewDefault().
                    WithName("orders").
                    WithSubjects("orders.>").
                    Build(),
            })
        }),

        // 4. Producer
        fx.Provide(jsprod.New),

        // 5. Consumer
        fx.Provide(func(js jetstream.JetStream, stream *jsstream.Stream) *jscons.Consumer {
            return jscons.New(jscons.Opts{
                JetStream: js,
                Stream:    stream,
                ConsumerOptsBuilder: consbldr.NewDefault().
                    WithName("order-processor", "order-processor").
                    WithFilterSubject("orders.>").
                    Build(),
                HandlerFunc: jscons.NewHandlerFunc(func(ctx context.Context, msg jscons.Message[OrderEvent]) error {
                    event, err := msg.Decode()
                    if err != nil {
                        return err
                    }
                    return handleOrderEvent(ctx, event)
                }),
            })
        }),

        // 6. Outbox
        outbox.Module,

        // 7. TimedActor
        timedactor.Module[TimerMeta](),
    ).Run()
}
```

---

## Configuration Reference

### NATS Connection

| Field | Type | Required | Default |
|---|---|---|---|
| `Servers` | `[]string` | ✓ | — |
| `SeedKey` | `string` | ✓ | — |
| `TLSCertFile` | `string` | | — |
| `TLSKeyFile` | `string` | | — |
| `TLSCAFile` | `string` | | — |
| `ConnName` | `string` | | — |
| `MaxReconnects` | `int` | | NATS default |
| `ReconnectWait` | `time.Duration` | | NATS default |

### Stream Defaults

| Setting | Default |
|---|---|
| Storage | `FileStorage` |
| MaxBytes | `2 GB` |
| MaxAge | `12 hours` |
| Retention | `WorkQueuePolicy` |
| MaxMsgsPerSubject | `1,000` |
| Compression | `S2Compression` |
| Replicas | `1` |

### Consumer Defaults

| Setting | Default |
|---|---|
| AckPolicy | `AckExplicitPolicy` |
| AckWait | `10 seconds` |
| DeliverPolicy | `DeliverAllPolicy` |
| Pull: MaxMessages | `50` |
| Pull: Heartbeat | `10 seconds` |

### Outbox Defaults

| Setting | Default |
|---|---|
| Mode | `UpdateAfterSend` |
| BatchSize | `100` |
| TickInterval | `2 seconds` |
| DeleteOlderThan | `1 hour` |
| Stall detection | `5 minutes` (IN_FLIGHT revert) |
| Cleanup ticker | `5 minutes` |
| Publish timeout | `30 seconds` per batch |

### TimedActor Defaults

| Setting | Default |
|---|---|
| BucketName | `"TimedActorBucket"` |
| CheckInterval | `15 seconds` |
| MaxHoldDuration | `30 seconds` |
| BucketTTL | `48 hours` |

---

## Testing

```bash
# All tests (no NATS server required — all mocked)
go test -v ./...

# Specific package
go test -v ./pkg/timedactor/...

# With race detector
go test -race ./...
```

### TimedActor test coverage

| Area | Scenarios |
|---|---|
| `marshalPayload / unmarshalPayload` | Round-trip, legacy timestamp format, invalid data |
| `isRevisionMismatch` | API error detection, string matching, unrelated errors |
| `New` | Default config, custom config, bucket creation error |
| `Add` | Success with metadata, put error, key overwrite |
| `Clear` | Success, key not found, delete error |
| `List` | Empty result, all keys with metadata, specific key subset |
| `tryFireCallback` | Win race, lose race (CAS mismatch), key already deleted |
| `Subscribe` | Callback on expiry, revision mismatch handling, delete marker |
| `Subscribe routing` | Per-metadata routing, metadata overwrite, wildcard match |
| `Stop` | Timer cancellation, watcher stopping, goroutine drain |
| `Hold` | CAS update, metadata preservation, seamless transition, restore on failure, CAS conflict, key deleted during hold |

---

## CI/CD

`.github/workflows/release.yml` runs on every push to `main`:

1. **Test** — `go mod tidy` + `go test -v ./...`
2. **Release** — auto-bump semver tag and create GitHub release with changelog

Every green `main` commit produces a new patch release automatically.

---

## License

MIT © 2026 dehwyy
