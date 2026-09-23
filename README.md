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
  - [pkg/replywait](#pkgreplywait)
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
    ├── replywait/                         # Request/reply over JetStream via per-pod ephemeral consumer
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

Creates a durable JetStream consumer and starts a subscription loop. The handler is authoritative: the message is **Ack'd on success and Nak'd on error** (redelivery). Panics are recovered per-message and Nak'd.

While the handler runs, the consumer sends `msg.InProgress()` on a 5s ticker to keep extending the ack deadline (`AckWait`, default 30s), so a slow money-path handler (tx + gRPC) is not redelivered to a second goroutine before it finishes.

**Middleware execution order:**

```
BeforeMiddleware → Handler (with InProgress heartbeat) → ACK → AfterMiddleware
```

Handlers **MUST be idempotent**: producer-side `Nats-Msg-Id` dedup bounds replay to the stream `Duplicates` window, but redelivery on Nak/restart is still possible. For consumers that are not already idempotent downstream, use the opt-in [consume-side idempotency helper](#consume-side-idempotency).

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

#### Consume-side idempotency

`WithIdempotency` is an **opt-in** handler wrapper that dedups at consume time within
the consumer's own transaction. It is for new/other consumers that are not already
idempotent via downstream idem-keys — existing money consumers (ledger) already are,
so their behavior is unchanged.

For each message it opens one transaction (via `txmanager`) and:

- if the dedup key already exists in `processed_messages` → **Ack without running** the
  business handler;
- otherwise runs the handler, then inserts the dedup row — both in the **same
  transaction**, so the side effects and the dedup marker commit atomically (a handler
  error rolls back the marker and triggers Nak/redelivery).

The dedup key defaults to the `Nats-Msg-Id` header, falling back to a deterministic
`sha256(subject + payload)` when the header is absent. The wrapped handler must use
`txmanager.GetConnection(ctx)` for its own writes so they share the transaction.

```go
import jscons "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"

// Once at startup (mirrors outbox.AutoMigrate):
if err := jscons.AutoMigrateProcessed(db); err != nil {
    log.Fatal(err)
}

handler := jscons.WithIdempotency(
    txManager,
    nil, // nil → DefaultIdempotencyKey (Nats-Msg-Id, fallback hash)
    func(ctx context.Context, msg jetstream.Msg) error {
        return processOrder(ctx, msg) // writes via txManager.GetConnection(ctx)
    },
)

// pass `handler` as Opts.HandlerFunc
```

`processed_messages` columns: `message_id` (PK), `subject`, `processed_at`.

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
| Duplicates | `15 minutes` |

> **Dedup invariant:** the `Duplicates` window must be `>= 2 ×` the outbox relay
> `StallThreshold` (default 5m). The relay re-publishes a stalled IN_FLIGHT row with
> `Nats-Msg-Id = row.ID`; with 2× headroom the re-publish always lands inside a live
> dedup window, so the duplicate is suppressed. Keep `Duplicates < MaxAge`.

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
| `StallThreshold` | `time.Duration` | `5m` | Age after which an IN_FLIGHT row is re-picked |

**Stall recovery:** Events stuck in `IN_FLIGHT` past `StallThreshold` (default 5 minutes) are re-picked and re-published on the next relay cycle (handles a crash between NATS ack and the DB state update). The re-publish carries `Nats-Msg-Id = row.ID`, so the stream `Duplicates` window suppresses the duplicate — which is why `Duplicates` must be `>= 2 × StallThreshold` (see Stream defaults).

#### Outbox v2 (opt-in)

Everything below is additive on top of the v0.1.9 schema and API. A service that does nothing keeps v0.1.9 behavior exactly: same columns, same `Save`, same `DefaultConfig()`. v2 features turn on individually.

**Opt-in.** Use `outbox.RecommendedConfig()` instead of `outbox.DefaultConfig()` as the `Config` passed into `RelayDeps`. It sets `CleanupInterval`, `DeleteOlderThan`, `RetainParked`, `MaxAttempts`, `RetryBackoffBase/Max`, `StatsInterval` to values fit for production. `DefaultConfig()` itself is unchanged (ОК-0): it still zero-values `RetainParked`, `MaxAttempts`, `RetryBackoffBase/Max` and `StatsInterval`, which disables the corresponding v2 behavior, but `DeleteOlderThan` is `1h` and a zero `CleanupInterval` falls back to 5 minutes at relay start — DONE-row cleanup runs either way.

**Schema migration.** `outbox.AutoMigrate(db)` now also adds the v2 columns to `outbox_events` (`attempts`, `last_error`, `next_attempt_at`, `headers`) via `ADD COLUMN IF NOT EXISTS`, plus an index:

```sql
alter table outbox_events add column if not exists attempts integer not null default 0;
alter table outbox_events add column if not exists last_error text null;
alter table outbox_events add column if not exists next_attempt_at timestamptz null;
alter table outbox_events add column if not exists headers jsonb null;
create index if not exists idx_outbox_events_state_updated_at on outbox_events (state, updated_at);
```

A service that runs its own SQL migrations instead of `AutoMigrate` (no `outbox_retries` table, for example) applies the same five statements by hand. The relay detects the schema at startup (`information_schema.columns` for the four v2 columns, `to_regclass` for `outbox_retries`) and runs in legacy v0.1.9 mode when they are absent — no crash, just no v2 behavior (attempts/backoff/parking/headers/signing) until the columns exist. That silent degradation applies to the relay only: `SaveMessage` itself returns `ErrSchemaOutdated` for any message that ends up carrying headers on a legacy schema. `KVDelete` always sets the `KV-Operation` header, so `SaveMessage(KVDelete(...))` fails until the migration runs, and so does any `SaveMessage` call combined with `WithHeaders`. Plain `Save`, and `SaveMessage`/`KVPut` calls that carry no headers, keep working on the legacy schema.

**Relay pause (D-17).** Set env `BROKERFX_OUTBOX_RELAY_PAUSED=true` (or `1`) before the pod starts, or `Config.Paused = true`, to keep the relay from publishing while it keeps accepting `SaveMessage`/`Save` writes — rows pile up as `PENDING` instead of draining. Useful for draining a topology change without losing events. Same effect at runtime via `(*OutboxRelay).Pause()` / `.Resume()`; `.Paused()` reports the current state. Env is checked once at relay construction and OR'd with `Config.Paused`.

**`SaveMessage` / `KVPut` / `KVDelete`.** `SaveMessage` replaces `Save` when a message needs headers or a JetStream KV projection, in the same business transaction:

```go
err := s.outboxStore.SaveMessage(ctx, outbox.KVPut("merchant-config", merchantID, payload),
    outbox.WithHeaders(map[string]string{"X-Trace-Id": traceID}))

err := s.outboxStore.SaveMessage(ctx, outbox.KVDelete("merchant-config", merchantID))
```

`KVPut`/`KVDelete` build a `Message{Subject: "$KV.<bucket>.<key>", ...}`; the relay publishes to that subject like any other outbox row, and JetStream's KV projection turns the publish into a KV write. `KVDelete` sets the reserved `KV-Operation: DEL` header so the projection deletes the key instead of writing it. Within a single relay batch, only the latest queued row per `$KV.<bucket>.<key>` subject is actually published — the relay indexes rows by subject before publishing and marks any earlier row for that subject as done without sending it, so a put followed by a delete (or several puts) in the same batch collapses to one publish. `WithHeaders` merges caller headers onto the message; setting `Nats-Msg-Id` explicitly through it returns `ErrReservedHeader` (the relay always sets it itself, from the row id). Bucket must match `^[A-Za-z0-9_-]+$`, key must match `^[-/_=.A-Za-z0-9]+$` and not start or end with `.`; violations return `ErrInvalidKVBucket` / `ErrInvalidKVKey` before any DB write.

**`kv.Ensure`.** `pkg/nats/jetstream/kv.Ensure(ctx, js, kv.Opts{Bucket, Replicas, History, Storage, MaxBytes, Duplicates})` idempotently creates or reconciles the KV bucket's backing stream (`KV_<bucket>`). `Replicas < 1` returns `kv.ErrReplicasRequired` before any stream call — callers must pass an explicit replica count, there is no default. `Duplicates` defaults to `kv.DefaultDuplicates` (15m) when zero, matching the outbox relay's own dedup window — a KV bucket written through the outbox should use the same or a longer window. Repeated calls with the same `Opts` only issue an `UpdateStream` when the live stream config actually differs; `Replicas` and `MaxBytes` come from the caller's config, never hardcoded (stage: 1, prod: 3 after ОК-1).

**Signer.** Pass a `Signer` (`Sign(subject string, headers map[string]string, payload []byte) error`) as the optional `Signer` field on `RelayDeps` to have the relay sign every published message before publish, headers included. `pkg/nats/signature.NewEd25519Signer(apiKeyID, privateKey)` is the Ed25519 implementation already used by `pkg/nats/core`'s middleware (same `Sign`/`Verify`, same headers `X-API-Key-ID`/`X-Timestamp`/`X-Signature`). Signing is skipped for `$KV.*` subjects, since a KV publish carries no request to authenticate on the receiving side.

**Observer.** Pass an `Observer` as the optional `Observer` field on `RelayDeps` to get relay lifecycle hooks: `OnPublished(n)`, `OnFailed(n)`, `OnParked(eventID, subject, lastError)`, `OnStats(stats Stats)` (`Stats` — `Pending`, `InFlight`, `Done`, `Parked`, `OldestPendingAge`). `OnStats` fires every `Config.StatsInterval` when set. All four are best-effort hooks for metrics/alerting; a nil `Observer` (the default) skips them entirely.

**Attempts, backoff, PARKED.** With `Config.MaxAttempts > 0`, a failed publish increments `attempts` and schedules `next_attempt_at` using exponential backoff between `RetryBackoffBase` and `RetryBackoffMax`. Once `attempts >= MaxAttempts`, the row moves to `StateParked` instead of being retried again, and `Observer.OnParked` fires if set. `Config.RetainParked` controls how long parked rows survive before cleanup deletes them (`0` keeps them forever). Transport-level failures (no NATS servers reachable, connection closed/reconnecting) are the exception: they do not increment `attempts` or set a backoff, so a NATS outage alone never parks a row — the row stays `PENDING` and is retried as soon as the connection recovers.

**`RequeueParked` / `Stats`.** `(*OutboxStore).RequeueParked(ctx, ids []string) (int64, error)` moves parked rows back to `PENDING` with `attempts` reset, for manual recovery. Passing a non-empty `ids` limits it to those rows; passing a nil or empty slice requeues **every** currently parked row, which matters for anything exposing this as an admin endpoint. `(*OutboxStore).Stats(ctx) (Stats, error)` returns the same counts as the `OnStats` hook, for on-demand polling (health checks, admin endpoints).

**ОК-0.** `stream-opts-builder.NewDefault` (MaxAge 12h, Replicas 1, Duplicates 15m, WorkQueue) and `outbox.DefaultConfig()` are unchanged by v2 and must stay that way — v2 config lives entirely in the new, separately-opted-in fields and in `RecommendedConfig()`.

---

### pkg/replywait

Request/reply over JetStream for a request that already has its own delivery contract (published via the Outbox, consumed by a downstream service) but needs a synchronous answer back to the caller. Each pod runs one ephemeral `OrderedConsumer` on its own reply subject and resolves waiters by `correlation_id`; there is no durable consumer per pod and no Core NATS request/reply (GW-02, D-23).

#### Subjects and stream ownership

`Config.ReplySubject` is the pod's own subject, at least four dot-separated tokens with no `*`, `>`, empty token, or whitespace — e.g. `balance.result.balanceapi.balanceapi-7f9c-x2`, where the last token is the pod instance (see `POD_NAME` below). `Config.Stream` is the reply stream that carries it, named per domain (e.g. `BALANCE_REPLY`).

The reply stream is **owned by the domain that publishes the replies**, not by `replywait`. That owner calls `EnsureReplyStream(ctx, js, ReplyStreamOpts{Name, Subjects, Replicas, MaxAge})` — `LimitsPolicy`/`FileStorage`, `Replicas` and `MaxAge` from that service's own config (stage: 1 node; prod: 3, only after ОК-1) — to create or reconcile the stream. `replywait` itself never creates or updates a stream it doesn't own (ОК-0); it only opens an `OrderedConsumer` filtered to its own `ReplySubject` once the stream already exists — `listener.start` returns an error naming the stream when it doesn't.

#### Usage

```go
import "github.com/dehwyy/brokerfx/pkg/replywait"

fx.Options(
    replywait.Module,
    fx.Provide(func() replywait.Config {
        return replywait.Config{
            Stream:            "BALANCE_REPLY",
            ReplySubject:      fmt.Sprintf("balance.result.balanceapi.%s", instance),
            CorrelationFunc:   extractCorrelationID,
            DefaultTimeout:    5 * time.Second,
            InactiveThreshold: 30 * time.Second,
            DrainTimeout:      10 * time.Second,
        }
    }),
)

type Opts struct {
    fx.In
    RW    *replywait.ReplyWaiter
    Store *outbox.OutboxStore
}

func (s *Service) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
    msg, err := s.rw.Request(ctx, req.CorrelationID, func(ctx context.Context) error {
        return s.txManager.Do(ctx, func(ctx context.Context) error {
            if err := s.repo.Save(ctx, order); err != nil {
                return err
            }
            return s.store.SaveMessage(ctx, outboxMessage)
        })
    })
    ...
}
```

The `publish` callback passed to `Request` must not return until the `SaveMessage` transaction has **committed**. `Request` calls `Waker.WakeupRelay()` immediately after `publish` returns, before waiting for a reply — if `publish` only queues the outbox row inside a still-open outer transaction, the wake fires before the row is visible to the relay and the request falls back to the relay's own `TickInterval` instead of an immediate wake. `*outbox.OutboxStore` satisfies `Waker` directly; `Waker` is optional on `ModuleDeps` and a nil `Waker` just skips the wake.

#### Errors and `PENDING`

`Request` returns `ErrTimeout` (deadline passed, `DefaultTimeout` or the caller's shorter context deadline), `ErrDrained` (`Drain` completed while this call was in flight), or `ErrDraining` (`BeginDrain` already called, no `publish` attempted). None of these are terminal failures for the caller's request: until NP-22 lands, the gateway maps all three to a `PENDING` response rather than an error (D-32) — the underlying outbox write already committed, so the eventual reply (or a client retry) still resolves it.

#### Draining and `POD_NAME`

`Module`'s `OnStop` hook calls `BeginDrain()` then `Drain(ctx)`: new `Request`/`Register` calls fail fast with `ErrDraining`, in-flight waiters keep waiting for `DrainTimeout` (or the passed `ctx`, whichever is shorter), then any still-inflight waiter fails with `ErrDrained` and the pod's listener stops. For this to actually catch in-flight replies instead of being cut off by SIGKILL, the pod's `terminationGracePeriodSeconds` must be **greater than** `Config.DrainTimeout`.

`InstanceFromEnv()` reads `POD_NAME` and rejects an empty value or one containing `.` (`ErrInvalidInstance`) — the pod manifest must set it from the downward API, not a static value:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

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
