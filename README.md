# brokerfx

NATS JetStream abstraction library for Go with Uber fx integration. Provides typed consumers, producers, transactional Outbox pattern, and a distributed timer scheduler with exactly-once execution guarantees.

## Key Features

- **Typed consumers & producers** — generic message handling with automatic msgpack encoding
- **Outbox pattern** — transactional guaranteed delivery to NATS (write to DB + publish in one tx), with an opt-in batched relay
- **Batch consumer** — per-message verdicts over a fetched batch, one transaction per batch (`InTx`), retry ladder and DLQ hook
- **TimedActor** — distributed timer scheduler backed by NATS KV with exactly-once callback execution across N replicas
- **Distributed Hold** — suppress timeouts during long transactions without losing them
- **Postgres timers** — `pkg/pgtimer` polls a timer table the service owns, with a lease per claim and delete after the handler
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
    - [Batch consumer](#batch-consumer)
    - [Processed messages cleanup](#processed-messages-cleanup)
    - [Stream](#stream)
  - [pkg/outbox](#pkgoutbox)
  - [pkg/pgtimer](#pkgpgtimer)
  - [pkg/replay](#pkgreplay)
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
    │           ├── batch/                 # Batch pull consumer, InTx, CoalesceByKey
    │           ├── cleanup/               # Background retention of processed_messages
    │           ├── consumer-opts-builder/ # Fluent consumer config builder
    │           └── middleware/            # Before/after handler middleware
    ├── outbox/                            # Transactional Outbox pattern
    ├── pgtimer/                           # Lease-claim poller over a service timer table in Postgres
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

`Produce` does not report a lost publish, so it is not for money or status events. `ProduceSync(ctx, event, opts...)` publishes with `PublishMsg`, waits for the PubAck within `ctx` and returns the error (no stream for the subject, timeout, a server rejection). Inside a business transaction use the outbox instead of either.

`BROKERFX_JS_PUBLISH_ASYNC_MAX_PENDING` (v0.8.0, unset = the nats.go default of 4000) sets the async publish window of every JetStream context the package creates. A value that is not a positive integer is ignored with a warning.

#### Consumer

Creates a durable JetStream consumer and starts a subscription loop. The handler is authoritative: the message is **Ack'd on success and Nak'd on error** (redelivery). Panics are recovered per-message and Nak'd.

While the handler runs, the consumer sends `msg.InProgress()` on a 5s ticker to keep extending the ack deadline (`AckWait`, default 30s), so a slow money-path handler (tx + gRPC) is not redelivered to a second goroutine before it finishes.

**Middleware execution order:**

```
BeforeMiddleware → Handler (with InProgress heartbeat) → ACK → AfterMiddleware
```

Handlers **MUST be idempotent**: producer-side `Nats-Msg-Id` dedup bounds replay to the stream `Duplicates` window, but redelivery on Nak/restart is still possible. For consumers that are not already idempotent downstream, use the opt-in [consume-side idempotency helper](#consume-side-idempotency).

A handler may settle the message itself (Ack, Nak, NakWithDelay, Term or TermWithReason on the `jetstream.Msg` it received; InProgress does not settle the message, so the consumer still sends a normal Ack afterwards). If the message is already settled when the consumer acknowledges it, `ErrMsgAlreadyAckd` is treated as handled: no error log, no second settlement, and the heartbeat stops quietly.

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

`DrainContext(ctx)` does the same within `ctx` (pass the fx `OnStop` context) and returns the context error when it runs out. With `MaxInFlight > 0` both wait until all `K` slots are free, so a handler that took a slot while the subscription was closing is waited for too; a message still waiting for a slot at that point is not handled and gets `Nak` (immediate redelivery, another replica takes it). With `MaxInFlight = 0` they return once the subscription is drained, as before.

**Bounded dispatch and NAK backoff (v0.7.0).** Both are off by default; with the fields and env unset the consumer works as in v0.6.2.

| Field | Env | Default | Effect |
|---|---|---|---|
| `Opts.MaxInFlight` | `BROKERFX_CONSUMER_MAX_IN_FLIGHT` | `0` (one goroutine per message, pull batch 50) | `K > 0`: at most `K` handlers run at once, one message per pull request, the receive callback blocks while all slots are busy (backpressure) |
| `Opts.NakBackoff` | `BROKERFX_CONSUMER_NAK_BACKOFF` | off (plain `Nak`, immediate redelivery) | `NakWithDelay` by `NumDelivered` on handler error, before-middleware error and panic |

- An explicit `Opts` value wins over env. A negative `MaxInFlight` or a non-positive `NakBackoff` step makes `New` return `ErrInvalidOpts`. An unparsable env value is logged as a warning and the default is used.
- `BROKERFX_CONSUMER_NAK_BACKOFF`: `true` (or `1`, `on`, `yes`) uses `consumer.DefaultNakDelays` (`1s, 5s, 30s, 2m, 5m`, the same ladder as `batch.DefaultDelays`); a comma list such as `1s,2s,5s` sets the ladder; empty, `0`, `false`, `off` or `no` keeps it off. Delivery `n` gets step `n-1`, clamped to the last step; a message without metadata gets the first step.
- With `K > 0` the consumer pulls one message per request, so the client never holds a message the callback has not seen: at most `K` handlers run and one message waits for a slot. The waiting message sends `InProgress` from the moment it is received, and the heartbeat period is `min(5s, AckWait/3)` (at least 100ms), so a handler longer than `AckWait` does not get a duplicate of the queued message. A larger pull batch would leave up to `batch - 1` messages in the nats.go buffer without a heartbeat while all slots are busy, and the server would redeliver them after `AckWait`.
- The price is one pull round trip per message: one consumer on one pod receives at most about `1 / RTT` messages per second from the NATS server (embedded server on one host: about 7k/s with an empty handler, 13k/s at `K = 32` and 2ms handlers, the same as with batch `K`). `K = 0` keeps the batch of 50.
- `K` handlers that never return stop this consumer on the pod: the one waiting message keeps sending `InProgress` and does not move to another replica until the pod restarts (with `K = 0` only the stuck messages themselves were held). Handlers get a context without a deadline, so bound slow calls inside the handler. The waiting message is deliberately not NAKed on a timeout: every NAK spends a `MaxDeliver` attempt, and with one replica it would only walk the message towards the DLQ.
- **Without `MaxDeliver` the NAK is eternal**: a message that always fails is redelivered forever, every 5m on the default ladder. Set `WithMaxDeliver` (and a DLQ) where that matters.
- `MaxAckPending` stays the service's choice (`WithMaxAckPending`); keep it at or above `K`.

**Consumer replicas (v0.7.0).** `consbuilder.WithReplicas(n)` sets the consumer replica count for `consumer.New` and `batch.New`; `0` (default) inherits the stream. The server only accepts the stream's own count on a workqueue or interest stream and at most the stream's count on a limits stream; any other value is reset to `0` with a warning before create/update (`consbuilder.FitReplicas(cfg, stream.Config())`), so the consumer still starts instead of failing or silently keeping the old durable.

**Strict durable update (v0.9.0).** `consumer.Opts.StrictUpdate` and `batch.Opts.StrictUpdate` are off by default. Without them an update the server rejects (an immutable field such as `AckPolicy` or `MaxWaiting` changed: code 500, err_code 10012) is logged as a warning and the existing durable is used as is, so the pod runs with a config it did not ask for; the per-message consumer now logs that warning too, like batch.

- `StrictUpdate: true` makes `New` fail with `consumer.ErrUpdateRejected`; the error names the stream, the durable, the server code, err_code and description.
- Transient errors (timeout, no responders, reconnecting, disconnected, API code 503, a create race) are retried with backoff from 250ms to 2s for 30s; after that `New` returns `consumer.ErrBindTransient` wrapping the last error.
- After binding, the durable's filters (`FilterSubject` and `FilterSubjects` as one sorted set) are compared with the wanted set; a difference fails with `consumer.ErrFilterMismatch` naming both.
- An update keeps the durable's `DeliverPolicy`, `OptStartSeq` and `OptStartTime` on both paths, so a changed start option does not trigger a rejection.
- `BROKERFX_CONSUMER_STRICT_UPDATE=false` (or `0`, `off`, `no`) turns strict off for the process as a rollback switch; the env cannot turn it on. An unknown value logs a warning and keeps the code value.

**Several filters (v0.9.0).** `consbuilder.WithFilterSubjects(subjects...)` sets `FilterSubjects` and clears `FilterSubject`; `WithFilterSubject` sets one filter and clears `FilterSubjects`, so the last call wins. `consumer.New` and `batch.New` reject a config with both fields, an empty filter or a duplicate filter (`ErrInvalidOpts` of each package, wrapping `consumer.ErrInvalidFilters`); overlap checks stay with the server. The start log prints the bound filters as `filters`. A live durable moves from `FilterSubject` to `FilterSubjects` by a normal update and keeps its ack floor; `[order.updated, order.updated.>]` receives both the old and the new subject form.

**Pinned priority group and partition durables (v0.9.0).** `consbuilder.WithPinnedGroup(group, ttl)` sets `PriorityPolicy: pinned_client`, one priority group and `PinnedTTL` (0 = server default 2m). The server serves one client of the group at a time and moves the pin to another client after `PinnedTTL` without pull requests from the pinned one.

- `batch.New` fetches with `FetchPriorityGroup`; a lost pin (`ErrPinIDMismatch`) is an empty fetch with a debug log. It requires one group matching `[a-zA-Z0-9/_=-]{1,16}` and `PinnedTTL > AckWait + 5s idle poll + LingerMax`, otherwise `ErrInvalidOpts` wrapping `consumer.ErrInvalidPinned`. `consumer.New` consumes with `PullPriorityGroup`, needs `PinnedTTL` above its 30s pull expiry and logs a lost pin at debug instead of a consume warning.
- `consbuilder.PartitionConfigs(consbuilder.Partitions{Base, Count, Filters, Group, PinnedTTL})` returns one config per partition `i`: name and durable `<base>-p<i>`, `FilterSubjects = Filters(i)` (the service builds them from its proto partition helpers; the library knows no hash), `AckExplicit`, `DeliverAll`, pinned. Filters of different partitions must not overlap as NATS patterns, every partition needs a filter, otherwise `ErrInvalidPartitions`. `consbuilder.FromConfig(cfg)` turns a config into a builder for `consumer.New` and `batch.New`; nats.go code can create the configs directly.
- Pinned is not exclusive at a pin change: messages the old client took and did not ack come back to the new client after `AckWait`, so a handler that committed but did not ack is run twice. Keep the database deduplication (`processed_messages`, business keys, CAS) on partitioned consumers.
- The server (2.14) updates `PriorityPolicy` of a live durable in place, none → pinned and back, keeping the ack floor; a `StrictUpdate` consumer accepts that update.
- A workqueue stream accepts only `AckExplicit` durables: `AckAll` fails create with err_code 10098, so partition durables have no ack-last mode.

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

#### Batch consumer

`pkg/nats/jetstream/consumer/batch` is a pull consumer that hands the handler a batch of messages instead of one at a time. It exists for consumers where one transaction per message is the bottleneck (projections, ledgers, mirrors). The single-message `consumer.New` is unchanged.

The core owns the fetch loop, verdicts, retries, poison handling, heartbeats and drain. The handler owns the transaction and decides, per message, what happened.

```go
import (
    "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/batch"
    consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
)

func RunProjection(lc fx.Lifecycle, js jetstream.JetStream, st *stream.Stream, tx txmanager.TxManager, dlq *DLQ, shutdowner fx.Shutdowner) {
    var consumer *batch.Consumer
    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            start, err := batch.ResumeSequence(ctx, js, st.Name(), "orders-projection")
            if err != nil {
                return err
            }
            handler, err := batch.InTx(batch.InTxOpts{
                TxManager: tx,
                Apply:     applyOrders,
            })
            if err != nil {
                return err
            }
            consumer, err = batch.New(batch.Opts{
                JetStream: js,
                Stream:    st,
                ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().
                    WithName("orders-projection-batch", true).
                    WithFilterSubject("orders.>").
                    WithDeliverByStartSequence(start),
                Handler:  handler,
                OnPoison: dlq.Write,
                OnFatal: func(err error) {
                    if shutdownErr := shutdowner.Shutdown(fx.ExitCode(1)); shutdownErr != nil {
                        log.Error().Err(shutdownErr).Msg("shutdown after consumer loss failed")
                    }
                },
            })
            return err
        },
        OnStop: func(ctx context.Context) error {
            return consumer.Drain(ctx)
        },
    })
}
```

**Handler contract.** `type Handler func(ctx context.Context, msgs []jetstream.Msg) []batch.Result`.

- `results[i]` belongs to `msgs[i]`. A shorter slice means the rest get `Nak(ErrMissingResult)`; extra results are ignored. The zero `Result{}` is a Nak.
- Verdicts: `batch.Ack()`, `batch.Nak(err)`, `batch.NakDelay(d, err)`, `batch.Term(err)`.
- Verdicts are applied only after the handler returns, in message order. A handler that settled a message itself (`ErrMsgAlreadyAckd` afterwards) is fine.
- The handler context is `context.WithoutCancel` of the loop context: `Drain` never interrupts a batch that has started.
- A panicking handler does not lose the batch: every unsettled message is run through the handler again on its own (`[]Msg{m}`) under `recover`; a message that panics again gets `Nak(ErrHandlerPanic)` and follows the retry policy below. Inside `InTx` a panic of `Apply` or `ApplyResults` is recovered in the transaction, rolls it back and is an `ErrHandlerPanic` error of the attempt, so the batch is bisected as for any other error and `OrderKey` holds the tail of the panicking message.
- One heartbeat goroutine per batch sends `InProgress` to every unsettled message every `AckWait/2` (15 s if `AckWait` is 0), so a batch may take longer than `AckWait` without redelivery.
- Order by key is guaranteed only inside a batch, and between batches only with `Pipeline = 1`. With `Pipeline > 1` or retries, use a version guard in the UPSERT.

**Retry policy** (same as the `deadletter` packages in services):

- `Nak(err)`: `NakWithDelay(Delays[min(NumDelivered, len(Delays)) - 1])`. `Opts.Delays` defaults to `batch.DefaultDelays` = 1s, 5s, 30s, 2m, 5m; every element must be > 0.
- `NakDelay(d, err)`: `NakWithDelay(d)`.
- The library counts attempts: once `NumDelivered >= Opts.MaxAttempts`, a Nak becomes poison. `MaxAttempts` 0 means `len(Delays) + 1` (6 with the default ladder, about 8.5 minutes in total); `-1` means retry forever.
- An error marked with `batch.Poison(err)` (checked with `errors.Is(err, batch.ErrPoison)`) skips the ladder and goes to poison on the first delivery.
- `Term(err)` and poison call `Opts.OnPoison(ctx, msg, err)` (write the message to the service DLQ stream there) and then `Term`. If `OnPoison` returns an error or panics, the message gets `NakWithDelay(Delays[0])` instead of `Term`, so it is not lost: it comes back, is past `MaxAttempts` and goes to `OnPoison` again. Without `OnPoison` the message is terminated with an error log.
- Because of that, the server `MaxDeliver` must be `-1` (default, unlimited) or greater than `MaxAttempts`; otherwise `New` returns `ErrMaxDeliver`. The last server delivery must never be the last chance to write the DLQ.
- Draining (`nakAll` of fetched but not started messages) uses a plain `Nak` without delay.

**Fetch loop.** When idle, the loop waits for one message (`Fetch(1)`, up to 5 s, cancelled by `Drain`), then collects up to `Max - 1` more for `Linger`. Under load (the previous fetch was full) it fetches `Max` with `Linger` directly. `Pipeline` loops run in parallel over the same durable. Fetch timeouts are not errors. `ErrConsumerDeleted` / `ErrConsumerNotFound` naks what was fetched, calls `OnFatal` once and stops the loop. Any other fetch error is logged and retried after 1 s, so a NATS server restart does not stop the consumer.

**Options and env.** An explicit `Opts` field wins over env; env wins over the default.

| Field | Env | Default | Range |
|---|---|---|---|
| `Max` | `BROKERFX_BATCH_MAX` | `16` | 1..256 |
| `Linger` | `BROKERFX_BATCH_LINGER` | `5ms` | (0, 1s] |
| `Pipeline` | `BROKERFX_BATCH_PIPELINE` | `1` | 1..8 |
| `Delays` | — | `DefaultDelays` | each > 0 |
| `MaxAttempts` | — | `len(Delays)+1` | -1 or >= 1 |

A malformed env value returns `ErrInvalidEnv`, an out-of-range value `ErrInvalidOpts`. `Handler`, `JetStream`, `Stream` and `ConsumerOptsBuilder` are required.

**Consumer config checks** (done against the config the server actually holds):

- `AckPolicy` must be explicit (`ErrInvalidOpts`).
- `MaxAckPending`: when the builder leaves it at 0, `New` sets `Pipeline * Max` (`WithMaxAckPending`). An explicit value below `Pipeline * Max` returns `ErrMaxAckPending`; `-1` is allowed.
- `MaxDeliver`: see the retry policy above (`ErrMaxDeliver`).

**Existing durable.** If the durable already exists, `New` keeps it: it updates the mutable fields from the builder (AckWait, MaxAckPending, MaxDeliver and so on) but keeps the deliver policy and start sequence of the existing consumer, so a restart never moves the start position. If the server rejects the update, the existing durable is used as is.

**Moving a consumer to a new batch durable.** Start the new `*-batch` durable right after the old one: `start, err := batch.ResumeSequence(ctx, js, stream, oldDurable)` returns the old durable's ack floor stream sequence + 1, or 0 when the old durable does not exist. `WithDeliverByStartSequence(start)` turns 0 into `DeliverAll` and anything else into `DeliverByStartSequence`. Events between the ack floor and what the old durable had already applied are applied again, so the handler must be idempotent (version guard or `InTx` dedup).

**Drain.** `Drain(ctx)` stops fetching, naks fetched messages that have not started, waits for batches in progress and returns `nil`, or `ctx.Err()` if they do not finish in time. A second `Drain` is safe. `consumer.Drainer` does not accept a batch consumer; call `Drain` from `OnStop`.

**`InTx`** is a ready handler for "apply a batch in one transaction":

```go
handler, err := batch.InTx(batch.InTxOpts{
    TxManager: txManager,
    Apply: func(ctx context.Context, msgs []jetstream.Msg) error {
        return repo.UpsertMany(ctx, decode(msgs))
    },
})
```

- One `txmanager.Do` per batch. Inside, one `INSERT INTO processed_messages (message_id, subject, processed_at) VALUES ... ON CONFLICT (message_id) DO NOTHING RETURNING message_id`; `Apply` gets only the messages whose key was inserted, in batch order, and only the first message of a key that repeats inside the batch. After commit every message of the batch is acked.
- `Key` defaults to `consumer.DefaultIdempotencyKey` (`Nats-Msg-Id`, or `sha256(subject + payload)` without it: two messages with the same subject and payload and no msg id are duplicates). The table is `consumer.ProcessedMessage` (`consumer.AutoMigrateProcessed`).
- `NoDedup: true` skips the insert. Use it for projections that already have a version guard.
- A transient error (`Transient`, default `batch.DefaultTransient`) retries the same set while the batch budget lasts (`RetryBudget`, default 2, negative = no retries); after that every remaining message gets `Nak(err)`, so the core retry ladder applies by `NumDelivered` (1s/5s/30s/2m/5m with `DefaultDelays`) and the message reaches `OnPoison` only after `MaxAttempts`, about 8.5 minutes of a database outage. `TransientDelay > 0` replaces the ladder delay with `NakDelay(TransientDelay)` on every attempt; it still counts toward `MaxAttempts`, so a short value sends messages to `OnPoison` within seconds of an outage.
- Any other error splits the set in halves, each in its own transaction, down to single messages. A single message with a non-transient error gets `Nak(err)`: the core applies the retry ladder and sends it to `OnPoison` after `MaxAttempts`, or right away if `Apply` returned `batch.Poison(err)`. Healthy messages commit once.
- `Apply` writes only through `TxManager.GetConnection(ctx)` and does not call external services (it can run several times for the same messages during bisection).
- `DefaultTransient` is true for SQLSTATE `40001`, `40P01`, `55P03`, `57014`, `57P01`..`57P03` and classes `08` and `53` (any error with a `SQLState() string` method, pgx included), `driver.ErrBadConn`, `context.DeadlineExceeded` and a `net.Error` with `Timeout()`.

**`CoalesceByKey`** groups a batch by key and calls `apply` once with all groups (`[]batch.Group{Key, Msgs}`), groups in order of first appearance and messages inside a group in batch order. `apply` returns one `Result` per group, and that result applies to every message of the group; groups without a result get `Nak(ErrMissingResult)`. `GroupByKey(msgs, key)` is the same grouping for a handler that composes it with `InTx` by hand. A nil `key` or `apply` panics at construction.

**Batch options (v0.8.0).** All of them are off when unset; an explicit `Opts` field wins over the env, the env is the process default when the field is 0.

- `Opts.MinBatch` / `BROKERFX_BATCH_MIN` and `Opts.LingerMax` / `BROKERFX_BATCH_LINGER_MAX`: after the first `Linger` fetch the consumer keeps fetching with `min(Linger, time left)` until the batch has `MinBatch` messages, is full, or `LingerMax` has passed since the first linger fetch. A full batch still goes out at once. `MinBatch > Max` and `LingerMax >= AckWait/4` (30s when `AckWait` is unset) fail `New` with `ErrInvalidOpts`; `MinBatch` without `LingerMax` is switched off with a warning. At 300 msg/s with `Linger` 5ms a batch grows from about 1.5 to `MinBatch`, at the cost of up to `LingerMax` of latency.
- `Opts.Replicas` / `BROKERFX_BATCH_REPLICAS`: with `Replicas > 0` and no explicit `MaxAckPending` the durable gets `batch.MaxAckPendingFor(replicas, pipeline, max)` = `ceil(1.5 × Replicas × Pipeline × Max)` instead of `Pipeline × Max` of one pod. An explicit value below that fails `New` with `ErrMaxAckPending`; a service that takes the value from its own config clamps it with `MaxAckPendingFor` before the call. `MaxAckPending` belongs to the durable, so during a rolling update the last pod to start writes it; the durable left by an older pod (`Pipeline × Max`) is accepted. `New` logs `replicas`, `max_ack_pending` and `max_ack_pending_wanted`.
- `InTxOpts.ApplyResults` (instead of `Apply`) returns one `Result` per fresh message: `Ack()`, `batch.Skip()` (settles as ack, counted apart), or a retry or poison verdict. The transaction commits the acked messages; the `processed_messages` claim of every non-acked message is removed in the same transaction, so its redelivery is applied again. Exactly one of `Apply` and `ApplyResults` must be set.
- `InTxOpts.OrderKey` keeps order per key during bisection: the batch is split by key groups, a failed group of one key commits its longest good prefix, and the rest of that key gets `NakDelay` with `batch.ErrOrderBlocked`, which the consumer settles without counting toward `MaxAttempts`. A held message on delivery `MaxDeliver` of the durable goes to `OnPoison` with `ErrAttemptsExhausted` and `ErrOrderBlocked`: the server would not deliver it after that NAK. A hold still spends a delivery, so the tail's own failure after `MaxAttempts` deliveries goes to `OnPoison` at once; give `OrderKey` consumers `MaxAttempts` and `MaxDeliver` for the head's failures plus the tail's own, or `MaxAttempts = -1` with `MaxDeliver = -1`. An ack after a non-ack of the same key inside one `ApplyResults` call rolls the attempt back and blocks the tail. A non-acked message is NAKed with a delay not shorter than the delay of the previous non-acked message of its key in the batch (its `NakDelay` or its ladder step by its own `NumDelivered`), so the tail comes back together with or after the head and never alone before it. The order holds inside one batch; with `Pipeline > 1` or several pods two batches of one key can still run at the same time. `OrderKey` nil keeps the index bisection.
- `Opts.Observer` and `InTxOpts.Observer` (`batch.Observer`: `OnFetch`, `OnApply`, `OnBisect`, `OnSettle` with counts of ack, skip, retry, poison and blocked) are metric hooks; nil skips them.

#### Processed messages cleanup

`processed_messages` (used by `WithIdempotency` and `batch.InTx`) grows forever unless something deletes old keys. `pkg/nats/jetstream/consumer/cleanup` does it in the background:

```go
fx.Options(
    fx.Supply(cleanup.Opts{Duplicates: 2 * time.Minute}),
    cleanup.Module,
)
```

- `cleanup.Module` needs `*gorm.DB` in the graph (an absent DB is a start error) and takes optional `cleanup.Opts`. `Start` runs on `OnStart`, the first pass after `Interval`; `Stop` on `OnStop` waits for the current pass.
- `Retention` (env `BROKERFX_PROCESSED_RETENTION`, default `48h`): keys older than that are deleted. It must be at least `1h` and greater than `2 × Duplicates` (`Opts.Duplicates`, the stream duplicate window), otherwise `ErrInvalidOpts`. 48h is orders of magnitude above the stream duplicate window (minutes) and the worst retry ladder (1s+5s+30s+2m+5m×k, about 35 minutes at 10 attempts); the margin covers consumer downtime and a manual replay within a day. A key younger than the retention is never deleted, so a redelivery inside the window is still a duplicate.
- `Interval` (env `BROKERFX_PROCESSED_CLEANUP_INTERVAL`, default `1h`), `BatchLimit` (default 5000).
- One pass is `DELETE FROM processed_messages WHERE ctid IN (SELECT ctid FROM processed_messages WHERE processed_at < $1 LIMIT $2)` in a loop until a short batch, 100 ms between batches. `RunOnce(ctx)` runs one pass by hand. A failed pass goes to `OnError` and a warning log; the loop keeps running.
- `cleanup.EnsureIndex(ctx, db)` creates `idx_processed_messages_processed_at` with `CREATE INDEX CONCURRENTLY IF NOT EXISTS` (outside a transaction). `AutoMigrateProcessed` does not create it; call `EnsureIndex` once at startup or add the same statement to the service migrations.

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
    WithMaxBytes(512 * 1024 * 1024). // 512 MiB
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
| MaxBytes | `256 MiB` for `RoleEvent` (override: `BROKERFX_STREAM_MAX_BYTES`); `1 GiB` for command/result/reply roles (override: `BROKERFX_STREAM_MAX_BYTES_CRITICAL`); `256 MiB` for `RoleDLQ` (override: `BROKERFX_STREAM_MAX_BYTES_DLQ`) |
| Discard | `DiscardOld` for `RoleEvent`; `DiscardNew` for command/result/reply/DLQ roles |
| MaxAge | `12 hours` |
| Retention | `WorkQueuePolicy` |
| MaxMsgsPerSubject | `1,000` |
| Compression | `S2Compression` for event/result/DLQ roles; none for command/reply roles (v0.7.0) |
| Replicas | `1` |
| Duplicates | `15 minutes`, clamped to `MaxAge` when `MaxAge` is shorter (v0.7.0) |

**Stream role (NF-D-192).** `WithRole(streamoptsbuilder.RoleX)` selects the limit policy explicitly; there is no guessing from the stream name. `RoleEvent` (the default when `WithRole` is not called) gets `256 MiB` and `DiscardOld`. `RoleCommand`, `RoleResult` and `RoleReply` get `1 GiB` and `DiscardNew`, `RoleDLQ` gets `256 MiB` and `DiscardNew`: a full stream rejects the publish with an error and the outbox retries it, instead of silently dropping the oldest message. The last `WithRole` call wins. An unknown role makes `Err()` return `ErrUnknownRole`. Services that build a raw `jetstream.StreamConfig` take the same numbers from `streambuilder.LimitsFor(role)`.

**MaxBytes resolution.** Priority: explicit `WithMaxBytes` > env of the role's class > role default. The classes are `BROKERFX_STREAM_MAX_BYTES` (event, default `256 MiB`), `BROKERFX_STREAM_MAX_BYTES_CRITICAL` (command/result/reply, default `1 GiB`) and `BROKERFX_STREAM_MAX_BYTES_DLQ` (DLQ, default `256 MiB`); all are positive integers in bytes and are read in `NewDefault()`. A malformed or non-positive value of any of the three variables is never replaced by the default: `Err()` returns `ErrInvalidMaxBytesEnv`, `jsstream.New` returns it, and `Build()` panics. The check applies for every role and even when `WithMaxBytes` is set. An explicit `WithDiscard` wins over the role policy in either call order, as `WithMaxBytes` does.

**Reserve.** JetStream reserves `max_bytes` of every stream against the server's `max_file_store`; in a cluster each server counts one `max_bytes` per stream replica it hosts. Before raising the critical limit, sum `max_bytes` of all streams on the smallest server.

**Shrinking an existing stream.** `jsstream.New` never lowers `max_bytes` in a way that could drop messages. If the stream already exists, the requested limit is below its current one (unlimited counts as the highest) and `State.Bytes` is greater than half of the requested limit, the current `max_bytes` is kept and a `warn` is logged with the stream name and both figures. Raising the limit, or lowering it while the stream holds at most half of the new limit, is applied as before. A role change on a live stream (for example `DiscardOld` to `DiscardNew`) is applied by the same update and never fails the start; the max_bytes part of it still goes through the guard above. Services that build a raw `jetstream.StreamConfig` can get the same guard through `jsstream.CreateOrUpdate(ctx, js, cfg)` and the same limits through `streambuilder.LimitsFor(role)`. KV buckets (`kv.Ensure`) do not use the builder and are not affected by the env.

> **Dedup invariant:** the `Duplicates` window must be `>= 2 ×` the outbox relay
> `StallThreshold` (default 5m). The relay re-publishes a stalled IN_FLIGHT row with
> `Nats-Msg-Id = row.ID`; with 2× headroom the re-publish always lands inside a live
> dedup window, so the duplicate is suppressed. Keep `Duplicates < MaxAge`.

**Compression by role (v0.7.0).** `RoleCommand` and `RoleReply` streams get no compression, `RoleEvent`, `RoleResult` and `RoleDLQ` keep S2: command and reply messages are small, short-lived and on the latency path. An explicit `WithCompression` wins in either call order. `streambuilder.CompressionFor(role)` gives the same value for raw configs. The server accepts the change on a live stream and applies it to new blocks only. Until every service that declares the same stream runs v0.7.0, the compression of that stream flips on each start of a service of the other version; this is harmless but shows up in `stream info`.

**Duplicates window and MaxAge (v0.7.0).** The server rejects `Duplicates > MaxAge`. `Build()`, `jsstream.CreateOrUpdate` and `kv.Ensure` (against the bucket TTL) now clamp the window to `MaxAge` with a warning when `MaxAge > 0`, for the default and an explicit window alike (`streambuilder.FitDuplicates(cfg)`). Valid configs are not changed. The invariant above still has to hold after the clamp: the window must be at least `2 ×` the relay `StallThreshold` (`BROKERFX_OUTBOX_STALL_THRESHOLD`, 5m by default) of every outbox that publishes to the stream, that is `StallThreshold <= MaxAge / 2`; the warning prints that bound as `outbox_max_stall_threshold`. Example: `MaxAge = 5m` with the default threshold gives a 5m window where 10m is needed, so a stalled `IN_FLIGHT` row re-published after 5 minutes passes the dedup and the consumer handles the event twice. Set `BROKERFX_OUTBOX_STALL_THRESHOLD` (for example `60s`) on every publisher or keep `MaxAge >= 2 × StallThreshold`. Before v0.7.0 such a config was rejected by the server and the owner did not start; now it starts with the warning.

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

-- outbox_retries
CREATE TABLE outbox_retries (
    id         UUID PRIMARY KEY,
    event_id   UUID REFERENCES outbox_events(id),
    error      TEXT,
    created_at TIMESTAMPTZ
);
CREATE INDEX ON outbox_retries (event_id);
```

Event ids are UUIDv7 since v0.7.0 (time-ordered, so inserts go to the right edge of the primary key index); the string format, `Nats-Msg-Id = id` and `processed_messages` are unchanged. `BROKERFX_OUTBOX_ID_V7=false` switches back to UUIDv4; the env is read once per process, and an unparsable value keeps v7 with a warning.

Since v0.7.0 `AutoMigrate` no longer creates `idx_outbox_events_state` and `idx_outbox_events_created_at` (the claim uses `idx_outbox_events_state_updated_at` and `idx_outbox_events_pending_created_at`). `AutoMigrate` never drops an existing index, so existing databases keep them until the service drops them. After such a drop an image below v0.7.0 recreates them with a plain `CREATE INDEX` (a short write lock on `outbox_events`).

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
| `Batched` | `bool` | `false` | Batched relay R1 (also env `BROKERFX_OUTBOX_RELAY_BATCHED=true\|1`) |
| `Linger` | `time.Duration` | `0` (`5ms` when batched) | Wait after a wakeup before claiming, so close commits share one batch |
| `ClaimV2` | `bool` | `false` | Claim v2 (also env `BROKERFX_OUTBOX_CLAIM_V2=true\|1`), see below |
| `ClaimTimeout` | `time.Duration` | `5s` | Claim v2 only: deadline of one claim or reclaim statement |
| `StallReclaimInterval` | `time.Duration` | `1m` (not above `StallThreshold`) | Claim v2 only: how often stalled `IN_FLIGHT` rows are returned to `PENDING` |

Env overrides (v0.6.2, read once in `NewRelay`, `time.ParseDuration`, positive only; an invalid value is ignored with a warning and the env wins over `Config`):

| Env | Overrides |
|---|---|
| `BROKERFX_OUTBOX_CLAIM_TIMEOUT` | `ClaimTimeout` |
| `BROKERFX_OUTBOX_STALL_THRESHOLD` | `StallThreshold` |
| `BROKERFX_OUTBOX_STALL_RECLAIM_INTERVAL` | `StallReclaimInterval` |

**Batched relay (R1).** With `Config.Batched` (or the env) and the v2 schema, one relay pass is: (1) claim one batch with a single statement, `UPDATE outbox_events SET state='IN_FLIGHT', updated_at=now WHERE id IN (SELECT id ... WHERE (state='PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= now)) OR (state='IN_FLIGHT' AND updated_at < stall) ORDER BY created_at LIMIT BatchSize FOR UPDATE SKIP LOCKED) RETURNING ...`, sorted by `created_at`; (2) publish: a producer that implements `outbox.BatchProducer` (`JetStreamProducer` does: all `PublishMsgAsync` first, then one timer for all acks, an error per event) gets the whole batch, any other producer gets the old per-event fan-out; (3) successes are deleted (`ModeDeleteAfterSend`) or marked `DONE` with one statement, failures go through the usual attempts/backoff/`outbox_retries` path row by row. After a wakeup the relay waits `Linger`, and repeats the pass while batches come back full. Headers, `Nats-Msg-Id`, signing and KV last-wins are the same as in the old branch. On the legacy schema the old branch runs. Recommended with R1: `ModeDeleteAfterSend` and a partial index in the service migration, `CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_created_at ON outbox_events (created_at) WHERE state <> 'DONE'`.

**Claim v2 (v0.6.0, opt-in).** `Config.ClaimV2` or env `BROKERFX_OUTBOX_CLAIM_V2=true|1` (env is read once in `NewRelay` and OR'd with the field). Works on the v2 schema in both the batched and the plain relay; on the legacy schema the old branch runs. The claim is one statement that touches only `PENDING` rows:

```sql
WITH c AS MATERIALIZED (
  SELECT id FROM outbox_events
  WHERE state <> 'DONE' AND state = 'PENDING'
    AND (next_attempt_at IS NULL OR next_attempt_at <= $now)
  ORDER BY created_at LIMIT $batch
  FOR UPDATE SKIP LOCKED)
UPDATE outbox_events o SET state = 'IN_FLIGHT', updated_at = $now
FROM c WHERE o.id = c.id
RETURNING o.id, o.topic, o.payload, o.headers, o.attempts, o.created_at
```

States are literals, so the planner can use the partial index `idx_outbox_events_pending_created_at (created_at) WHERE state <> 'DONE'` (the literal `state <> 'DONE'` matches its predicate exactly), and `MATERIALIZED` keeps the `LIMIT ... SKIP LOCKED` subquery from being re-run inside a semi join. Without the old `OR (state = 'IN_FLIGHT' AND updated_at < stall)` the plan does not depend on table statistics. Stalled `IN_FLIGHT` rows are returned to `PENDING` by a separate statement (`WITH s AS MATERIALIZED (... state = 'IN_FLIGHT' AND updated_at < now - StallThreshold ORDER BY updated_at LIMIT 10×BatchSize FOR UPDATE SKIP LOCKED) UPDATE ... SET state = 'PENDING'`) at most once per `StallReclaimInterval` per relay (the first claim after start runs it), and the next claim picks them up by `created_at` as before; `attempts` is unchanged. Each statement runs with `context.WithTimeout(ClaimTimeout)` detached from the relay context, so a slow claim is cancelled instead of holding a snapshot (`xmin`) open. Publishing, settling, retries, parking, signing and KV last-wins are unchanged; the claimed set and its order are the same as with the flag off. Worst-case stall recovery is `StallThreshold + StallReclaimInterval` instead of `StallThreshold`, so the `Duplicates >= 2 × StallThreshold` invariant still holds with the defaults (5m + 1m < 15m).

`outbox.EnsurePendingIndex(ctx, db)` creates that partial index with `CREATE INDEX CONCURRENTLY IF NOT EXISTS` on one connection that holds `pg_try_advisory_lock(hashtext('brokerfx.outbox.pending_index'))`. When another instance holds the lock the call returns `nil` at once and does not wait: a session waiting for the lock would keep a snapshot that the concurrent build waits for, which is a deadlock (v0.6.0). The relay works without the index until the instance that holds the lock finishes it. The build runs with `statement_timeout = 0` on that connection, and the previous value is restored afterwards. It drops and rebuilds it when a previous concurrent build left it invalid, and does nothing when `outbox_events` does not exist. It must run outside a transaction (not inside a transactional goose migration). `AutoMigrate` does not create it: `AutoMigrate` runs on every start in some services, and a non-concurrent index build blocks writes to a large `outbox_events`. Services that already create the same index themselves (payout-processing `ensure_outbox_pending_index.go`) need not call it.

**`SaveMessages` (v0.6.0).** `(*OutboxStore).SaveMessages(ctx, messages []Message, opts ...MessageOption) error` writes all messages in the caller's transaction (`txmanager.GetConnection(ctx)`) with one multi-row `INSERT` per 500 rows instead of one `INSERT` per `SaveMessage`. `opts` (`WithHeaders`) apply to every message, a message's own headers (`KVDelete`) win over them, as in `SaveMessage`. `created_at` is assigned in the client as `now` truncated to microseconds plus `i` µs for the i-th message, so the relay publishes the slice in input order. Validation happens before any write: a `Message` error (`ErrInvalidKVBucket`, `ErrInvalidKVKey`), `ErrReservedHeader`, or `ErrSchemaOutdated` (headers on the legacy schema) returns without inserting anything. An empty slice is a no-op. Call `WakeupRelay` after commit as with `SaveMessage`.

**Per-message headers (v0.8.0).** `Message.Headers` is an optional map on each element of `SaveMessages` (and on `SaveMessage`). The order is `WithHeaders` first, then `Message.Headers`, then the library headers of `KVDelete`, so an element overrides the shared options and cannot change `KV-Operation`. `Nats-Msg-Id` in `Message.Headers` returns `ErrReservedHeader` for the whole call, and headers on the legacy schema return `ErrSchemaOutdated`, both before any write. A service that signs each message puts the signature there; the relay `Signer` works as before.

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

`pkg/nats/signature.NewKeyedResigner(keys map[string]ed25519.PrivateKey)` (v0.6.2) is a `Signer` for rows saved already signed: at publish time it refreshes `X-Timestamp` and `X-Signature` when `X-API-Key-ID` names one of its keys, so a row that waited in the outbox does not go out with an expired signature. A message with another key or without a signature is left unchanged.

```go
deps := outbox.RelayDeps{
	Store:  store,
	Signer: signature.NewKeyedResigner(map[string]ed25519.PrivateKey{
		"payout-processing": privateKey,
	}),
}
```

**Observer.** Pass an `Observer` as the optional `Observer` field on `RelayDeps` to get relay lifecycle hooks: `OnPublished(n)`, `OnFailed(n)`, `OnParked(eventID, subject, lastError)`, `OnStats(stats Stats)` (`Stats` — `Pending`, `InFlight`, `Done`, `Parked`, `OldestPendingAge`). `OnStats` fires every `Config.StatsInterval` when set. All four are best-effort hooks for metrics/alerting; a nil `Observer` (the default) skips them entirely.

**Attempts, backoff, PARKED.** With `Config.MaxAttempts > 0`, a failed publish increments `attempts` and schedules `next_attempt_at` using exponential backoff between `RetryBackoffBase` and `RetryBackoffMax`. Once `attempts >= MaxAttempts`, the row moves to `StateParked` instead of being retried again, and `Observer.OnParked` fires if set. `Config.RetainParked` controls how long parked rows survive before cleanup deletes them (`0` keeps them forever). Transport-level failures (no NATS servers reachable, connection closed/reconnecting) are the exception: they do not increment `attempts` or set a backoff, so a NATS outage alone never parks a row — the row stays `PENDING` and is retried as soon as the connection recovers.

**`RequeueParked` / `Stats`.** `(*OutboxStore).RequeueParked(ctx, ids []string) (int64, error)` moves parked rows back to `PENDING` with `attempts` reset, for manual recovery. Passing a non-empty `ids` limits it to those rows; passing a nil or empty slice requeues **every** currently parked row, which matters for anything exposing this as an admin endpoint. `(*OutboxStore).Stats(ctx) (Stats, error)` returns the same counts as the `OnStats` hook, for on-demand polling (health checks, admin endpoints).

**Throughput options (v0.8.0).** Unset means the v0.7.0 behaviour. For these knobs a valid env value wins over `Config`, as for the v0.6.2 claim env.

- **Pending gauge** — `Config.PendingGaugeInterval` / `BROKERFX_OUTBOX_PENDING_GAUGE_INTERVAL` (0 = off). When the relay `Observer` also implements `outbox.PendingObserver` (`OnPending(PendingStats{Pending, Capped, OldestPendingAge})`), the relay counts `PENDING` rows every interval by the partial index `idx_outbox_events_pending_created_at`, capped at 10 000 (`Capped`), and takes the oldest age from the database clock. An observer without `OnPending` turns the gauge off with a warning.
- **Cleanup in batches** — `Config.CleanupBatch` / `BROKERFX_OUTBOX_CLEANUP_BATCH` (0 = one `DELETE` as before). `N > 0` deletes `DONE` and expired `PARKED` rows in `ctid` batches of `N` with a 100ms pause, in a goroutine off the relay loop; a new cleanup does not start while one runs, and `Run` waits for it on stop.
- **Latest-only subjects** — `Config.LatestOnlyPrefixes` / `BROKERFX_OUTBOX_LATEST_ONLY_PREFIXES` (comma-separated; `$KV.` is always included). Inside one claimed batch only the latest event of a subject under these prefixes is published: when every event of the subject carries the `Brokerfx-Latest-Version` header (`outbox.LatestVersionHeader`, an integer), the highest version wins, ties go to the later row; otherwise the last row wins. The rest are closed as published without a publish. An `Observer` that implements `outbox.SupersededObserver` gets `OnSuperseded(n)`.
- **tx-drain** — `Config.TxDrain` / `BROKERFX_OUTBOX_TX_DRAIN=true|1` (off) and `Config.TxDrainPublishTimeout` / `BROKERFX_OUTBOX_TX_DRAIN_PUBLISH_TIMEOUT` (2s, must be below 25s, otherwise `NewRelay` keeps the error and the fx `OnStart` returns it). One transaction claims the rows `FOR UPDATE SKIP LOCKED` (`DELETE ... RETURNING` in `DeleteAfterSend`, `UPDATE ... SET state = 'DONE' ... RETURNING` in `UpdateAfterSend`), publishes them within the timeout, writes back the rows that failed and commits. A row whose outcome is unknown (timeout, cancel, transport error) goes back as `PENDING` without `attempts++` and with `next_attempt_at = now + max(TxDrainPublishTimeout, 5s)`: rows of a stream without a leader do not stay at the head of the `created_at` order, the other subjects keep publishing, and the retry stays inside a duplicates window of 2 minutes. After the publish timeout the relay waits up to 100ms for the per-message results of a `BatchProducer`, so rows that got their PubAck in a batch with stuck rows are not written back. Each batch that contains rows of a stuck stream still costs one full publish timeout; a server refusal counts an attempt with backoff and parks at `MaxAttempts`. No `IN_FLIGHT` rows are written, so a crash leaves the rows `PENDING` for the next relay, and two relays never publish the same row. It runs only on the v2 schema; in `DeleteAfterSend` failed rows are inserted back, so a table with extra `NOT NULL` columns without a default keeps the claim path (error log). After 3 transaction failures in a row it switches itself off for the process (warning). The stalled `IN_FLIGHT` reclaim keeps running for rows of the old path.
- **Id lists** — settle, revert and `RequeueParked` pass ids as `= ANY(?::uuid[])` or `= ANY(?::text[])` by the type of `outbox_events.id` read once from `information_schema`; the batch claim of `processed_messages` uses `unnest` with `WITH ORDINALITY`. Each statement has one form for any batch size; `IN ?` stays when the type is unknown.

#### Frames (v0.9.0, opt-in)

A frame packs several outbox rows of one subject into one NATS message: one publish, one stream write and one ack instead of N. Package `pkg/nats/frame` has no fx and no proto dependency.

- **Wire format.** Headers `Brokerfx-Frame: 1` and `Brokerfx-Frame-Count: N`; the body is a msgpack array of elements `{subject, headers, payload}` in row order. An element is the `ProducerEvent` the relay would have published on its own: its own `Nats-Msg-Id`, trace and signature headers. The frame itself is not signed. The frame subject is the subject of its elements; the frame `Nats-Msg-Id` is `frame-` + sha256 of the sorted element ids, so a retry of the same rows is deduplicated by the stream, and a retry with another set of rows is a new frame whose elements the consumer deduplicates by their own ids.
- **Relay.** `Config.FrameSubjects` / `BROKERFX_FRAME_SUBJECTS` (comma list of NATS subject patterns with `*` and `>`, not prefixes; empty = off, the default; a set env, even empty, wins over `Config`). Only the batched relay frames (`Batched` and tx-drain); the per-message relay logs a warning and publishes per message. Within one claimed batch, rows of the same exact subject are grouped in row order, up to `BROKERFX_FRAME_MAX_ELEMENTS` (16, range 2..1024) and `BROKERFX_FRAME_MAX_BYTES` (262144, at least 1024; clamped to the server `max_payload` minus 1 KiB with a warning). A row bigger than the frame limit, a `$KV.` row, a subject from the latest-only prefixes, a row without `Nats-Msg-Id` and a row with `Nats-Expected-*`, `Nats-TTL` or `Nats-Rollup` go per message, and a row that goes per message closes the open frame of its subject, so the order inside a subject is kept. Rows of different subjects are published concurrently, as before. A failed frame publish fails every row of the frame. An `Observer` that implements `outbox.FrameObserver` gets `OnFramed(FramedEvent{Messages, Frames, Elements})`.
- **Consumers.** Unpacking has no switch: a message without `Brokerfx-Frame` is handled as before. `frame.Unpack(msg, heartbeat)` returns the elements as `jetstream.Msg` (the element subject, headers and data, the frame metadata) and a plain message as a one-element slice; `frame.IsFrame(msg)` is the predicate. Ack, Nak, NakWithDelay, Term and InProgress of an element record its outcome; when the last element is settled the frame is settled once: every element acked or terminated → Ack, any Nak → NakWithDelay with the largest element delay (Nak when all delays are 0). InProgress of the elements reaches the frame at most once per half heartbeat. A second settle of an element returns `jetstream.ErrMsgAlreadyAckd`. Element `Metadata()` is the frame metadata: every element of a frame has the same stream sequence and `NumDelivered`, so never key anything by the sequence.
- **Batch consumer** unpacks right after the fetch: verdicts, `OrderKey`, bisection, isolation, `processed_messages` claims and `OnPoison` work per element, and the attempt of an element is the delivery count of its frame. A failing element goes to `OnPoison` alone at `MaxAttempts`; the elements already applied come back with the redelivered frame and are skipped by their claims. `Max` and `MaxAckPending` count frames, so one handler call can hold up to `Max × BROKERFX_FRAME_MAX_ELEMENTS` messages. A frame that cannot be decoded (bad msgpack, count mismatch, element subject other than the frame subject, unknown version) goes to `OnPoison` whole and is terminated; a `batch.FrameObserver` gets `OnFrame(FrameEvent{Elements, Err})`.
- **Per-message consumer** runs middlewares and the handler for every element in order, a failed element does not stop the next ones, and the frame gets `InProgress` while its elements run. A broken frame goes to the new `consumer.Opts.OnPoison` and is terminated (without `OnPoison` it is terminated with an error log; an `OnPoison` error NAKs the frame).
- **Rollout.** Every durable of a subject must run a consumer that unpacks frames (brokerfx v0.9.0 or `frame.Unpack` in nats.go code) and deduplicate elements by their id in the database before the producer lists the subject: an older consumer sees a frame as one undecodable message. Rollback is the empty env on the producer; a consumer image older than v0.9.0 only after the frames of the subject are drained.

**ОК-0.** `stream-opts-builder.NewDefault` (MaxAge 12h, Replicas 1, Duplicates 15m, WorkQueue) and `outbox.DefaultConfig()` are unchanged by v2 and must stay that way — v2 config lives entirely in the new, separately-opted-in fields and in `RecommendedConfig()`.

---

### pkg/pgtimer

Timers in a Postgres table the service owns, for a service that writes the timer intent in its business transaction. The library claims due rows with a lease, calls the handler registered for the timer name and deletes the row only after the handler returned `nil`.

```go
poller, err := pgtimer.New(db, pgtimer.Config{
	Table: pgtimer.Table{
		Name:      "payin_timers",
		Key:       "order_id",
		TimerName: "name",
		Attempt:   "attempt_id",
		DueAt:     "due_at",
		CreatedAt: "created_at",
		UpdatedAt: "updated_at",
	},
})
err = poller.Handle("VISIT_RESOURCE", func(ctx context.Context, timer pgtimer.Timer) error {
	return orders.ExpireVisit(ctx, timer.Key, timer.Attempt)
})
pgtimer.RegisterLifecycle(lc, poller)
```

- **Table.** Column names are plain identifiers (`[A-Za-z_][A-Za-z0-9_]*`, table may be `schema.name`) and are quoted; `CreatedAt` and `UpdatedAt` are optional. The service creates the table and an index on `due_at`.
- **Claim.** One statement: `WITH due AS MATERIALIZED (SELECT ctid ... WHERE due_at <= now() AND name IN (registered names) ORDER BY due_at, created_at LIMIT free slots FOR UPDATE SKIP LOCKED) UPDATE ... SET due_at = now() + lease, updated_at = now() ... RETURNING` the original `due_at`, the lease `due_at` and the database `now()`. Two pollers never take the same row; a poller that dies gives the row back when the lease ends. Rows with names this poller did not register are not touched.
- **Fire.** A pool of `Concurrency` workers; each handler runs with `HandlerTimeout`, which must be at most `Lease / 2`, and a panic is recovered as `ErrHandlerPanic`. The delete after the handler has `min(5s, Lease / 4)`, so handler and delete end before the lease. The handler must return on `ctx.Done()`: a handler that ignores its context outlives the lease and the timer fires again on another poller in parallel. After success the row is deleted by key, name, attempt and the exact lease `due_at`, so a timer that the handler or the business code rescheduled meanwhile (new `due_at`, name or attempt) is kept. A failed handler leaves the row; it fires again when the lease ends, which is the retry delay.
- **Loop.** A poll every `Poll`; a full batch polls again at once. `Stop(ctx)` stops claiming and waits for running handlers within `ctx`; after the deadline their context is cancelled and unfinished rows fire again after the lease. Nothing claimed is lost.
- **Observer.** `OnClaim(ClaimEvent{Limit, Claimed, Duration, Err})` and `OnFire(FireEvent{Name, Lag, Duration, Deleted, Err})`, where `Lag` is the database `now()` at claim minus the original `due_at`.
- **Delivery.** At-least-once: a handler that outlives the lease, or a lost delete, fires again, so handlers re-check the state under a lock.

### pkg/replay

Snapshot replay over JetStream: a publisher reads items from a `Source` and publishes one message per item to a single subject, followed by an end marker that carries the item count and status.

- The stream is created by its owner, not by this package: Limits retention, `MaxMsgsPerSubject` of -1, `Replicas` and `MaxAge` from config. `NewDefault` of the stream builder is not suitable as is. `Publisher.Run` checks the stream and returns `ErrStreamUnsuitable` if it would truncate replay subjects.
- One subject per run: the caller must include the replay id in the subject, so runs do not mix; `Request.Validate` does not check this.
- Each item message carries `X-Replay-Id` and `X-Replay-Seq`; the end marker carries `X-Replay-Id`, `X-Replay-End`, `X-Replay-Count` and `X-Replay-Status` (`complete` or `failed`, with `X-Replay-Error` on failure) and no `X-Replay-Seq`, so `ParseMeta` returns Seq 0 for it. `ParseMeta` decodes them.

Reader rules:

- Compare `Count` with the number of unique keys received (the key is the middle part of `Nats-Msg-Id`, `<id>:<key>:<version>`, or lives in your payload); a mismatch means the snapshot is incomplete.
- `failed` means wait for a retry or request a new replay with a new id.
- Set your own timeout: a run that never delivers the end marker is not signalled by the library.
- Apply snapshots by `Version`; older versions must not overwrite newer state.

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
| `MarkerTTL` | `time.Duration` | `0` | v0.7.0. `> 0` (at least 1s): fired and cleared keys are purged with a delete marker that expires after `MarkerTTL`; `0` keeps the bucket's current setting |
| `RescanInterval` | `time.Duration` | `0` | v0.7.0. `> 0`: full rescan on this period (and once after the initial watch values) instead of every `CheckInterval`, plus a local retry of a failed key removal |

#### Marker TTL, rescan and rollback (v0.7.0)

- **Why.** A fired or cleared timer used to leave a delete marker that lived for `BucketTTL` (48h), so `num_subjects` of the bucket grew with every timer ever set. With `MarkerTTL > 0` the bucket is created or updated with `LimitMarkerTTL` (`allow_msg_ttl`, `subject_delete_marker_ttl`) and keys are removed with `Purge(PurgeTTL(MarkerTTL), LastRevision(rev))`: the CAS guard and the exactly-once callback stay, and the marker goes away after `MarkerTTL`. Markers written before the switch stay until `BucketTTL` or a one-off `nats kv compact`.
- **`MarkerTTL = 0` on a bucket that already has a marker TTL keeps it**: `New` reads the KV stream and passes the existing value, because the server cannot turn message TTL off.
- **Rollback trap.** Once a bucket has `allow_msg_ttl`, any image below v0.7.0 (or any code that calls `CreateOrUpdateKeyValue` without `LimitMarkerTTL`) fails on start with `message TTL status can not be disabled` and crash-loops. To roll back below v0.7.0, delete the bucket first (`nats kv del <bucket>`), then start the old image. Live timers are then restored only by the service reconciler from its database, so **a reconciler is mandatory** for every service that enables `MarkerTTL`. Back up `nats kv info <bucket> --json` before enabling.
- **`RescanInterval`.** With `0` the safety-net rescan runs every `CheckInterval` as before. With `> 0` it runs on that period and once right after the watcher delivered the initial values (overdue keys left by a stopped pod fire at start). In this mode an unexpected error while removing a fired key (anything but a lost CAS race) schedules a local retry after `CheckInterval` instead of waiting for the next rescan. If the removal actually succeeded and only the reply was lost, the retry sees a revision mismatch and the callback is not run; the reconciler covers that case.
- **At-most-once callback.** The CAS removal picks the winner before the callback runs, so a pod that stops between the removal and the end of the callback loses that timer: the key is already gone and no replica fires it again. timedactor keeps this order on purpose; a service that cannot lose a timer either runs a reconciler over its database or uses `pkg/pgtimer`, which deletes the row only after the handler returned without error.

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
| MaxBytes | `256 MiB` for `RoleEvent` (override: `BROKERFX_STREAM_MAX_BYTES`); `1 GiB` for command/result/reply roles (override: `BROKERFX_STREAM_MAX_BYTES_CRITICAL`); `256 MiB` for `RoleDLQ` (override: `BROKERFX_STREAM_MAX_BYTES_DLQ`) |
| Discard | `DiscardOld` for `RoleEvent`; `DiscardNew` for command/result/reply/DLQ roles |
| MaxAge | `12 hours` |
| Retention | `WorkQueuePolicy` |
| MaxMsgsPerSubject | `1,000` |
| Compression | `S2Compression`; none for command/reply roles |
| Replicas | `1` |

### Consumer Defaults

| Setting | Default |
|---|---|
| AckPolicy | `AckExplicitPolicy` |
| AckWait | `30 seconds` |
| DeliverPolicy | `DeliverAllPolicy` (`WithDeliverByStartSequence(n)` for a start sequence, 0 keeps `DeliverAll`) |
| MaxAckPending | server default (1000); `WithMaxAckPending(n)`; the batch consumer sets `Pipeline × Max` when unset, `MaxAckPendingFor(Replicas, Pipeline, Max)` with `Replicas > 0` |
| Pull: MaxMessages | `50`; `1` when `MaxInFlight > 0` |
| MaxInFlight | `0` = unbounded (`BROKERFX_CONSUMER_MAX_IN_FLIGHT`) |
| NakBackoff | off (`BROKERFX_CONSUMER_NAK_BACKOFF=true` or a list such as `1s,2s,5s`) |
| Replicas | `0` = inherit the stream (`WithReplicas(n)`, reset to `0` when it does not fit) |
| StrictUpdate | off; `true` fails `New` on a rejected update (`BROKERFX_CONSUMER_STRICT_UPDATE=false` turns it off) |
| Filters | one `FilterSubject`; `WithFilterSubjects` for several, never both |
| Priority | none; `WithPinnedGroup(group, ttl)` for pinned, `PartitionConfigs` for `<durable>-p<i>` |
| Pull: Heartbeat | `10 seconds` |
| Batch: Max / Linger / Pipeline | `16` / `5ms` / `1` (`BROKERFX_BATCH_MAX`, `BROKERFX_BATCH_LINGER`, `BROKERFX_BATCH_PIPELINE`) |
| Batch: MinBatch / LingerMax | off (`BROKERFX_BATCH_MIN`, `BROKERFX_BATCH_LINGER_MAX`) |
| Batch: Replicas | `0` = one pod (`BROKERFX_BATCH_REPLICAS`) |
| Batch: retry ladder | `1s, 5s, 30s, 2m, 5m`, `MaxAttempts = len(Delays)+1` |
| Processed cleanup | retention `48h` (`BROKERFX_PROCESSED_RETENTION`), interval `1h` (`BROKERFX_PROCESSED_CLEANUP_INTERVAL`), batch `5000` |

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
| Batched (R1) | off (`Config.Batched` or `BROKERFX_OUTBOX_RELAY_BATCHED=true\|1`) |
| Linger | `5ms` when batched |
| Event id | UUIDv7 (`BROKERFX_OUTBOX_ID_V7=false` for UUIDv4) |
| Pending gauge | off (`BROKERFX_OUTBOX_PENDING_GAUGE_INTERVAL`) |
| Cleanup batch | `0`, one `DELETE` (`BROKERFX_OUTBOX_CLEANUP_BATCH`) |
| Latest-only prefixes | `$KV.` only (`BROKERFX_OUTBOX_LATEST_ONLY_PREFIXES`) |
| tx-drain | off (`BROKERFX_OUTBOX_TX_DRAIN`), publish timeout `2s` |
| Frames | off (`BROKERFX_FRAME_SUBJECTS`), `16` elements (`BROKERFX_FRAME_MAX_ELEMENTS`), `262144` bytes (`BROKERFX_FRAME_MAX_BYTES`) |

### TimedActor Defaults

| Setting | Default |
|---|---|
| BucketName | `"TimedActorBucket"` |
| CheckInterval | `15 seconds` |
| MaxHoldDuration | `30 seconds` |
| BucketTTL | `48 hours` |
| MarkerTTL | `0` (off, existing bucket setting kept) |
| RescanInterval | `0` (rescan every `CheckInterval`) |

### PgTimer Defaults

| Setting | Default | Env |
|---|---|---|
| Lease | `60s` | `BROKERFX_PGTIMER_LEASE` |
| HandlerTimeout | `30s`, at most `Lease / 2` | `BROKERFX_PGTIMER_HANDLER_TIMEOUT` |
| Poll | `1s` | `BROKERFX_PGTIMER_POLL` |
| Batch | `100` (1..1000) | `BROKERFX_PGTIMER_BATCH` |
| Concurrency | `8` (1..256) | `BROKERFX_PGTIMER_CONCURRENCY` |

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

`.github/workflows/release.yml` runs on every push to `main` and to `feat/**` branches (release only on `main`):

1. **Test** — `go mod tidy` + `go test -v ./...`
2. **Integration** — `go test -tags=integration` in `integration/`
3. **Release** — after both are green, auto-bump semver tag and create GitHub release with changelog

Release convention: commits use Conventional Commits. `feat` produces a minor tag, `fix` a patch tag, other types a patch tag (`default_bump: patch`). `#minor` in the message is not read. `BREAKING CHANGE` and `!` are not used. Tags are set by the action and are never moved by hand.

---

## License

MIT © 2026 dehwyy
