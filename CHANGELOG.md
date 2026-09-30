# Changelog

## v0.4.1

- Stream default `MaxBytes` lowered from 2 GiB to 256 MiB. 48 streams at 2 GiB reserve 96 GiB of JetStream storage against well under 1 MiB of real data.
- Separate limits per explicit stream role (NF-D-192): `StreamOptsBuilder.WithRole(Role)` with `RoleEvent` (default, 256 MiB, `DiscardOld`) and `RoleCommand`, `RoleResult`, `RoleReply` (1 GiB) and `RoleDLQ` (256 MiB), all `DiscardNew` (overflow is a publish error, the outbox retries). `WithDiscard` sets the policy explicitly. `LimitsFor(role)` returns the same numbers for raw `jetstream.StreamConfig` users. A stream that never calls `WithRole` is an event stream, so services must opt in when they bump.
- `BROKERFX_STREAM_MAX_BYTES` overrides the event limit and `BROKERFX_STREAM_MAX_BYTES_CRITICAL` the command/result/reply limit, `BROKERFX_STREAM_MAX_BYTES_DLQ` the DLQ limit (positive integers, bytes). An explicit `WithMaxBytes` takes priority over both. A malformed or non-positive value fails startup: `StreamOptsBuilder.Err()`, `stream.New` returns `ErrInvalidMaxBytesEnv`, `Build()` panics.
- `stream.New` no longer lowers `max_bytes` of an existing stream when its `State.Bytes` exceeds half of the requested limit. The current limit is kept and a warning is logged. Lowering below that threshold and raising the limit behave as before.
- New exported helpers for services that create streams from raw `jetstream.StreamConfig`: `stream.CreateOrUpdate(ctx, js, cfg)` applies the same shrink guard, `streamoptsbuilder.MaxBytesFromEnv()` returns the env-or-default limit.
- KV buckets (`kv.Ensure`) are unchanged.
- Rollout note: services pick up the new default at the next start after bumping brokerfx. Existing streams are updated by `CreateOrUpdateStream` when the owner restarts.
