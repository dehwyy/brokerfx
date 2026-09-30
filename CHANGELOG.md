# Changelog

## v0.4.1

- Stream default `MaxBytes` lowered from 2 GiB to 256 MiB. 48 streams at 2 GiB reserve 96 GiB of JetStream storage against well under 1 MiB of real data.
- `BROKERFX_STREAM_MAX_BYTES` overrides the default (positive integer, bytes). An explicit `WithMaxBytes` takes priority over both. A malformed or non-positive value fails startup: `StreamOptsBuilder.Err()`, `stream.New` returns `ErrInvalidMaxBytesEnv`, `Build()` panics.
- `stream.New` no longer lowers `max_bytes` of an existing stream when its `State.Bytes` exceeds half of the requested limit. The current limit is kept and a warning is logged. Lowering below that threshold and raising the limit behave as before.
- New exported helpers for services that create streams from raw `jetstream.StreamConfig`: `stream.CreateOrUpdate(ctx, js, cfg)` applies the same shrink guard, `streamoptsbuilder.MaxBytesFromEnv()` returns the env-or-default limit.
- KV buckets (`kv.Ensure`) are unchanged.
- Rollout note: services pick up the new default at the next start after bumping brokerfx. Existing streams are updated by `CreateOrUpdateStream` when the owner restarts.
