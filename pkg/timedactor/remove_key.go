package timedactor

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

func (ta *TimedActor[T]) removeKey(
	ctx context.Context,
	key string,
	opts ...jetstream.KVDeleteOpt,
) error {
	if ta.config.MarkerTTL > 0 {
		return ta.kv.Purge(
			ctx,
			key,
			append(
				opts,
				jetstream.PurgeTTL(ta.config.MarkerTTL),
			)...,
		)
	}

	return ta.kv.Delete(
		ctx,
		key,
		opts...,
	)
}
