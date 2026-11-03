package batch

import (
	"context"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/itemctx"
	"github.com/nats-io/nats.go/jetstream"
)

func ItemContext(ctx context.Context, msg jetstream.Msg) context.Context {
	return itemctx.WithBaggage(
		ctx,
		msg.Headers(),
	)
}
