package consumer

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

func NewHandlerFunc[T any](
	handlerFunc func(context.Context, Message[T]) error,
) func(context.Context, jetstream.Msg) error {
	return func(ctx context.Context, msg jetstream.Msg) error {
		return handlerFunc(ctx, Message[T]{
			Msg: msg,
		})
	}
}
