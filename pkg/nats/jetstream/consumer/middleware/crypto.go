package middleware

import (
	"context"

	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/nats-io/nats.go/jetstream"
)

// Sets decoded value to context like: "contextKey" - *T
func NewDecodeMiddleware[T any](contextKey string) Middleware {
	return func(ctx context.Context, msg jetstream.Msg) (context.Context, error) {
		data, err := cryptov1.Decode[T](msg.Data())
		if err != nil {
			return ctx, err
		}

		ctx = context.WithValue(ctx, contextKey, &data)

		return ctx, nil
	}
}
