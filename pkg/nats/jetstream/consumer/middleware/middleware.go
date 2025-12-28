package middleware

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type Middleware func(ctx context.Context, msg jetstream.Msg) (context.Context, error)
