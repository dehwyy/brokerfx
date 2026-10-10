package batch

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type Handler func(ctx context.Context, msgs []jetstream.Msg) []Result

type KeyFunc func(msg jetstream.Msg) string

type PoisonFunc func(ctx context.Context, msg jetstream.Msg, err error) error
