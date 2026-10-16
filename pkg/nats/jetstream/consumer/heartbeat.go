package consumer

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var inProgressInterval = 5 * time.Second

func inProgressIntervalForTest(d time.Duration) func() {
	prev := inProgressInterval
	inProgressInterval = d
	return func() { inProgressInterval = prev }
}
func runWithHeartbeat(
	ctx context.Context,
	msg jetstream.Msg,
	handler func(ctx context.Context, msg jetstream.Msg) error,
) error {
	return runWithHeartbeatEvery(
		ctx,
		msg,
		inProgressInterval,
		handler,
	)
}

func runWithHeartbeatEvery(
	ctx context.Context,
	msg jetstream.Msg,
	every time.Duration,
	handler func(ctx context.Context, msg jetstream.Msg) error,
) error {
	stop := keepAlive(
		msg,
		every,
	)
	defer stop()

	return handler(
		ctx,
		msg,
	)
}
