package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
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
	ticker := time.NewTicker(inProgressInterval)
	defer ticker.Stop()

	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
						return
					}
					log.Warn().
						Err(err).
						Str("subject", msg.Subject()).
						Msg("failed to send InProgress heartbeat")
				}
			}
		}
	}()

	return handler(ctx, msg)
}
