package consumer

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

// inProgressInterval is how often msg.InProgress() is sent while a handler runs.
// It must stay comfortably below the consumer AckWait (default 30s) so the server
// keeps extending the ack deadline instead of redelivering to a second goroutine.
// The consumer does not see the server-side AckWait, so this is a fixed sane value.
// It is a var (not const) so tests can shorten it; production never reassigns it.
var inProgressInterval = 5 * time.Second

// inProgressIntervalForTest overrides the heartbeat interval and returns a restore
// func. Intended for tests only.
func inProgressIntervalForTest(d time.Duration) func() {
	prev := inProgressInterval
	inProgressInterval = d
	return func() { inProgressInterval = prev }
}

// runWithHeartbeat invokes handler while periodically sending msg.InProgress() so
// JetStream extends the ack deadline (AckWait) for the duration of a slow handler.
// Without this a handler slower than AckWait gets redelivered while the original
// goroutine still runs, causing concurrent double-processing on the money path.
//
// The ticker is stopped as soon as the handler returns (success or error). The
// Ack/Nak/recover decisions stay with the caller — this only keeps the lease alive.
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
