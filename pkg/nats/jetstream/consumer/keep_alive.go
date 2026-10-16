package consumer

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func keepAlive(
	msg jetstream.Msg,
	every time.Duration,
) func() {
	ticker := time.NewTicker(every)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					if errors.Is(
						err,
						jetstream.ErrMsgAlreadyAckd,
					) {
						return
					}
					log.Warn().
						Err(err).
						Str(
							"subject",
							msg.Subject(),
						).
						Msg("failed to send InProgress heartbeat")
				}
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(done)
	}
}
