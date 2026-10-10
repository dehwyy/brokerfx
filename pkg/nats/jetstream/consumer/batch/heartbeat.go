package batch

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func (c *Consumer) heartbeat(msgs []jetstream.Msg) func() {
	interval := c.heartbeatEvery
	if interval <= 0 {
		interval = defaultHeartbeat
	}

	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				progressUnsettled(msgs)
			}
		}
	}()

	return func() {
		close(done)
		<-exited
	}
}

const defaultHeartbeat = 15 * time.Second

func progressUnsettled(msgs []jetstream.Msg) {
	for _, msg := range msgs {
		err := msg.InProgress()
		if err == nil || errors.Is(
			err,
			jetstream.ErrMsgAlreadyAckd,
		) {
			continue
		}

		log.Warn().
			Err(err).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("batch in-progress heartbeat failed")
	}
}
