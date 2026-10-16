package consumer

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func waitForSlot(
	msg jetstream.Msg,
	opts Opts,
	inFlight *pool,
) bool {
	stop := keepAlive(
		msg,
		heartbeatEvery(opts),
	)
	acquired := inFlight.acquire()
	stop()
	if acquired {
		return true
	}

	if err := msg.Nak(); err != nil && !settled(err) {
		log.Warn().
			Err(err).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("consumer stopped before a slot was free, NAK failed, redelivery after AckWait")
		return false
	}

	log.Info().
		Str(
			"subject",
			msg.Subject(),
		).
		Msg("consumer stopped before a slot was free, NAK for redelivery")
	return false
}
