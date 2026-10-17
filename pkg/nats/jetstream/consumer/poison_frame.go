package consumer

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func poisonFrame(
	msg jetstream.Msg,
	opts Opts,
	reason error,
) {
	if opts.OnPoison == nil {
		log.Error().
			Err(reason).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("consumer frame cannot be unpacked, terminated without OnPoison")
		logTermErr(
			msg,
			msg.Term(),
		)
		return
	}

	if err := callOnPoison(
		msg,
		opts,
		reason,
	); err != nil {
		log.Error().
			Err(err).
			AnErr(
				"reason",
				reason,
			).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("consumer OnPoison failed for a frame, redelivered later")
		if nakErr := nak(
			msg,
			opts.NakBackoff,
		); nakErr != nil && !settled(nakErr) {
			log.Error().Err(nakErr).Msg("failed to NAK a frame after OnPoison error")
		}
		return
	}

	log.Error().
		Err(reason).
		Str(
			"subject",
			msg.Subject(),
		).
		Msg("consumer frame cannot be unpacked, handed to OnPoison")
	logTermErr(
		msg,
		msg.Term(),
	)
}

func callOnPoison(
	msg jetstream.Msg,
	opts Opts,
	reason error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"consumer: OnPoison panicked: %v",
				recovered,
			)
		}
	}()

	return opts.OnPoison(
		context.Background(),
		msg,
		reason,
	)
}

func logTermErr(
	msg jetstream.Msg,
	err error,
) {
	if err == nil || settled(err) {
		return
	}
	log.Error().
		Err(err).
		Str(
			"subject",
			msg.Subject(),
		).
		Msg("failed to TERM a frame")
}
