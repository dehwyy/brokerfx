package consumer

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

func settled(err error) bool {
	return errors.Is(err, jetstream.ErrMsgAlreadyAckd)
}

func process(msg jetstream.Msg, opts Opts) {
	log.Debug().Any("subject", msg.Subject()).Msg("nats message received")
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Str("subject", msg.Subject()).
				Any("panic", r).
				Msg("consumer handler panic — NAK for redelivery")
			if nakErr := msg.Nak(); nakErr != nil && !settled(nakErr) {
				log.Error().Err(nakErr).Msg("failed to NAK after panic")
			}
		}
	}()

	ctx := context.Background()
	var err error
	for _, mw := range opts.BeforeHandlerMiddleware {
		ctx, err = mw(ctx, msg)
		if err != nil {
			log.Error().Err(err).Msg("error in before handler middleware — NAK for redelivery")
			if nakErr := msg.Nak(); nakErr != nil && !settled(nakErr) {
				log.Error().Err(nakErr).Msg("failed to NAK after middleware error")
			}
			return
		}
	}

	if err = runWithHeartbeat(ctx, msg, opts.HandlerFunc); err != nil {
		nakErr := msg.Nak()
		switch {
		case nakErr == nil:
			log.Error().Err(err).Str("subject", msg.Subject()).Msg("handler failed — NAK for redelivery")
		case settled(nakErr):
			log.Warn().Err(err).Str("subject", msg.Subject()).Msg("handler failed, message already settled by handler")
		default:
			log.Error().Err(err).Str("subject", msg.Subject()).Msg("handler failed — NAK for redelivery")
			log.Error().Err(nakErr).Msg("failed to NAK after handler error")
		}
		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		if settled(ackErr) {
			log.Debug().Str("subject", msg.Subject()).Msg("message already settled by handler")
		} else {
			log.Error().Err(ackErr).Str("subject", msg.Subject()).Msg("failed to ACK after successful handler")
		}
	}

	for _, mw := range opts.AfterHandlerMiddleware {
		ctx, err = mw(ctx, msg)
		if err != nil {
			log.Error().Err(err).Msg("error in after handler middleware (message already acked)")
			return
		}
	}
}
