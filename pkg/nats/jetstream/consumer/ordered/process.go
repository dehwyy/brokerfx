package ordered

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/internal/itemctx"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

var errHandlerPanic = errors.New("panic")

func markCaughtUp(msg jetstream.Msg, caughtUp *atomic.Bool) {
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return
	}

	if meta.NumPending == 0 {
		caughtUp.Store(true)
	}
}

func hasTraceContext(msg jetstream.Msg) bool {
	extracted, err := tracenats.ExtractTraceContext(
		context.Background(),
		msg,
	)
	if err != nil {
		return false
	}

	return trace.SpanContextFromContext(extracted).IsValid()
}

func process(loopCtx context.Context, msg jetstream.Msg, opts Opts, caughtUp *atomic.Bool) {
	defer markCaughtUp(
		msg,
		caughtUp,
	)

	ctx := loopCtx

	for _, mw := range opts.BeforeHandlerMiddleware {
		var err error
		ctx, err = mw(
			ctx,
			msg,
		)
		if err != nil {
			log.Error().
				Err(err).
				Str(
					"subject",
					msg.Subject(),
				).
				Msg("ordered consumer middleware failed, message skipped")
			return
		}
	}

	var span trace.Span
	if caughtUp.Load() && hasTraceContext(msg) {
		ctx, span = tracenats.StartConsumer(
			ctx,
			msg.Subject(),
			msg.Headers(),
			streamAttributes(msg)...,
		)
		defer span.End()
	} else {
		ctx = itemctx.WithBaggage(
			ctx,
			msg.Headers(),
		)
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			log.Error().
				Any(
					"panic",
					recovered,
				).
				Str(
					"subject",
					msg.Subject(),
				).
				Msg("ordered consumer handler panic")

			failSpan(
				span,
				errHandlerPanic,
			)
		}
	}()

	if err := opts.HandlerFunc(
		ctx,
		msg,
	); err != nil {
		log.Error().
			Err(err).
			Str(
				"subject",
				msg.Subject(),
			).
			Msg("ordered consumer handler failed")

		failSpan(
			span,
			err,
		)
	}
}
