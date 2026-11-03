package ordered

import (
	"context"

	tracenats "github.com/dehwyy/tracerfx/pkg/tracer/delivery/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

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

func process(loopCtx context.Context, msg jetstream.Msg, opts Opts) {
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
				Str("subject", msg.Subject()).
				Msg("ordered consumer middleware failed, message skipped")
			return
		}
	}

	var span trace.Span
	if hasTraceContext(msg) {
		ctx, span = tracenats.StartConsumer(
			ctx,
			msg.Subject(),
			msg.Headers(),
		)
		defer span.End()
	}

	if err := opts.HandlerFunc(
		ctx,
		msg,
	); err != nil {
		log.Error().
			Err(err).
			Str("subject", msg.Subject()).
			Msg("ordered consumer handler failed")

		if span != nil {
			span.RecordError(err)
			span.SetStatus(
				codes.Error,
				err.Error(),
			)
		}
	}
}
