package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

var (
	retryWindow = 30 * time.Second
	retryFirst  = 250 * time.Millisecond
	retryMax    = 2 * time.Second
)

func bindRetrying(
	ctx context.Context,
	req *BindRequest,
) (jetstream.Consumer, error) {
	deadline := time.Now().Add(retryWindow)
	delay := retryFirst
	for attempt := 1; ; attempt++ {
		bound, err := bindOnce(
			ctx,
			req,
		)
		if err == nil || !isTransient(err) {
			return bound, err
		}
		if time.Now().Add(delay).After(deadline) {
			return nil, fmt.Errorf(
				"%w: stream %s durable %s after %d attempts: %w",
				ErrBindTransient,
				req.Stream,
				Name(req.Config),
				attempt,
				err,
			)
		}

		log.Warn().
			Err(err).
			Str(
				"stream",
				req.Stream,
			).
			Str(
				"consumer",
				Name(req.Config),
			).
			Int(
				"attempt",
				attempt,
			).
			Dur(
				"retry_in",
				delay,
			).
			Msg("consumer strict bind failed with a transient error, retrying")

		if err := sleep(
			ctx,
			delay,
		); err != nil {
			return nil, err
		}
		delay = min(
			delay*2,
			retryMax,
		)
	}
}

func sleep(
	ctx context.Context,
	delay time.Duration,
) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
