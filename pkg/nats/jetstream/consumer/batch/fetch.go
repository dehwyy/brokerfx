package batch

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const idlePoll = 5 * time.Second

type fetcher interface {
	Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error)
}

func (c *Consumer) fetchBatch(
	ctx context.Context,
	idle bool,
) ([]jetstream.Msg, error) {
	msgs := make(
		[]jetstream.Msg,
		0,
		c.max,
	)

	if idle {
		idleCtx, cancel := context.WithTimeout(
			ctx,
			idlePoll,
		)
		first, err := c.collect(
			1,
			jetstream.FetchContext(idleCtx),
		)
		cancel()
		if err != nil || len(first) == 0 || ctx.Err() != nil {
			return first, err
		}
		msgs = append(
			msgs,
			first...,
		)
	}

	room := c.max - len(msgs)
	if room <= 0 {
		return msgs, nil
	}

	rest, err := c.collect(
		room,
		jetstream.FetchMaxWait(c.linger),
	)
	msgs = append(
		msgs,
		rest...,
	)

	return msgs, err
}

func (c *Consumer) collect(
	count int,
	opt jetstream.FetchOpt,
) ([]jetstream.Msg, error) {
	fetched, err := c.fetcher.Fetch(
		count,
		opt,
	)
	if err != nil {
		return nil, err
	}

	msgs := make(
		[]jetstream.Msg,
		0,
		count,
	)
	for msg := range fetched.Messages() {
		msgs = append(
			msgs,
			msg,
		)
	}

	if err := fetched.Error(); err != nil && !isQuietFetchErr(err) {
		return msgs, err
	}

	return msgs, nil
}

func isQuietFetchErr(err error) bool {
	for _, quiet := range []error{
		nats.ErrTimeout,
		jetstream.ErrNoMessages,
		context.Canceled,
		context.DeadlineExceeded,
	} {
		if errors.Is(
			err,
			quiet,
		) {
			return true
		}
	}

	return false
}

func isFatalFetchErr(err error) bool {
	return errors.Is(
		err,
		jetstream.ErrConsumerDeleted,
	) || errors.Is(
		err,
		jetstream.ErrConsumerNotFound,
	)
}
