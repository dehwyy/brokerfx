package batch

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
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

	started := c.clock()
	rest, err := c.collect(
		room,
		jetstream.FetchMaxWait(c.linger),
	)
	msgs = append(
		msgs,
		rest...,
	)
	if err != nil {
		return msgs, err
	}

	return c.fillToMin(
		ctx,
		msgs,
		started,
	)
}

func (c *Consumer) fillToMin(
	ctx context.Context,
	msgs []jetstream.Msg,
	started time.Time,
) ([]jetstream.Msg, error) {
	if c.minBatch <= 0 || c.lingerMax <= 0 {
		return msgs, nil
	}

	deadline := started.Add(c.lingerMax)
	for len(msgs) < c.minBatch && len(msgs) < c.max && ctx.Err() == nil {
		remaining := deadline.Sub(c.clock())
		if remaining <= 0 {
			return msgs, nil
		}

		wait := min(
			c.linger,
			remaining,
		)
		if wait < time.Millisecond {
			wait = time.Millisecond
		}

		more, err := c.collect(
			c.max-len(msgs),
			jetstream.FetchMaxWait(wait),
		)
		msgs = append(
			msgs,
			more...,
		)
		if err != nil {
			return msgs, err
		}
	}

	return msgs, nil
}

func (c *Consumer) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}

	return c.now()
}

func (c *Consumer) collect(
	count int,
	opt jetstream.FetchOpt,
) ([]jetstream.Msg, error) {
	fetched, err := c.fetcher.Fetch(
		count,
		c.fetchOpts(opt)...,
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
		if errors.Is(
			err,
			jetstream.ErrPinIDMismatch,
		) {
			log.Debug().
				Str(
					"pinned_group",
					c.group,
				).
				Int(
					"fetched",
					len(msgs),
				).
				Msg("batch fetch lost the pin, another pod holds the group")
			return msgs, nil
		}
		return msgs, err
	}

	return msgs, nil
}

func (c *Consumer) fetchOpts(opt jetstream.FetchOpt) []jetstream.FetchOpt {
	if c.group == "" {
		return []jetstream.FetchOpt{opt}
	}

	return []jetstream.FetchOpt{
		opt,
		jetstream.FetchPriorityGroup(c.group),
	}
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
