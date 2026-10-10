package cleanup

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

func (c *Cleaner) Start(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		return nil
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.stopped = make(chan struct{})

	go c.loop(
		loopCtx,
		c.stopped,
	)

	log.Info().
		Dur("retention", c.retention).
		Dur("interval", c.interval).
		Int("batch_limit", c.batchLimit).
		Msg("processed_messages cleanup started")

	return nil
}

func (c *Cleaner) loop(
	ctx context.Context,
	stopped chan struct{},
) {
	defer close(stopped)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pass(ctx)
		}
	}
}

func (c *Cleaner) pass(ctx context.Context) {
	deleted, err := c.RunOnce(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Warn().
			Err(err).
			Int64("deleted", deleted).
			Msg("processed_messages cleanup pass failed")
		if c.onError != nil {
			c.onError(err)
		}
		return
	}

	if deleted > 0 {
		log.Info().
			Int64("deleted", deleted).
			Msg("processed_messages cleanup pass done")
	}
}
