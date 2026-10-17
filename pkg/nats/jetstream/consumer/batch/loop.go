package batch

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

const errorPause = time.Second

func (c *Consumer) start() {
	c.loopCtx, c.cancel = context.WithCancel(context.Background())
	c.stopped = make(chan struct{})

	for range c.pipeline {
		c.loops.Add(1)
		go func() {
			defer c.loops.Done()
			c.loop(c.loopCtx)
		}()
	}

	go func() {
		c.loops.Wait()
		close(c.stopped)
	}()
}

func (c *Consumer) loop(ctx context.Context) {
	idle := true
	for ctx.Err() == nil {
		started := time.Now()
		msgs, err := c.fetchBatch(
			ctx,
			idle,
		)
		c.watch().OnFetch(FetchEvent{
			Messages: len(msgs),
			Max:      c.max,
			Duration: time.Since(started),
		})
		if err != nil && isFatalFetchErr(err) {
			c.nakAll(msgs)
			c.fatal(err)
			return
		}
		if ctx.Err() != nil {
			c.nakAll(msgs)
			return
		}
		if err != nil {
			log.Warn().
				Err(err).
				Int(
					"fetched",
					len(msgs),
				).
				Msg("batch fetch failed")
			if len(msgs) == 0 {
				pause(
					ctx,
					errorPause,
				)
				continue
			}
		}

		idle = len(msgs) < c.max
		if len(msgs) == 0 {
			continue
		}

		c.processBatch(
			context.WithoutCancel(ctx),
			msgs,
		)
	}
}

func (c *Consumer) fatal(err error) {
	log.Error().
		Err(err).
		Msg("batch consumer stopped: consumer is gone")

	c.fatalOnce.Do(func() {
		if c.onFatal != nil {
			c.onFatal(err)
		}
	})
}

func pause(
	ctx context.Context,
	duration time.Duration,
) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
