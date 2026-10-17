package pgtimer

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

func (p *Poller) loop(ctx context.Context) {
	defer close(p.loopDone)

	for ctx.Err() == nil {
		reserved := p.reserve(ctx)
		if reserved == 0 {
			return
		}

		timers, err := p.claim(
			ctx,
			reserved,
		)
		p.release(reserved - len(timers))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn().
				Err(err).
				Msg("pgtimer claim failed")
			p.pause(ctx)
			continue
		}

		for _, timer := range timers {
			p.dispatch(timer)
		}

		if len(timers) == reserved {
			continue
		}
		p.pause(ctx)
	}
}

func (p *Poller) reserve(ctx context.Context) int {
	select {
	case <-ctx.Done():
		return 0
	case p.slots <- struct{}{}:
	}

	reserved := 1
	for reserved < p.settings.batch {
		select {
		case p.slots <- struct{}{}:
			reserved++
		default:
			return reserved
		}
	}

	return reserved
}

func (p *Poller) release(count int) {
	for range count {
		<-p.slots
	}
}

func (p *Poller) pause(ctx context.Context) {
	timer := time.NewTimer(p.settings.poll)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (p *Poller) dispatch(timer Timer) {
	p.fires.Add(1)
	go func() {
		defer p.fires.Done()
		defer p.release(1)
		p.fire(timer)
	}()
}
