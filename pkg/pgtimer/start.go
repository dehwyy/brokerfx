package pgtimer

import (
	"context"
	"slices"

	"github.com/rs/zerolog/log"
)

func (p *Poller) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateIdle {
		return ErrStarted
	}
	if len(p.handlers) == 0 {
		return ErrNoHandlers
	}

	names := make(
		[]string,
		0,
		len(p.handlers),
	)
	for name := range p.handlers {
		names = append(
			names,
			name,
		)
	}
	slices.Sort(names)
	p.names = names

	loopCtx, cancel := context.WithCancel(context.Background())
	work, stopWork := context.WithCancel(context.Background())
	p.cancel = cancel
	p.work = work
	p.stopWork = stopWork
	p.state = stateRunning

	go p.loop(loopCtx)

	log.Info().
		Strs(
			"names",
			names,
		).
		Dur(
			"lease",
			p.settings.lease,
		).
		Dur(
			"handler_timeout",
			p.settings.handlerTimeout,
		).
		Dur(
			"poll",
			p.settings.poll,
		).
		Int(
			"batch",
			p.settings.batch,
		).
		Int(
			"concurrency",
			p.settings.concurrency,
		).
		Msg("pgtimer poller started")

	return nil
}
