package pgtimer

import (
	"context"

	"github.com/rs/zerolog/log"
)

func (p *Poller) Stop(ctx context.Context) error {
	p.mu.Lock()
	if p.state != stateRunning {
		p.state = stateStopped
		p.mu.Unlock()
		return nil
	}
	p.state = stateStopped
	p.mu.Unlock()

	p.cancel()
	<-p.loopDone

	drained := make(chan struct{})
	go func() {
		p.fires.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		p.stopWork()
		log.Info().Msg("pgtimer poller stopped")
		return nil
	case <-ctx.Done():
		p.stopWork()
		log.Warn().
			Err(ctx.Err()).
			Msg("pgtimer poller stop deadline passed, unfinished timers fire again after the lease")
		return ctx.Err()
	}
}
