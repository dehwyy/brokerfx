package pgtimer

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const removeTimeout = 5 * time.Second

func (p *Poller) fire(timer Timer) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(
		p.work,
		p.settings.handlerTimeout,
	)
	err := call(
		ctx,
		p.handlers[timer.Name],
		timer,
	)
	cancel()

	deleted := false
	if err == nil {
		deleted, err = p.remove(timer)
	}
	if err != nil {
		log.Warn().
			Err(err).
			Str(
				"name",
				timer.Name,
			).
			Str(
				"key",
				timer.Key,
			).
			Time(
				"lease_due_at",
				timer.LeaseDueAt,
			).
			Msg("pgtimer fire failed, timer fires again after the lease")
	}

	p.observer.OnFire(FireEvent{
		Name:     timer.Name,
		Lag:      timer.ClaimedAt.Sub(timer.DueAt),
		Duration: time.Since(started),
		Deleted:  deleted,
		Err:      err,
	})
}

func call(
	ctx context.Context,
	handler HandlerFunc,
	timer Timer,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%w: %v",
				ErrHandlerPanic,
				recovered,
			)
		}
	}()

	return handler(
		ctx,
		timer,
	)
}

func (p *Poller) remove(timer Timer) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		removeTimeout,
	)
	defer cancel()

	result := p.db.WithContext(ctx).
		Exec(
			p.queries.remove,
			timer.Key,
			timer.Name,
			timer.Attempt,
			timer.LeaseDueAt,
		)

	return result.RowsAffected > 0, result.Error
}
