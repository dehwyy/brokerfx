package pgtimer

import (
	"context"
	"time"
)

type claimedRow struct {
	TimerKey      string    `gorm:"column:timer_key"`
	TimerName     string    `gorm:"column:timer_name"`
	TimerAttempt  string    `gorm:"column:timer_attempt"`
	OriginalDueAt time.Time `gorm:"column:original_due_at"`
	LeaseDueAt    time.Time `gorm:"column:lease_due_at"`
	ClaimedAt     time.Time `gorm:"column:claimed_at"`
}

func (p *Poller) claim(
	ctx context.Context,
	limit int,
) ([]Timer, error) {
	started := time.Now()
	var rows []claimedRow
	err := p.db.WithContext(ctx).
		Raw(
			p.queries.claim,
			p.names,
			limit,
			p.settings.lease.Milliseconds(),
		).
		Scan(&rows).
		Error

	timers := make(
		[]Timer,
		0,
		len(rows),
	)
	if err == nil {
		for _, row := range rows {
			timers = append(
				timers,
				Timer{
					Key:        row.TimerKey,
					Name:       row.TimerName,
					Attempt:    row.TimerAttempt,
					DueAt:      row.OriginalDueAt,
					LeaseDueAt: row.LeaseDueAt,
					ClaimedAt:  row.ClaimedAt,
				},
			)
		}
	}

	p.observer.OnClaim(ClaimEvent{
		Limit:    limit,
		Claimed:  len(timers),
		Duration: time.Since(started),
		Err:      err,
	})

	return timers, err
}
