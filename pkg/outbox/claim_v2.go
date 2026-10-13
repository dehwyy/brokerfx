package outbox

import (
	"context"
	"sort"
	"time"

	"gorm.io/gorm"
)

const reclaimBatchFactor = 10

const claimBatchV2Query = `WITH c AS MATERIALIZED (
	SELECT id FROM outbox_events
	WHERE state <> '` + string(StateDone) + `' AND state = '` + string(StatePending) + `'
		AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
	ORDER BY created_at
	LIMIT ?
	FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events o SET state = '` + string(StateInFlight) + `', updated_at = ?
FROM c WHERE o.id = c.id
RETURNING o.id, o.topic, o.payload, o.headers, o.attempts, o.created_at`

const reclaimStalledQuery = `WITH s AS MATERIALIZED (
	SELECT id FROM outbox_events
	WHERE state = '` + string(StateInFlight) + `' AND updated_at < ?
	ORDER BY updated_at
	LIMIT ?
	FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events o SET state = '` + string(StatePending) + `', updated_at = ?
FROM s WHERE o.id = s.id`

func reclaimDue(last, now time.Time, interval time.Duration) bool {
	if last.IsZero() || last.After(now) {
		return true
	}

	return now.Sub(last) >= interval
}

func (r *OutboxRelay) claimBatchV2(ctx context.Context) ([]relayEvent, error) {
	work := context.WithoutCancel(ctx)
	now := time.Now()

	if reclaimDue(
		r.lastReclaim,
		now,
		r.config.StallReclaimInterval,
	) {
		r.lastReclaim = now
		r.reclaimStalled(
			work,
			now,
		)
	}

	claimCtx, cancel := context.WithTimeout(
		work,
		r.config.ClaimTimeout,
	)
	defer cancel()

	var rows []outboxEventRow
	if err := r.store.DB().
		WithContext(work).
		Transaction(
			func(tx *gorm.DB) error {
				if err := tx.
					WithContext(claimCtx).
					Raw(
						claimBatchV2Query,
						now,
						r.config.BatchSize,
						now,
					).
					Scan(&rows).Error; err != nil {
					return err
				}

				return claimCtx.Err()
			},
		); err != nil {
		return nil, err
	}

	sort.SliceStable(
		rows,
		func(i, j int) bool {
			return rows[i].CreatedAt.Before(rows[j].CreatedAt)
		},
	)

	return r.decodeRows(rows), nil
}

func (r *OutboxRelay) reclaimStalled(ctx context.Context, now time.Time) {
	reclaimCtx, cancel := context.WithTimeout(
		ctx,
		r.config.ClaimTimeout,
	)
	defer cancel()

	reclaim := r.store.DB().
		WithContext(reclaimCtx).
		Exec(
			reclaimStalledQuery,
			now.Add(-r.config.StallThreshold),
			r.config.BatchSize*reclaimBatchFactor,
			now,
		)
	if reclaim.Error != nil {
		r.logger.Error().
			Err(reclaim.Error).
			Msg("failed to reclaim stalled outbox events")
		return
	}

	if reclaim.RowsAffected > 0 {
		r.logger.Warn().
			Int64(
				"count",
				reclaim.RowsAffected,
			).
			Msg("reclaimed stalled outbox events")
	}
}
