package outbox

import (
	"context"
	"sort"
	"time"
)

const claimBatchQuery = `UPDATE outbox_events SET state = ?, updated_at = ? WHERE id IN (SELECT id FROM outbox_events WHERE (state = ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (state = ? AND updated_at < ?) ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED) RETURNING id, topic, payload, headers, attempts, created_at`

func (r *OutboxRelay) claimBatch(ctx context.Context) ([]relayEvent, error) {
	now := time.Now()

	var rows []outboxEventRow
	if err := r.store.DB().
		WithContext(ctx).
		Raw(
			claimBatchQuery,
			StateInFlight,
			now,
			StatePending,
			now,
			StateInFlight,
			now.Add(-r.config.StallThreshold),
			r.config.BatchSize,
		).
		Scan(&rows).Error; err != nil {
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
