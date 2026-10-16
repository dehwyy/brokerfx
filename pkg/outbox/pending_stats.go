package outbox

import (
	"context"
	"database/sql"
	"time"
)

const pendingGaugeCap = 10000

const pendingStatsQuery = `SELECT
	(SELECT count(*) FROM (SELECT 1 FROM outbox_events WHERE state <> 'DONE' AND state = ? LIMIT ?) p) AS pending,
	(SELECT created_at FROM outbox_events WHERE state <> 'DONE' AND state = ? ORDER BY created_at LIMIT 1) AS oldest,
	now() AS db_now`

func (s *OutboxStore) pendingStats(ctx context.Context) (PendingStats, error) {
	var row struct {
		Pending int64
		Oldest  sql.NullTime
		DBNow   time.Time
	}
	if err := s.db.WithContext(ctx).Raw(
		pendingStatsQuery,
		StatePending,
		pendingGaugeCap+1,
		StatePending,
	).Scan(&row).Error; err != nil {
		return PendingStats{}, err
	}

	stats := PendingStats{
		Pending: row.Pending,
	}
	if row.Pending > pendingGaugeCap {
		stats.Pending = pendingGaugeCap
		stats.Capped = true
	}
	if row.Oldest.Valid && row.DBNow.After(row.Oldest.Time) {
		stats.OldestPendingAge = row.DBNow.Sub(row.Oldest.Time)
	}

	return stats, nil
}
