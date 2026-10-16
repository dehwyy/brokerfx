package outbox

import (
	"context"
	"time"

	"gorm.io/gorm"
)

const (
	cleanupBatchQuery = `DELETE FROM outbox_events WHERE ctid IN (SELECT ctid FROM outbox_events WHERE state = ? AND updated_at < ? LIMIT ?) AND state = ? AND updated_at < ?`
	cleanupBatchPause = 100 * time.Millisecond
)

func (r *OutboxRelay) cleanup(ctx context.Context) {
	if r.config.CleanupBatch <= 0 {
		r.cleanupDone()
		return
	}

	if !r.cleaning.CompareAndSwap(
		false,
		true,
	) {
		return
	}

	r.cleanupWG.Add(1)
	go func() {
		defer r.cleanupWG.Done()
		defer r.cleaning.Store(false)
		r.cleanupBatched(ctx)
	}()
}

func (r *OutboxRelay) cleanupBatched(ctx context.Context) {
	db := r.store.DB()

	r.cleanupState(
		ctx,
		db,
		StateDone,
		time.Now().Add(-r.config.DeleteOlderThan),
	)

	if r.config.RetainParked <= 0 {
		return
	}

	r.cleanupState(
		ctx,
		db,
		StateParked,
		time.Now().Add(-r.config.RetainParked),
	)
}

func (r *OutboxRelay) cleanupState(
	ctx context.Context,
	db *gorm.DB,
	state OutboxState,
	before time.Time,
) {
	var total int64
	defer func() {
		if total > 0 {
			r.logger.Debug().
				Str("state", state.String()).
				Int64("deleted_count", total).
				Msg("cleaned up outbox events in batches")
		}
	}()

	for {
		deletion := db.WithContext(ctx).Exec(
			cleanupBatchQuery,
			state,
			before,
			r.config.CleanupBatch,
			state,
			before,
		)
		if deletion.Error != nil {
			if ctx.Err() == nil {
				r.logger.Error().
					Err(deletion.Error).
					Str("state", state.String()).
					Msg("failed to clean up outbox events in batches")
			}
			return
		}

		total += deletion.RowsAffected
		if deletion.RowsAffected < int64(r.config.CleanupBatch) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cleanupBatchPause):
		}
	}
}
