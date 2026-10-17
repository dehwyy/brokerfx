package outbox

import (
	"context"
	"sort"
	"time"

	"gorm.io/gorm"
)

const (
	txDrainMaxFailures       = 3
	txDrainUnknownRetryFloor = 5 * time.Second
)

var txDrainSelect = `WITH c AS MATERIALIZED (
	SELECT id FROM outbox_events
	WHERE state <> '` + StateDone.String() + `' AND state = '` + StatePending.String() + `'
		AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
	ORDER BY created_at
	LIMIT ?
	FOR UPDATE SKIP LOCKED
)
`

const txDrainReturning = `
RETURNING o.id, o.topic, o.payload, o.state, o.attempts, o.last_error, o.next_attempt_at, o.headers, o.created_at, o.updated_at`

var txDrainDeleteQuery = txDrainSelect + `DELETE FROM outbox_events o USING c WHERE o.id = c.id` + txDrainReturning

var txDrainDoneQuery = txDrainSelect + `UPDATE outbox_events o SET state = '` + StateDone.String() + `', updated_at = ? FROM c WHERE o.id = c.id` + txDrainReturning

type drainParked struct {
	id      string
	subject string
	err     string
}

type drainResult struct {
	claimed   int
	published int
	failed    int
	parked    []drainParked
}

func (r *OutboxRelay) txDrainActive(ctx context.Context) bool {
	if !r.config.TxDrain || r.drainOff {
		return false
	}

	caps := r.ensureSchemaCaps(context.WithoutCancel(ctx))
	if !caps.V2 {
		return false
	}

	if r.config.Mode != ModeUpdateAfterSend && !caps.DrainSafe {
		r.drainOff = true
		r.logger.Error().Msg("outbox tx-drain off: outbox_events has NOT NULL columns without default that the relay cannot re-insert, using the claim path")
		return false
	}

	return true
}

func (r *OutboxRelay) processTxDrain(ctx context.Context) {
	work := context.WithoutCancel(ctx)
	for {
		if r.Paused() {
			return
		}

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

		caps := r.ensureSchemaCaps(work)
		result, err := r.drainBatch(
			work,
			caps,
		)
		if err != nil {
			r.drainFailures++
			r.logger.Error().
				Err(err).
				Int("consecutive_failures", r.drainFailures).
				Msg("outbox tx-drain batch rolled back, rows stay PENDING")
			if r.drainFailures >= txDrainMaxFailures {
				r.drainOff = true
				r.logger.Error().
					Int("consecutive_failures", r.drainFailures).
					Msg("outbox tx-drain off until restart, using the claim path")
			}
			return
		}
		r.drainFailures = 0

		r.reportDrain(result)

		more := result.claimed >= r.config.BatchSize && result.failed == 0
		if !more || ctx.Err() != nil {
			return
		}
	}
}

func (r *OutboxRelay) drainBatch(
	ctx context.Context,
	caps schemaCaps,
) (drainResult, error) {
	var result drainResult

	err := r.store.DB().
		WithContext(ctx).
		Transaction(
			func(tx *gorm.DB) error {
				result = drainResult{}

				now := time.Now()
				rows, err := r.drainClaim(
					tx,
					now,
				)
				if err != nil {
					return err
				}
				if len(rows) == 0 {
					return nil
				}
				result.claimed = len(rows)

				sort.SliceStable(
					rows,
					func(i, j int) bool {
						return rows[i].CreatedAt.Before(rows[j].CreatedAt)
					},
				)

				outcomes := r.drainPublish(
					ctx,
					r.decodeRows(rows),
				)

				return r.drainSettle(
					tx,
					caps,
					rows,
					outcomes,
					&result,
				)
			},
		)

	return result, err
}

func (r *OutboxRelay) drainClaim(
	tx *gorm.DB,
	now time.Time,
) ([]outboxEventRow, error) {
	var rows []outboxEventRow
	if r.config.Mode == ModeUpdateAfterSend {
		err := tx.Raw(
			txDrainDoneQuery,
			now,
			r.config.BatchSize,
			now,
		).Scan(&rows).Error

		return rows, err
	}

	err := tx.Raw(
		txDrainDeleteQuery,
		now,
		r.config.BatchSize,
	).Scan(&rows).Error

	return rows, err
}

func (r *OutboxRelay) drainSettle(
	tx *gorm.DB,
	caps schemaCaps,
	rows []outboxEventRow,
	outcomes []pubResult,
	result *drainResult,
) error {
	now := time.Now()
	var restored []outboxEventRow
	var retries []OutboxRetry
	for i, outcome := range outcomes {
		if outcome.err == nil {
			result.published++
			continue
		}

		result.failed++
		row := rows[i]
		errMsg := outcome.err.Error()
		row.LastError = &errMsg
		row.State = StatePending
		retryAt := now.Add(max(
			r.config.TxDrainPublishTimeout,
			txDrainUnknownRetryFloor,
		))
		row.NextAttemptAt = &retryAt
		row.UpdatedAt = now
		if !unknownOutcome(outcome.err) {
			row.Attempts++
			at := now.Add(nextAttemptDelay(
				row.Attempts,
				r.config.RetryBackoffBase,
				r.config.RetryBackoffMax,
			))
			row.NextAttemptAt = &at
			if r.config.MaxAttempts > 0 && row.Attempts >= r.config.MaxAttempts {
				row.State = StateParked
				row.NextAttemptAt = nil
				result.parked = append(
					result.parked,
					drainParked{
						id:      row.ID,
						subject: row.Topic,
						err:     errMsg,
					},
				)
			}
		}

		r.logger.Error().
			Err(outcome.err).
			Str("event_id", row.ID).
			Bool("unknown_outcome", unknownOutcome(outcome.err)).
			Msg("failed to publish outbox event in tx-drain")

		restored = append(
			restored,
			row,
		)
		retries = append(
			retries,
			OutboxRetry{
				EventID: row.ID,
				Error:   errMsg,
			},
		)
	}

	if len(restored) > 0 {
		if err := r.drainRestore(
			tx,
			restored,
		); err != nil {
			return err
		}
	}

	if len(retries) > 0 && caps.Retries {
		if err := tx.Create(&retries).Error; err != nil {
			return err
		}
	}

	return nil
}

func (r *OutboxRelay) drainRestore(
	tx *gorm.DB,
	restored []outboxEventRow,
) error {
	if r.config.Mode != ModeUpdateAfterSend {
		return tx.CreateInBatches(
			&restored,
			saveMessagesChunk,
		).Error
	}

	for _, row := range restored {
		if err := tx.Model(&outboxEventRow{}).
			Where(
				"id = ?",
				row.ID,
			).
			Select(
				"state",
				"attempts",
				"last_error",
				"next_attempt_at",
				"updated_at",
			).
			Updates(row).Error; err != nil {
			return err
		}
	}

	return nil
}

func (r *OutboxRelay) reportDrain(result drainResult) {
	if r.observer == nil {
		return
	}

	if result.published > 0 {
		r.observer.OnPublished(result.published)
	}
	if result.failed > 0 {
		r.observer.OnFailed(result.failed)
	}
	for _, parked := range result.parked {
		r.observer.OnParked(
			parked.id,
			parked.subject,
			parked.err,
		)
	}
}
