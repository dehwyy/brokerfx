package outbox

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// OutboxRelay is a background worker that reads uncommitted outbox events from PostgreSQL
// and publishes them to NATS JetStream. It reacts to wakeup signals from the store
// for immediate processing and uses a fallback ticker as a safety net.
type OutboxRelay struct {
	store  *OutboxStore
	js     jetstream.JetStream
	logger zerolog.Logger
	config Config

	cancel context.CancelFunc
	done   chan struct{}
}

// NewRelay creates a new OutboxRelay.
func NewRelay(deps RelayDeps) *OutboxRelay {
	cfg := deps.Config
	if cfg.Mode == "" {
		cfg = DefaultConfig()
	}

	r := &OutboxRelay{
		store:  deps.Store,
		js:     deps.JS,
		logger: log.With().Str("component", "outbox-relay").Logger(),
		config: cfg,
		done:   make(chan struct{}),
	}

	return r
}

// Run starts the relay loop. It blocks until the context is cancelled.
// When the context is cancelled, the relay finishes processing the current batch
// before returning, ensuring graceful shutdown.
func (r *OutboxRelay) Run(ctx context.Context) {
	defer close(r.done)

	ticker := time.NewTicker(r.config.TickInterval)
	defer ticker.Stop()

	// Ticker for cleanup if mode is UpdateAfterSend
	var cleanupTicker *time.Ticker
	var cleanupChan <-chan time.Time
	if r.config.Mode == ModeUpdateAfterSend {
		cleanupTicker = time.NewTicker(5 * time.Minute)
		cleanupChan = cleanupTicker.C
		defer cleanupTicker.Stop()
	}

	r.logger.Info().
		Int("batch_size", r.config.BatchSize).
		Dur("tick_interval", r.config.TickInterval).
		Str("mode", string(r.config.Mode)).
		Msg("outbox relay started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Msg("outbox relay stopping, processing final batch")
			r.processBatch()
			r.logger.Info().Msg("outbox relay stopped")
			return
		case <-r.store.WakeupChan():
			r.processBatch()
		case <-ticker.C:
			r.processBatch()
		case <-cleanupChan:
			r.cleanupDone()
		}
	}
}

// Done returns a channel that is closed when the relay has fully stopped.
func (r *OutboxRelay) Done() <-chan struct{} {
	return r.done
}

func (r *OutboxRelay) cleanupDone() {
	db := r.store.DB()
	threshold := time.Now().Add(-r.config.DeleteOlderThan)
	res := db.Where("state = ? AND updated_at < ?", StateDone, threshold).Delete(&OutboxEvent{})
	if res.Error != nil {
		r.logger.Error().Err(res.Error).Msg("failed to clean up DONE outbox events")
	} else if res.RowsAffected > 0 {
		r.logger.Debug().Int64("deleted_count", res.RowsAffected).Msg("cleaned up DONE outbox events")
	}
}

// processBatch fetches a batch of outbox events, marks them as IN_FLIGHT,
// publishes each to NATS JetStream asynchronously, waits for ACKs, and updates the states.
func (r *OutboxRelay) processBatch() {
	db := r.store.DB()

	var events []OutboxEvent

	// Open a transaction to lock and update the rows to IN_FLIGHT
	err := db.Transaction(func(tx *gorm.DB) error {
		// Find events that are PENDING, or IN_FLIGHT but stalled for > 5 mins
		stalledThreshold := time.Now().Add(-5 * time.Minute)

		err := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Where("state = ?", StatePending).
			Or("state = ? AND updated_at < ?", StateInFlight, stalledThreshold).
			Order("created_at ASC").
			// Preload("Retries").
			Limit(r.config.BatchSize).
			Find(&events).Error

		if err != nil {
			return err
		}

		if len(events) == 0 {
			return nil
		}

		// Collect IDs to mark as IN_FLIGHT
		ids := make([]string, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}

		// Update state to IN_FLIGHT
		return tx.Model(&OutboxEvent{}).
			Where("id IN ?", ids).
			Update("state", StateInFlight).Error
	})

	if err != nil {
		r.logger.Error().Err(err).Msg("failed to fetch and lock outbox events")
		return
	}

	if len(events) == 0 {
		return
	}

	r.logger.Debug().Int("count", len(events)).Msg("processing outbox batch")

	type pubResult struct {
		id  string
		err error
	}

	results := make(chan pubResult, len(events))

	for _, ev := range events {
		// Capture loop variable
		event := ev

		// PublishAsync returns a PubAckFuture
		future, err := r.js.PublishAsync(event.Topic, event.Payload)
		if err != nil {
			results <- pubResult{id: event.ID, err: err}
			continue
		}

		// Wait for the future in a goroutine
		go func(id string, f jetstream.PubAckFuture) {
			select {
			case <-f.Ok():
				results <- pubResult{id: id, err: nil}
			case err := <-f.Err():
				results <- pubResult{id: id, err: err}
			case <-time.After(30 * time.Second): // timeout safety
				results <- pubResult{id: id, err: context.DeadlineExceeded}
			}
		}(event.ID, future)
	}

	var successIDs []string
	var failedOutcomes []pubResult

	for i := 0; i < len(events); i++ {
		res := <-results
		if res.err != nil {
			r.logger.Error().
				Err(res.err).
				Str("event_id", res.id).
				Msg("failed to publish outbox event to NATS")
			failedOutcomes = append(failedOutcomes, res)
		} else {
			successIDs = append(successIDs, res.id)
		}
	}

	// Process outcomes
	if len(failedOutcomes) > 0 {
		failedIDs := make([]string, len(failedOutcomes))
		retries := make([]OutboxRetry, len(failedOutcomes))
		for i, outcome := range failedOutcomes {
			failedIDs[i] = outcome.id
			retries[i] = OutboxRetry{
				EventID: outcome.id,
				Error:   outcome.err.Error(),
			}
		}

		log.Debug().
			Int("count", len(failedIDs)).
			Msg("reverting failed events to PENDING")
		// Revert failed IDs back to PENDING. Use plain DB without tx to release pool
		if err := db.Model(&OutboxEvent{}).Where("id IN ?", failedIDs).Update("state", StatePending).Error; err != nil {
			r.logger.Error().Err(err).Int("count", len(failedIDs)).Msg("failed to revert failed events to PENDING")
		}

		if err := db.Create(&retries).Error; err != nil {
			r.logger.Error().Err(err).Int("count", len(retries)).Msg("failed to save to retries table")
		}
	}

	if len(successIDs) > 0 {
		log.Debug().
			Int("count", len(successIDs)).
			Msg("marking published events as DONE (or deleting)")
		switch r.config.Mode {
		case ModeUpdateAfterSend:
			if err := db.Model(&OutboxEvent{}).Where("id IN ?", successIDs).Update("state", StateDone).Error; err != nil {
				r.logger.Error().Err(err).Int("count", len(successIDs)).Msg("failed to mark published events as DONE")
			}
		case ModeDeleteAfterSend:
			fallthrough
		default:
			if err := db.Where("id IN ?", successIDs).Delete(&OutboxEvent{}).Error; err != nil {
				r.logger.Error().Err(err).Int("count", len(successIDs)).Msg("failed to delete published outbox events")
			}
		}
	}

	r.logger.Info().
		Int("published", len(successIDs)).
		Int("failed", len(failedOutcomes)).
		Msg("outbox batch processed")
}

// AutoMigrate creates or updates the outbox_events table schema.
// Should be called during application startup.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&OutboxEvent{}, &OutboxRetry{})
}
