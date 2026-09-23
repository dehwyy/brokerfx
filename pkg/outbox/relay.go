package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"net/textproto"
	"strings"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type relayEvent struct {
	ID       string
	Topic    string
	Payload  []byte
	Headers  map[string]string
	Attempts int
}

type pubResult struct {
	id  string
	err error
}

func nextAttemptDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if max > 0 && delay >= max {
			return max
		}
		delay *= 2
	}

	if max > 0 && delay > max {
		return max
	}

	return delay
}

func isTransportError(err error) bool {
	return errors.Is(err, natsgo.ErrNoServers) ||
		errors.Is(err, natsgo.ErrConnectionClosed) ||
		errors.Is(err, natsgo.ErrDisconnected) ||
		errors.Is(err, natsgo.ErrConnectionReconnecting)
}

// OutboxRelay is a background worker that reads uncommitted outbox events from PostgreSQL
// and publishes them via a Producer. It reacts to wakeup signals from the store
// for immediate processing and uses a fallback ticker as a safety net.
type OutboxRelay struct {
	store    *OutboxStore
	producer Producer
	signer   Signer
	observer Observer
	logger   zerolog.Logger
	config   Config

	schemaCapsMu       sync.Mutex
	schemaCaps         schemaCaps
	schemaCapsResolved bool
	detectSchemaFn     func(context.Context, *gorm.DB) (schemaCaps, error)

	cancel context.CancelFunc
	done   chan struct{}
}

// NewRelay creates a new OutboxRelay.
func NewRelay(deps RelayDeps) *OutboxRelay {
	cfg := deps.Config
	if cfg.Mode == "" {
		cfg = DefaultConfig()
	}
	// Guard the stall invariant: a zero threshold would re-pick every IN_FLIGHT row
	// on each tick. Fall back to the default so callers that set only Mode are safe.
	if cfg.StallThreshold <= 0 {
		cfg.StallThreshold = DefaultConfig().StallThreshold
	}

	r := &OutboxRelay{
		store:          deps.Store,
		producer:       deps.Producer,
		signer:         deps.Signer,
		observer:       deps.Observer,
		logger:         log.With().Str("component", "outbox-relay").Logger(),
		config:         cfg,
		detectSchemaFn: detectSchema,
		done:           make(chan struct{}),
	}

	return r
}

// Run starts the relay loop. It blocks until the context is cancelled.
// When the context is cancelled, the relay finishes processing the current batch
// before returning, ensuring graceful shutdown.
func (r *OutboxRelay) Run(ctx context.Context) {
	defer close(r.done)

	r.ensureSchemaCaps(ctx)

	ticker := time.NewTicker(r.config.TickInterval)
	defer ticker.Stop()

	cleanupInterval := r.config.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = 5 * time.Minute
	}
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()
	cleanupChan := cleanupTicker.C

	var statsTicker *time.Ticker
	var statsChan <-chan time.Time
	if r.observer != nil && r.config.StatsInterval > 0 {
		statsTicker = time.NewTicker(r.config.StatsInterval)
		statsChan = statsTicker.C
		defer statsTicker.Stop()
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
			r.processBatch(ctx)
			r.logger.Info().Msg("outbox relay stopped")
			return
		case <-r.store.WakeupChan():
			r.processBatch(ctx)
		case <-ticker.C:
			r.processBatch(ctx)
		case <-cleanupChan:
			r.cleanupDone()
		case <-statsChan:
			r.sendStats(ctx)
		}
	}
}

func (r *OutboxRelay) sendStats(ctx context.Context) {
	if r.observer == nil {
		return
	}

	stats, err := r.store.Stats(ctx)
	if err != nil {
		r.logger.Error().Err(err).Msg("failed to compute outbox stats")
		return
	}

	r.observer.OnStats(stats)
}

// Done returns a channel that is closed when the relay has fully stopped.
func (r *OutboxRelay) Done() <-chan struct{} {
	return r.done
}

func (r *OutboxRelay) ensureSchemaCaps(ctx context.Context) schemaCaps {
	r.schemaCapsMu.Lock()
	defer r.schemaCapsMu.Unlock()

	if r.schemaCapsResolved {
		return r.schemaCaps
	}

	caps, err := r.detectSchemaFn(ctx, r.store.DB())
	if err != nil {
		r.logger.Error().Err(err).Msg("failed to detect outbox schema, retrying on next batch")
		return schemaCaps{}
	}

	r.schemaCaps = caps
	r.schemaCapsResolved = true

	if !caps.Retries {
		r.logger.Warn().Msg("outbox_retries missing")
	}

	return r.schemaCaps
}

func (r *OutboxRelay) lockRowsLegacy(query *gorm.DB) ([]relayEvent, error) {
	var rows []OutboxEvent
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}

	events := make([]relayEvent, len(rows))
	for i, row := range rows {
		events[i] = relayEvent{
			ID:      row.ID,
			Topic:   row.Topic,
			Payload: row.Payload,
		}
	}

	return events, nil
}

func (r *OutboxRelay) lockRowsV2(query *gorm.DB) ([]relayEvent, error) {
	var rows []outboxEventRow
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}

	events := make([]relayEvent, len(rows))
	for i, row := range rows {
		event := relayEvent{
			ID:       row.ID,
			Topic:    row.Topic,
			Payload:  row.Payload,
			Attempts: row.Attempts,
		}

		if len(row.Headers) > 0 {
			var headers map[string]string
			if err := json.Unmarshal(row.Headers, &headers); err != nil {
				r.logger.Error().Err(err).Str("event_id", row.ID).Msg("failed to decode outbox event headers")
			} else {
				event.Headers = headers
			}
		}

		events[i] = event
	}

	return events, nil
}

func (r *OutboxRelay) revertFailedV2(db *gorm.DB, events []relayEvent, failedOutcomes []pubResult) {
	eventByID := make(map[string]relayEvent, len(events))
	for _, ev := range events {
		eventByID[ev.ID] = ev
	}

	for _, outcome := range failedOutcomes {
		ev := eventByID[outcome.id]
		attempts := ev.Attempts

		var nextAttemptAt *time.Time
		if !isTransportError(outcome.err) {
			attempts++
			at := time.Now().Add(nextAttemptDelay(attempts, r.config.RetryBackoffBase, r.config.RetryBackoffMax))
			nextAttemptAt = &at
		}

		errMsg := outcome.err.Error()
		state := StatePending
		parked := r.config.MaxAttempts > 0 && attempts >= r.config.MaxAttempts

		if parked {
			state = StateParked
			nextAttemptAt = nil

			r.logger.Error().
				Str("event_id", outcome.id).
				Str("subject", ev.Topic).
				Err(outcome.err).
				Msg("outbox event parked")
		}

		update := outboxEventRow{
			State:         state,
			Attempts:      attempts,
			LastError:     &errMsg,
			NextAttemptAt: nextAttemptAt,
		}

		if err := db.Model(&outboxEventRow{}).
			Where("id = ?", outcome.id).
			Select("state", "attempts", "last_error", "next_attempt_at").
			Updates(update).Error; err != nil {
			r.logger.Error().Err(err).Str("event_id", outcome.id).Msg("failed to record retry attempt")
		} else if parked && r.observer != nil {
			r.observer.OnParked(outcome.id, ev.Topic, errMsg)
		}
	}
}

func (r *OutboxRelay) cleanupDone() {
	db := r.store.DB()

	doneThreshold := time.Now().Add(-r.config.DeleteOlderThan)
	doneRes := db.Where("state = ? AND updated_at < ?", StateDone, doneThreshold).Delete(&OutboxEvent{})
	if doneRes.Error != nil {
		r.logger.Error().Err(doneRes.Error).Msg("failed to clean up DONE outbox events")
	} else if doneRes.RowsAffected > 0 {
		r.logger.Debug().Int64("deleted_count", doneRes.RowsAffected).Msg("cleaned up DONE outbox events")
	}

	if r.config.RetainParked <= 0 {
		return
	}

	parkedThreshold := time.Now().Add(-r.config.RetainParked)
	parkedRes := db.Where("state = ? AND updated_at < ?", StateParked, parkedThreshold).Delete(&OutboxEvent{})
	if parkedRes.Error != nil {
		r.logger.Error().Err(parkedRes.Error).Msg("failed to clean up PARKED outbox events")
	} else if parkedRes.RowsAffected > 0 {
		r.logger.Debug().Int64("deleted_count", parkedRes.RowsAffected).Msg("cleaned up PARKED outbox events")
	}
}

// processBatch fetches a batch of outbox events, marks them as IN_FLIGHT,
// publishes each via the Producer concurrently, and updates the states.
func (r *OutboxRelay) processBatch(ctx context.Context) {
	db := r.store.DB()
	caps := r.ensureSchemaCaps(ctx)

	var events []relayEvent

	err := db.Transaction(func(tx *gorm.DB) error {
		stalledThreshold := time.Now().Add(-r.config.StallThreshold)

		lockedBase := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Order("created_at ASC").
			Limit(r.config.BatchSize)

		var locked *gorm.DB
		if caps.V2 {
			locked = lockedBase.Where(
				"(state = ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (state = ? AND updated_at < ?)",
				StatePending, time.Now(),
				StateInFlight, stalledThreshold,
			)
		} else {
			locked = lockedBase.
				Where("state = ?", StatePending).
				Or("state = ? AND updated_at < ?", StateInFlight, stalledThreshold)
		}

		var err error
		if caps.V2 {
			events, err = r.lockRowsV2(locked)
		} else {
			events, err = r.lockRowsLegacy(locked)
		}
		if err != nil {
			return err
		}

		if len(events) == 0 {
			return nil
		}

		ids := make([]string, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}

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

	kvLatestIndex := make(map[string]int)
	for i, ev := range events {
		if strings.HasPrefix(ev.Topic, kvSubjectPrefix) {
			kvLatestIndex[ev.Topic] = i
		}
	}

	results := make(chan pubResult, len(events))

	for i, ev := range events {
		event := ev
		index := i

		if latest, ok := kvLatestIndex[event.Topic]; ok && latest != index {
			results <- pubResult{id: event.ID}
			continue
		}

		go func() {
			headers := make(map[string]string, len(event.Headers)+1)
			for k, v := range event.Headers {
				if textproto.CanonicalMIMEHeaderKey(k) == jetstream.MsgIDHeader {
					continue
				}
				headers[k] = v
			}
			headers[jetstream.MsgIDHeader] = event.ID

			if err := applySignature(r.signer, event.Topic, headers, event.Payload); err != nil {
				results <- pubResult{
					id:  event.ID,
					err: err,
				}
				return
			}

			producerEvent := ProducerEvent{
				Subject: event.Topic,
				Headers: headers,
				Payload: event.Payload,
			}

			pubErr := r.producer.Produce(ctx, producerEvent)
			results <- pubResult{
				id:  event.ID,
				err: pubErr,
			}
		}()
	}

	var successIDs []string
	var failedOutcomes []pubResult

	for i := 0; i < len(events); i++ {
		res := <-results
		if res.err != nil {
			r.logger.Error().
				Err(res.err).
				Str("event_id", res.id).
				Msg("failed to publish outbox event")
			failedOutcomes = append(failedOutcomes, res)
		} else {
			successIDs = append(successIDs, res.id)
		}
	}

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

		if caps.V2 {
			r.revertFailedV2(db, events, failedOutcomes)
		} else if err := db.Model(&OutboxEvent{}).Where("id IN ?", failedIDs).Update("state", StatePending).Error; err != nil {
			r.logger.Error().Err(err).Int("count", len(failedIDs)).Msg("failed to revert failed events to PENDING")
		}

		if caps := r.ensureSchemaCaps(ctx); caps.Retries {
			if err := db.Create(&retries).Error; err != nil {
				r.logger.Error().Err(err).Int("count", len(retries)).Msg("failed to save to retries table")
			}
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

	if r.observer != nil {
		if len(successIDs) > 0 {
			r.observer.OnPublished(len(successIDs))
		}
		if len(failedOutcomes) > 0 {
			r.observer.OnFailed(len(failedOutcomes))
		}
	}

	r.logger.Info().
		Int("published", len(successIDs)).
		Int("failed", len(failedOutcomes)).
		Msg("outbox batch processed")
}
