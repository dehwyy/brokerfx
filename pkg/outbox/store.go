package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/producer"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

// OutboxStore manages outbox event persistence and provides a wakeup mechanism
// for the relay worker. The wakeup channel allows immediate processing after
// a business transaction commits, without waiting for the fallback ticker.
type OutboxStore struct {
	db         *gorm.DB
	wakeupChan chan struct{}
	txmanager  txmanager.TxManager

	schemaCapsMu       sync.Mutex
	schemaCaps         schemaCaps
	schemaCapsResolved bool
}

// NewStore creates a new OutboxStore.
// The db parameter is used by the relay for reading/deleting events.
func NewStore(deps StoreDeps) *OutboxStore {
	return &OutboxStore{
		db:         deps.DB,
		wakeupChan: make(chan struct{}, 1),
		txmanager:  deps.TxManager,
	}
}

// Save inserts an OutboxEvent within the provided GORM transaction.
// The caller is responsible for managing the transaction lifecycle (begin/commit/rollback).
// This ensures atomicity: the event is persisted only if the business transaction succeeds.
//
// Usage:
//
//	tx := db.Begin()
//	// ... business logic ...
//	store.Save(tx, &outbox.OutboxEvent{Topic: "orders.created", Payload: protoBytes})
//	tx.Commit()
//	store.WakeupRelay()
func (s *OutboxStore) Save(ctx context.Context, event producer.Event) error {
	data, err := cryptov1.Encode(event.Data())
	if err != nil {
		log.Err(err).
			Any("event", reflect.ValueOf(event).Type().Name()).
			Msg("failed to encode event")
		return err
	}

	return s.txmanager.GetConnection(ctx).Create(&OutboxEvent{
		ID:      uuid.NewString(),
		Topic:   event.Subject(),
		Payload: data,
	}).Error
}

func (s *OutboxStore) SaveMessage(ctx context.Context, msg Message, opts ...MessageOption) error {
	if msg.err != nil {
		return msg.err
	}

	options, err := newMessageOptions(opts)
	if err != nil {
		return err
	}

	if len(msg.headers) > 0 {
		if options.headers == nil {
			options.headers = make(map[string]string, len(msg.headers))
		}
		for k, v := range msg.headers {
			options.headers[k] = v
		}
	}

	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}

	caps, err := s.ensureSchemaCaps(ctx)
	if err != nil {
		return err
	}

	if len(options.headers) > 0 && !caps.V2 {
		return ErrSchemaOutdated
	}

	if !caps.V2 {
		return s.txmanager.GetConnection(ctx).Create(&OutboxEvent{
			ID:      uuid.NewString(),
			Topic:   msg.Subject,
			Payload: payload,
			State:   StatePending,
		}).Error
	}

	row := outboxEventRow{
		ID:      uuid.NewString(),
		Topic:   msg.Subject,
		Payload: payload,
		State:   StatePending,
	}

	if len(options.headers) > 0 {
		headersJSON, err := json.Marshal(options.headers)
		if err != nil {
			return err
		}
		row.Headers = headersJSON
	}

	return s.txmanager.GetConnection(ctx).Create(&row).Error
}

func (s *OutboxStore) ensureSchemaCaps(ctx context.Context) (schemaCaps, error) {
	s.schemaCapsMu.Lock()
	defer s.schemaCapsMu.Unlock()

	if s.schemaCapsResolved {
		return s.schemaCaps, nil
	}

	caps, err := detectSchema(ctx, s.db)
	if err != nil {
		return schemaCaps{}, err
	}

	s.schemaCaps = caps
	s.schemaCapsResolved = true

	return s.schemaCaps, nil
}

// WakeupRelay sends a non-blocking signal to the relay worker, triggering
// immediate processing of queued events. Safe to call from any goroutine.
// If a signal is already pending, this is a no-op (the relay will process all available events).
func (s *OutboxStore) WakeupRelay() {
	select {
	case s.wakeupChan <- struct{}{}:
	default:
	}
}

// WakeupChan returns a read-only channel that the relay listens on for wakeup signals.
func (s *OutboxStore) WakeupChan() <-chan struct{} {
	return s.wakeupChan
}

// DB returns the underlying *gorm.DB for use by the relay.
func (s *OutboxStore) DB() *gorm.DB {
	return s.db
}

func (s *OutboxStore) RequeueParked(ctx context.Context, ids []string) (int64, error) {
	query := s.db.WithContext(ctx).
		Model(&outboxEventRow{}).
		Where("state = ?", StateParked)

	if len(ids) > 0 {
		query = query.Where("id IN ?", ids)
	}

	res := query.Updates(map[string]any{
		"state":           StatePending,
		"attempts":        0,
		"next_attempt_at": nil,
	})

	return res.RowsAffected, res.Error
}

func (s *OutboxStore) Stats(ctx context.Context) (Stats, error) {
	var counts []struct {
		State OutboxState
		Count int64
	}

	if err := s.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Select("state, count(*) as count").
		Group("state").
		Scan(&counts).Error; err != nil {
		return Stats{}, err
	}

	var stats Stats
	for _, c := range counts {
		switch c.State {
		case StatePending:
			stats.Pending = c.Count
		case StateInFlight:
			stats.InFlight = c.Count
		case StateDone:
			stats.Done = c.Count
		case StateParked:
			stats.Parked = c.Count
		}
	}

	var oldestPending sql.NullTime
	if err := s.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("state = ?", StatePending).
		Select("min(created_at)").
		Scan(&oldestPending).Error; err != nil {
		return Stats{}, err
	}

	if oldestPending.Valid {
		stats.OldestPendingAge = time.Since(oldestPending.Time)
	}

	return stats, nil
}
