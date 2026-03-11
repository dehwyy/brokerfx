package outbox

import (
	"context"
	"reflect"

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
