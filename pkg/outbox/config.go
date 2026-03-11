package outbox

import (
	"time"

	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
	"gorm.io/gorm"
)

type OutboxMode string

const (
	// DeleteAfterSend indicates that messages should be deleted from the database
	// immediately after a successful NATS publish acknowledgment.
	ModeDeleteAfterSend OutboxMode = "delete_after_send"

	// UpdateAfterSend indicates that messages should be marked as DONE
	// after a successful publish. Old DONE messages are cleaned up periodically.
	ModeUpdateAfterSend OutboxMode = "update_after_send"
)

// Config holds configuration for the Outbox module.
type Config struct {
	Mode            OutboxMode
	BatchSize       int
	TickInterval    time.Duration
	DeleteOlderThan time.Duration
}

func DefaultConfig() Config {
	return Config{
		Mode:            ModeUpdateAfterSend,
		BatchSize:       100,
		TickInterval:    2 * time.Second,
		DeleteOlderThan: 1 * time.Hour, // relevant only for UpdateAfterSend mode
	}
}

// StoreDeps defines the dependencies needed to construct the OutboxStore.
type StoreDeps struct {
	fx.In

	DB        *gorm.DB
	TxManager txmanager.TxManager
}

// RelayDeps defines the dependencies needed to construct the OutboxRelay worker.
type RelayDeps struct {
	fx.In

	Store  *OutboxStore
	JS     jetstream.JetStream
	Config Config `optional:"true"`
}
