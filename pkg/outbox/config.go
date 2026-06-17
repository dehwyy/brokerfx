package outbox

import (
	"time"

	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
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

	// StallThreshold is how long an IN_FLIGHT row may sit before the relay re-picks
	// and re-publishes it (covers a crash between NATS ack and the DB state update).
	//
	// INVARIANT: the stream Duplicates window MUST be >= 2*StallThreshold. The relay
	// re-publishes with Nats-Msg-Id = row.ID; if the dedup window were not at least
	// twice the stall threshold, the re-publish could land exactly as the server-side
	// dedup record expires, so the duplicate would NOT be suppressed and the consumer
	// would process the event twice. With 2x headroom the re-publish always lands well
	// inside a live dedup window. Defaults: StallThreshold 5m, Duplicates 15m.
	StallThreshold time.Duration
}

func DefaultConfig() Config {
	return Config{
		Mode:            ModeUpdateAfterSend,
		BatchSize:       100,
		TickInterval:    2 * time.Second,
		DeleteOlderThan: 1 * time.Hour, // relevant only for UpdateAfterSend mode
		StallThreshold:  5 * time.Minute,
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

	Store    *OutboxStore
	Producer Producer
	Config   Config `optional:"true"`
}
