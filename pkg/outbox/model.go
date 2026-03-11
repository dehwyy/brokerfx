package outbox

import "time"

type OutboxState string

const (
	StatePending  OutboxState = "PENDING"
	StateInFlight OutboxState = "IN_FLIGHT"
	StateDone     OutboxState = "DONE"
)

// OutboxEvent is a GORM model representing an event queued for delivery to NATS JetStream.
// Events are inserted within business transactions and relayed asynchronously by the OutboxRelay.
type OutboxEvent struct {
	ID        string      `gorm:"primaryKey;default:gen_random_uuid()"`
	Topic     string      `gorm:"type:varchar(255);not null"`
	Payload   []byte      `gorm:"type:bytea;not null"`
	State     OutboxState `gorm:"type:varchar(50);not null;default:'PENDING';index"`
	CreatedAt time.Time   `gorm:"index;autoCreateTime"`
	UpdatedAt time.Time   `gorm:"autoUpdateTime"`
}

type OutboxRetry struct {
	ID        string    `gorm:"primaryKey;default:gen_random_uuid()"`
	EventID   string    `gorm:"type:uuid;not null;index"`
	Error     string    `gorm:"type:text;not null"`
	CreatedAt time.Time `gorm:"index;autoCreateTime"`
}
