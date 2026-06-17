package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ProcessedMessage records a message a consumer has already handled, so a
// redelivery can be deduped at consume time within the consumer's own transaction.
type ProcessedMessage struct {
	MessageID   string    `gorm:"column:message_id;primaryKey"`
	Subject     string    `gorm:"column:subject;type:varchar(255);not null"`
	ProcessedAt time.Time `gorm:"column:processed_at;autoCreateTime"`
}

func (ProcessedMessage) TableName() string {
	return "processed_messages"
}

// AutoMigrateProcessed creates or updates the processed_messages table schema.
// Call during startup before using WithIdempotency, mirroring outbox.AutoMigrate.
func AutoMigrateProcessed(db *gorm.DB) error {
	return db.AutoMigrate(&ProcessedMessage{})
}

// IdempotencyKeyFunc extracts a dedup key from a message. The default
// (DefaultIdempotencyKey) reads the Nats-Msg-Id header and falls back to a
// deterministic hash of subject+payload when the header is absent.
type IdempotencyKeyFunc func(msg jetstream.Msg) string

// DefaultIdempotencyKey returns the Nats-Msg-Id header, or, when it is empty,
// a deterministic sha256 hash of subject+payload so the key is still stable
// across redeliveries of the same message.
func DefaultIdempotencyKey(msg jetstream.Msg) string {
	if id := msg.Headers().Get(jetstream.MsgIDHeader); id != "" {
		return id
	}

	h := sha256.New()
	h.Write([]byte(msg.Subject()))
	h.Write(msg.Data())
	return hex.EncodeToString(h.Sum(nil))
}

// WithIdempotency wraps a business handler with consume-side deduplication.
//
// OPT-IN: existing consumers are unaffected. Use it for new consumers that are not
// already idempotent via downstream idem-keys (the money consumers already are).
//
// For each message it opens a single transaction (via txmanager) and:
//   - if the dedup key already exists in processed_messages → Ack WITHOUT running
//     the business handler;
//   - otherwise runs the business handler, then inserts the dedup row — both in the
//     SAME transaction so the side effects and the dedup marker commit atomically.
//
// The wrapped handler MUST use txmanager.GetConnection(ctx) for its own DB writes so
// they share the transaction opened here. The returned func is a drop-in HandlerFunc.
func WithIdempotency(
	tx txmanager.TxManager,
	keyFunc IdempotencyKeyFunc,
	handler func(ctx context.Context, msg jetstream.Msg) error,
) func(ctx context.Context, msg jetstream.Msg) error {
	if keyFunc == nil {
		keyFunc = DefaultIdempotencyKey
	}

	return func(ctx context.Context, msg jetstream.Msg) error {
		key := keyFunc(msg)

		return tx.Do(
			ctx,
			"consumer-idempotency",
			func(ctx context.Context) error {
				row := ProcessedMessage{
					MessageID: key,
					Subject:   msg.Subject(),
				}

				claimed, err := claimMessage(tx.GetConnection(ctx), row)
				if err != nil {
					return err
				}

				if !claimed {
					log.Debug().
						Str("subject", msg.Subject()).
						Str("message_id", key).
						Msg("duplicate message skipped by consume-side idempotency")
					return nil
				}

				return handler(ctx, msg)
			},
		)
	}
}

// claimMessage attempts to insert the dedup row. It returns claimed=true when the
// row was newly inserted (first delivery) and claimed=false when the key already
// existed (redelivery). It is a package var so tests can substitute an in-memory
// claimer without pulling in a SQL driver. Insert-first with ON CONFLICT DO NOTHING
// makes the claim atomic under the row lock of the surrounding transaction.
var claimMessage = func(conn *gorm.DB, row ProcessedMessage) (bool, error) {
	res := conn.
		Clauses(clause.OnConflict{
			DoNothing: true,
		}).
		Create(&row)
	if res.Error != nil {
		return false, res.Error
	}

	return res.RowsAffected > 0, nil
}
