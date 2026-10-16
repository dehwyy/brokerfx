package outbox

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *OutboxRelay) claimLegacy(db *gorm.DB, caps schemaCaps) ([]relayEvent, error) {
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

		return whereIDs(
			tx.Model(&OutboxEvent{}),
			caps.IDType,
			ids,
		).Update("state", StateInFlight).Error
	})

	return events, err
}
