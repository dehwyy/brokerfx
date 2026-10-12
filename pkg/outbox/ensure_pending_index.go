package outbox

import (
	"context"
	"database/sql"
	"errors"

	"gorm.io/gorm"
)

const (
	pendingIndexName        = "idx_outbox_events_pending_created_at"
	pendingIndexLockQuery   = `SELECT pg_advisory_lock(hashtext('brokerfx.outbox.pending_index'))`
	pendingIndexUnlockQuery = `SELECT pg_advisory_unlock(hashtext('brokerfx.outbox.pending_index'))`
	pendingIndexTableQuery  = `SELECT to_regclass('outbox_events') IS NOT NULL`
	pendingIndexValidQuery  = `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = '` + pendingIndexName + `' AND c.relnamespace = current_schema()::regnamespace`
	pendingIndexDropQuery   = `DROP INDEX CONCURRENTLY IF EXISTS ` + pendingIndexName
	pendingIndexCreateQuery = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ` + pendingIndexName + ` ON outbox_events (created_at) WHERE state <> '` + string(StateDone) + `'`
)

func EnsurePendingIndex(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Connection(func(connection *gorm.DB) (err error) {
		if err := connection.Exec(pendingIndexLockQuery).Error; err != nil {
			return err
		}
		defer func() {
			unlockErr := connection.
				WithContext(context.WithoutCancel(ctx)).
				Exec(pendingIndexUnlockQuery).
				Error
			err = errors.Join(
				err,
				unlockErr,
			)
		}()

		var tableExists bool
		if err := connection.Raw(pendingIndexTableQuery).Scan(&tableExists).Error; err != nil {
			return err
		}
		if !tableExists {
			return nil
		}

		var valid sql.NullBool
		if err := connection.Raw(pendingIndexValidQuery).Scan(&valid).Error; err != nil {
			return err
		}
		if valid.Valid && valid.Bool {
			return nil
		}
		if valid.Valid {
			if err := connection.Exec(pendingIndexDropQuery).Error; err != nil {
				return err
			}
		}

		return connection.Exec(pendingIndexCreateQuery).Error
	})
}
