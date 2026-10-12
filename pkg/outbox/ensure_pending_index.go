package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gorm.io/gorm"
)

const (
	pendingIndexName           = "idx_outbox_events_pending_created_at"
	pendingIndexCleanupTimeout = 5 * time.Second
	pendingIndexTryLockQuery   = `SELECT pg_try_advisory_lock(hashtext('brokerfx.outbox.pending_index'))`
	pendingIndexUnlockQuery    = `SELECT pg_advisory_unlock(hashtext('brokerfx.outbox.pending_index'))`
	pendingIndexTimeoutQuery   = `SELECT current_setting('statement_timeout')`
	pendingIndexNoTimeoutQuery = `SET statement_timeout = 0`
	pendingIndexRestoreQuery   = `SELECT set_config('statement_timeout', ?, false)`
	pendingIndexTableQuery     = `SELECT to_regclass('outbox_events') IS NOT NULL`
	pendingIndexValidQuery     = `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = '` + pendingIndexName + `' AND c.relnamespace = current_schema()::regnamespace`
	pendingIndexDropQuery      = `DROP INDEX CONCURRENTLY IF EXISTS ` + pendingIndexName
	pendingIndexCreateQuery    = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ` + pendingIndexName + ` ON outbox_events (created_at) WHERE state <> '` + string(StateDone) + `'`
)

func EnsurePendingIndex(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Connection(func(connection *gorm.DB) (err error) {
		var tableExists bool
		if err := connection.Raw(pendingIndexTableQuery).Scan(&tableExists).Error; err != nil {
			return err
		}
		if !tableExists {
			return nil
		}

		var locked bool
		if err := connection.Raw(pendingIndexTryLockQuery).Scan(&locked).Error; err != nil {
			return err
		}
		if !locked {
			return nil
		}
		defer func() {
			err = errors.Join(
				err,
				cleanupPendingIndexSession(
					ctx,
					connection,
					pendingIndexUnlockQuery,
				),
			)
		}()

		var statementTimeout string
		if err := connection.Raw(pendingIndexTimeoutQuery).Scan(&statementTimeout).Error; err != nil {
			return err
		}
		if err := connection.Exec(pendingIndexNoTimeoutQuery).Error; err != nil {
			return err
		}
		defer func() {
			err = errors.Join(
				err,
				cleanupPendingIndexSession(
					ctx,
					connection,
					pendingIndexRestoreQuery,
					statementTimeout,
				),
			)
		}()

		return buildPendingIndex(connection)
	})
}

func buildPendingIndex(connection *gorm.DB) error {
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
}

func cleanupPendingIndexSession(
	ctx context.Context,
	connection *gorm.DB,
	query string,
	args ...any,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		pendingIndexCleanupTimeout,
	)
	defer cancel()

	return connection.
		WithContext(cleanupCtx).
		Exec(
			query,
			args...,
		).
		Error
}
