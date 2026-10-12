//go:build integration

package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/outbox"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	pendingIndexValidity = `select i.indisvalid from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'idx_outbox_events_pending_created_at'`
	pendingIndexLockHold = `select pg_advisory_lock(hashtext('brokerfx.outbox.pending_index'))`
	pendingIndexSeedRows = `insert into outbox_events (id, topic, payload, state, created_at, updated_at) select gen_random_uuid(), 't', '\x00'::bytea, 'DONE', now() - interval '1 hour' + g * interval '1 microsecond', now() from generate_series(1, 200000) g`
)

func pendingIndexValidityRows(
	t *testing.T,
	db *gorm.DB,
) []bool {
	t.Helper()

	var validity []bool
	require.NoError(
		t,
		db.Raw(pendingIndexValidity).Scan(&validity).Error,
	)

	return validity
}

func TestEnsurePendingIndexConcurrentCallers(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(
		t,
		outbox.AutoMigrate(db),
	)
	require.NoError(
		t,
		db.Exec(pendingIndexSeedRows).Error,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Minute,
	)
	defer cancel()

	const callers = 3
	start := make(chan struct{})
	results := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- outbox.EnsurePendingIndex(
				ctx,
				db,
			)
		}()
	}
	close(start)
	group.Wait()
	close(results)

	for callErr := range results {
		require.NoError(
			t,
			callErr,
		)
	}
	require.Equal(
		t,
		[]bool{true},
		pendingIndexValidityRows(
			t,
			db,
		),
	)
}

func TestEnsurePendingIndexSkipsWhenLockHeld(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(
		t,
		outbox.AutoMigrate(db),
	)

	sqlDB, err := db.DB()
	require.NoError(
		t,
		err,
	)
	holder, err := sqlDB.Conn(context.Background())
	require.NoError(
		t,
		err,
	)
	t.Cleanup(
		func() {
			require.NoError(
				t,
				holder.Close(),
			)
		},
	)
	_, err = holder.ExecContext(
		context.Background(),
		pendingIndexLockHold,
	)
	require.NoError(
		t,
		err,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	started := time.Now()
	require.NoError(
		t,
		outbox.EnsurePendingIndex(
			ctx,
			db,
		),
	)
	require.Less(
		t,
		time.Since(started),
		time.Second,
	)
	require.Empty(
		t,
		pendingIndexValidityRows(
			t,
			db,
		),
	)
}

func TestEnsurePendingIndexIgnoresSessionStatementTimeout(t *testing.T) {
	t.Parallel()

	db := testenv.Postgres(t)
	require.NoError(
		t,
		outbox.AutoMigrate(db),
	)

	dialector, ok := db.Dialector.(*postgres.Dialector)
	require.True(
		t,
		ok,
	)
	builder, err := gorm.Open(
		postgres.Open(dialector.DSN),
		&gorm.Config{},
	)
	require.NoError(
		t,
		err,
	)
	builderSQL, err := builder.DB()
	require.NoError(
		t,
		err,
	)
	t.Cleanup(
		func() {
			require.NoError(
				t,
				builderSQL.Close(),
			)
		},
	)
	builderSQL.SetMaxOpenConns(1)
	builderSQL.SetMaxIdleConns(1)
	require.NoError(
		t,
		builder.Exec(`set statement_timeout = '200ms'`).Error,
	)

	writer := db.Begin()
	require.NoError(
		t,
		writer.Error,
	)
	require.NoError(
		t,
		writer.Exec(`lock table outbox_events in row exclusive mode`).Error,
	)
	writerDone := make(chan error, 1)
	go func() {
		time.Sleep(time.Second)
		writerDone <- writer.Commit().Error
	}()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Minute,
	)
	defer cancel()

	started := time.Now()
	require.NoError(
		t,
		outbox.EnsurePendingIndex(
			ctx,
			builder,
		),
	)
	require.GreaterOrEqual(
		t,
		time.Since(started),
		500*time.Millisecond,
	)
	require.NoError(
		t,
		<-writerDone,
	)
	require.Equal(
		t,
		[]bool{true},
		pendingIndexValidityRows(
			t,
			db,
		),
	)

	var statementTimeout string
	require.NoError(
		t,
		builder.Raw(`show statement_timeout`).Scan(&statementTimeout).Error,
	)
	require.Equal(
		t,
		"200ms",
		statementTimeout,
	)
}
