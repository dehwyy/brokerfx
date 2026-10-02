package outbox

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"gorm.io/gorm"
)

var ErrSchemaOutdated = errors.New("outbox: schema missing v2 columns, run outbox.AutoMigrate")

type schemaCaps struct {
	V2      bool
	Retries bool
}

const v2ColumnsQuery = `select count(*) from information_schema.columns where table_schema = current_schema() and table_name = 'outbox_events' and column_name in ('attempts', 'last_error', 'next_attempt_at', 'headers')`

const retriesTableQuery = `select to_regclass('outbox_retries')::text`

func detectSchema(ctx context.Context, db *gorm.DB) (schemaCaps, error) {
	var v2Columns int64
	if err := db.WithContext(ctx).Raw(v2ColumnsQuery).Scan(&v2Columns).Error; err != nil {
		return schemaCaps{}, err
	}

	var retriesTable sql.NullString
	if err := db.WithContext(ctx).Raw(retriesTableQuery).Scan(&retriesTable).Error; err != nil {
		return schemaCaps{}, err
	}

	return schemaCaps{
		V2:      v2Columns == 4,
		Retries: retriesTable.Valid,
	}, nil
}

func migrationStatements() []string {
	return []string{
		`alter table outbox_events add column if not exists attempts integer not null default 0`,
		`alter table outbox_events add column if not exists last_error text null`,
		`alter table outbox_events add column if not exists next_attempt_at timestamptz null`,
		`alter table outbox_events add column if not exists headers jsonb null`,
		`create index if not exists idx_outbox_events_state_updated_at on outbox_events (state, updated_at)`,
	}
}

func AutoMigrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&OutboxEvent{}, &OutboxRetry{}); err != nil {
		return err
	}

	for _, stmt := range migrationStatements() {
		if err := db.Exec(stmt).Error; err != nil {
			return err
		}
	}

	return nil
}

const defaultCapsRecheck = 5 * time.Second

type detectFunc func(context.Context, *gorm.DB) (schemaCaps, error)

type capsCache struct {
	mu          sync.Mutex
	caps        schemaCaps
	resolved    bool
	lastTry     time.Time
	haveTry     bool
	recheckWait time.Duration
}

func (c *capsCache) get(ctx context.Context, db *gorm.DB, detect detectFunc) (schemaCaps, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.resolved {
		return c.caps, true, nil
	}

	if c.haveTry && time.Since(c.lastTry) < c.recheckWait {
		return c.caps, false, nil
	}

	caps, err := detect(ctx, db)
	if err != nil {
		return schemaCaps{}, false, err
	}

	c.caps = caps
	c.lastTry = time.Now()
	c.haveTry = true
	c.resolved = caps.V2 && caps.Retries

	return c.caps, c.resolved, nil
}
