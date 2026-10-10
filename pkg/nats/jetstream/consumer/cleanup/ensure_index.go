package cleanup

import (
	"context"

	"gorm.io/gorm"
)

const ensureIndexQuery = `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processed_messages_processed_at ON processed_messages (processed_at)`

func EnsureIndex(
	ctx context.Context,
	db *gorm.DB,
) error {
	return db.WithContext(ctx).Exec(ensureIndexQuery).Error
}
