package cleanup

import (
	"context"
	"time"

	"gorm.io/gorm"
)

const deleteBatchQuery = `DELETE FROM processed_messages WHERE ctid IN (SELECT ctid FROM processed_messages WHERE processed_at < ? LIMIT ?)`

var deleteBatch = func(
	ctx context.Context,
	db *gorm.DB,
	before time.Time,
	limit int,
) (int64, error) {
	deletion := db.WithContext(ctx).Exec(
		deleteBatchQuery,
		before,
		limit,
	)

	return deletion.RowsAffected, deletion.Error
}
