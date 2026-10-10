package cleanup

import (
	"context"
	"sync"
	"time"

	"gorm.io/gorm"
)

type Cleaner struct {
	db         *gorm.DB
	retention  time.Duration
	interval   time.Duration
	batchLimit int
	onError    func(error)
	now        func() time.Time
	pause      time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	stopped chan struct{}
}
