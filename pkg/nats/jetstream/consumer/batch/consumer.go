package batch

import (
	"context"
	"sync"
	"time"
)

type Consumer struct {
	handler        Handler
	observer       Observer
	onPoison       PoisonFunc
	onFatal        func(error)
	delays         []time.Duration
	maxAttempts    int
	maxDeliver     int
	heartbeatEvery time.Duration
	max            int
	linger         time.Duration
	minBatch       int
	lingerMax      time.Duration
	now            func() time.Time
	pipeline       int
	fetcher        fetcher
	loopCtx        context.Context
	cancel         context.CancelFunc
	loops          sync.WaitGroup
	stopped        chan struct{}
	fatalOnce      sync.Once
}
