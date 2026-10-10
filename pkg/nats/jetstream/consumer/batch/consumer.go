package batch

import (
	"context"
	"sync"
	"time"
)

type Consumer struct {
	handler        Handler
	onPoison       PoisonFunc
	onFatal        func(error)
	delays         []time.Duration
	maxAttempts    int
	heartbeatEvery time.Duration
	max            int
	linger         time.Duration
	pipeline       int
	fetcher        fetcher
	loopCtx        context.Context
	cancel         context.CancelFunc
	loops          sync.WaitGroup
	stopped        chan struct{}
	drainOnce      sync.Once
}
