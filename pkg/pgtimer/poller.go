package pgtimer

import (
	"context"
	"fmt"
	"sync"

	"gorm.io/gorm"
)

type pollerState int

const (
	stateIdle pollerState = iota
	stateRunning
	stateStopped
)

type Poller struct {
	db       *gorm.DB
	settings settings
	queries  queries
	observer Observer

	mu       sync.Mutex
	state    pollerState
	handlers map[string]HandlerFunc
	names    []string

	slots    chan struct{}
	cancel   context.CancelFunc
	work     context.Context
	stopWork context.CancelFunc
	loopDone chan struct{}
	fires    sync.WaitGroup
}

func New(
	db *gorm.DB,
	cfg Config,
) (*Poller, error) {
	if db == nil {
		return nil, fmt.Errorf(
			"%w: db is nil",
			ErrInvalidConfig,
		)
	}

	s, err := resolve(cfg)
	if err != nil {
		return nil, err
	}

	built, err := buildQueries(cfg.Table)
	if err != nil {
		return nil, err
	}

	return &Poller{
		db:       db,
		settings: s,
		queries:  built,
		observer: observerOrNoop(cfg.Observer),
		handlers: map[string]HandlerFunc{},
		slots: make(
			chan struct{},
			s.concurrency,
		),
		loopDone: make(chan struct{}),
	}, nil
}
