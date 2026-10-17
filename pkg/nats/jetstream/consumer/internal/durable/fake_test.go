package durable

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	log.Logger = zerolog.Nop()
	retryFirst = time.Millisecond
	retryMax = 2 * time.Millisecond
	m.Run()
}

type fakeConsumer struct {
	jetstream.Consumer

	cfg jetstream.ConsumerConfig
}

func (c *fakeConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{Config: c.cfg}
}

type fakeJS struct {
	jetstream.JetStream

	mu         sync.Mutex
	existing   *fakeConsumer
	lookupErrs []error
	updateErrs []error
	createErrs []error
	updated    []jetstream.ConsumerConfig
	applied    func(jetstream.ConsumerConfig) jetstream.ConsumerConfig
	raced      *fakeConsumer
}

func pop(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeJS) Consumer(_ context.Context, _ string, _ string) (jetstream.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := pop(&f.lookupErrs); err != nil {
		return nil, err
	}
	if f.existing == nil {
		return nil, jetstream.ErrConsumerNotFound
	}
	return f.existing, nil
}

func (f *fakeJS) CreateConsumer(_ context.Context, _ string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := pop(&f.createErrs); err != nil {
		if f.raced != nil {
			f.existing = f.raced
		}
		return nil, err
	}
	f.existing = &fakeConsumer{cfg: cfg}
	return f.existing, nil
}

func (f *fakeJS) UpdateConsumer(_ context.Context, _ string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updated = append(f.updated, cfg)
	if err := pop(&f.updateErrs); err != nil {
		return nil, err
	}
	if f.applied != nil {
		cfg = f.applied(cfg)
	}
	f.existing = &fakeConsumer{cfg: cfg}
	return f.existing, nil
}
