package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultRetryBudget = 2
	defaultTxName      = "batch-in-tx"
)

type inTx struct {
	tx             txmanager.TxManager
	apply          func(ctx context.Context, msgs []jetstream.Msg) error
	key            KeyFunc
	noDedup        bool
	transient      func(err error) bool
	retryBudget    int
	transientDelay time.Duration
	txName         string
}

func InTx(opts InTxOpts) (Handler, error) {
	if opts.TxManager == nil || opts.Apply == nil {
		return nil, fmt.Errorf(
			"%w: InTx requires TxManager and Apply",
			ErrInvalidOpts,
		)
	}

	h := &inTx{
		tx:             opts.TxManager,
		apply:          opts.Apply,
		key:            opts.Key,
		noDedup:        opts.NoDedup,
		transient:      opts.Transient,
		retryBudget:    opts.RetryBudget,
		transientDelay: opts.TransientDelay,
		txName:         opts.TxName,
	}
	if h.key == nil {
		h.key = consumer.DefaultIdempotencyKey
	}
	if h.transient == nil {
		h.transient = DefaultTransient
	}
	switch {
	case h.retryBudget == 0:
		h.retryBudget = defaultRetryBudget
	case h.retryBudget < 0:
		h.retryBudget = 0
	}
	if h.transientDelay < 0 {
		h.transientDelay = 0
	}
	if h.txName == "" {
		h.txName = defaultTxName
	}

	return h.handle, nil
}

func (h *inTx) handle(
	ctx context.Context,
	msgs []jetstream.Msg,
) []Result {
	results := make(
		[]Result,
		len(msgs),
	)
	if len(msgs) == 0 {
		return results
	}

	subset := make(
		[]int,
		len(msgs),
	)
	for i := range subset {
		subset[i] = i
	}

	budget := h.retryBudget
	h.bisect(
		ctx,
		msgs,
		subset,
		results,
		&budget,
	)

	return results
}
