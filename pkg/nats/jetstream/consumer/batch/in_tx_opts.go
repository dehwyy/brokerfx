package batch

import (
	"context"
	"time"

	"github.com/dehwyy/txmanagerfx/pkg/txmanager"
	"github.com/nats-io/nats.go/jetstream"
)

type InTxOpts struct {
	TxManager      txmanager.TxManager
	Apply          func(ctx context.Context, msgs []jetstream.Msg) error
	Key            KeyFunc
	NoDedup        bool
	Transient      func(err error) bool
	RetryBudget    int
	TransientDelay time.Duration
	TxName         string
}
