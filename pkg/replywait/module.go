package replywait

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
)

type ModuleDeps struct {
	fx.In

	JS       jetstream.JetStream
	Config   Config
	Waker    Waker    `optional:"true"`
	Observer Observer `optional:"true"`
}

func newModuleComponents(deps ModuleDeps) (*ReplyWaiter, *listener, error) {
	if err := deps.Config.Validate(); err != nil {
		return nil, nil, err
	}

	reg := newRegistry()
	rw := newReplyWaiter(deps.Config, reg, deps.Waker)
	l := newListener(deps.JS, deps.Config, reg, deps.Observer)
	rw.attachListener(l)

	return rw, l, nil
}

func registerModuleLifecycle(lc fx.Lifecycle, rw *ReplyWaiter, l *listener) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return l.start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			rw.BeginDrain()
			return rw.Drain(ctx)
		},
	})
}

var Module = fx.Module("replywait",
	fx.Provide(newModuleComponents),
	fx.Invoke(registerModuleLifecycle),
)
