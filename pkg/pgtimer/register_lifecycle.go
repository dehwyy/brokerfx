package pgtimer

import (
	"context"

	"go.uber.org/fx"
)

func RegisterLifecycle(
	lc fx.Lifecycle,
	poller *Poller,
) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return poller.Start()
		},
		OnStop: func(ctx context.Context) error {
			return poller.Stop(ctx)
		},
	})
}
