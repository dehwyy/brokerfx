package timedactor

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Module creates an fx.Module that provides a TimedActor[T] instance.
//
// Since TimedActor is generic, the module must be parameterized with the
// metadata type T. Usage:
//
//	fx.New(
//	    timedactor.Module[MyMetadata](),
//	    // ...
//	)
func Module[T any]() fx.Option {
	return fx.Module("timedactor",
		fx.Provide(New[T]),
		fx.Invoke(registerLifecycle[T]),
	)
}

// registerLifecycle hooks TimedActor into the fx application lifecycle.
// OnStop cancels all background goroutines started by Subscribe and waits
// for them to drain gracefully via sync.WaitGroup.
func registerLifecycle[T any](lc fx.Lifecycle, actor *TimedActor[T]) {
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			log.Info().Msg("timed-actor lifecycle started")
			return nil
		},
		OnStop: func(_ context.Context) error {
			log.Info().Msg("timed-actor lifecycle stopping")
			actor.Stop()
			log.Info().Msg("timed-actor lifecycle stopped")
			return nil
		},
	})
}
