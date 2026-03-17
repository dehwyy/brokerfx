package timedactor

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Module provides the TimedActor distributed timer as an fx.Module.
// It registers TimedActor as a provider and manages its lifecycle
// (graceful shutdown) via fx.Lifecycle.
//
// Required dependencies in the fx container:
//   - jetstream.JetStream
//
// Optional dependencies:
//   - timedactor.Config (falls back to DefaultConfig if not provided)
var Module = fx.Module("timedactor",
	fx.Provide(New),
	fx.Invoke(registerLifecycle),
)

// registerLifecycle hooks TimedActor into the fx application lifecycle.
// OnStop cancels all background goroutines started by Subscribe and waits
// for them to drain gracefully via sync.WaitGroup.
func registerLifecycle(lc fx.Lifecycle, actor *TimedActor) {
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
