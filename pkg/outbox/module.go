package outbox

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Module provides the outbox transactional pattern as an fx.Module.
// It registers OutboxStore and OutboxRelay as providers and manages
// the relay lifecycle (start/stop) via fx.Lifecycle.
//
// Required dependencies in the fx container:
//   - *gorm.DB (with PostgreSQL driver configured)
//   - jetstream.JetStream
var Module = fx.Module("outbox",
	fx.Provide(NewStore),
	fx.Provide(NewRelay),
	fx.Invoke(registerRelayLifecycle),
)

func registerRelayLifecycle(lc fx.Lifecycle, relay *OutboxRelay) {
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			relay.cancel = cancel

			go relay.Run(ctx)

			log.Info().Msg("outbox relay lifecycle started")
			return nil
		},
		OnStop: func(_ context.Context) error {
			log.Info().Msg("outbox relay lifecycle stopping")

			if relay.cancel != nil {
				relay.cancel()
			}

			<-relay.Done()

			log.Info().Msg("outbox relay lifecycle stopped")
			return nil
		},
	})
}
