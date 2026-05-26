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
//
// Producer is wired from JetStream by default; supply CoreModule (or your own
// fx.Provide for outbox.Producer) to publish via plain NATS instead.
var Module = fx.Module("outbox",
	fx.Provide(NewStore),
	fx.Provide(
		fx.Annotate(
			NewJetStreamProducer,
			fx.As(new(Producer)),
		),
	),
	fx.Provide(NewRelay),
	fx.Invoke(registerRelayLifecycle),
)

// CoreModule is an alternate composition that omits the default JetStream
// Producer binding. Use it when the application supplies its own Producer
// (for example brokerfx/pkg/nats/core.CoreProducer) via fx.Provide.
var CoreModule = fx.Module("outbox-core",
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
