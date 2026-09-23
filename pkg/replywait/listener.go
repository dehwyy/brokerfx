package replywait

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type orderedConsumerCreator interface {
	OrderedConsumer(ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error)
}

type listener struct {
	js       orderedConsumerCreator
	cfg      Config
	reg      *registry
	observer Observer

	consumeCtx jetstream.ConsumeContext
}

func newListener(js orderedConsumerCreator, cfg Config, reg *registry, observer Observer) *listener {
	if observer == nil {
		observer = NopObserver{}
	}

	return &listener{
		js:       js,
		cfg:      cfg,
		reg:      reg,
		observer: observer,
	}
}

func (l *listener) start(ctx context.Context) error {
	consumer, err := l.js.OrderedConsumer(ctx, l.cfg.Stream, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{l.cfg.ReplySubject},
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		InactiveThreshold: l.cfg.InactiveThreshold,
	})
	if err != nil {
		return fmt.Errorf("replywait: ordered consumer on stream %s: %w", l.cfg.Stream, err)
	}

	consumeCtx, err := consumer.Consume(l.handle)
	if err != nil {
		return fmt.Errorf("replywait: consume stream %s: %w", l.cfg.Stream, err)
	}

	l.consumeCtx = consumeCtx

	return nil
}

//nolint:unused
func (l *listener) stop() {
	if l.consumeCtx != nil {
		l.consumeCtx.Stop()
	}
}

func (l *listener) handle(msg jetstream.Msg) {
	id, err := l.cfg.CorrelationFunc(msg)
	if err != nil {
		log.Warn().Err(err).Str("subject", msg.Subject()).Msg("replywait: correlation func failed")
		l.observer.ReplyRejected(err)
		return
	}

	if !l.reg.resolve(id, msg) {
		l.observer.ReplyLate(id)
	}
}
