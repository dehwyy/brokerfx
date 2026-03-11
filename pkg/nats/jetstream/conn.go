package jetstream

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
)

type Opts struct {
	fx.In

	Conn *nats.Conn
}

func New(opts Opts) (jetstream.JetStream, error) {
	js, err := jetstream.New(opts.Conn)
	if err != nil {
		return nil, err
	}

	return js, nil
}
