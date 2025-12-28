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

func New(opts Opts) jetstream.JetStream {
	js, err := jetstream.New(opts.Conn)
	if err != nil {
		panic(err)
	}

	return js
}
