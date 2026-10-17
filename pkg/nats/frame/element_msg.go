package frame

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type elementMsg struct {
	frame   *frameState
	subject string
	headers nats.Header
	data    []byte
	done    bool
}

var _ jetstream.Msg = (*elementMsg)(nil)
