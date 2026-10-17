package consumer

import (
	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go/jetstream"
)

func deliver(
	msg jetstream.Msg,
	opts Opts,
) {
	if !frame.IsFrame(msg) {
		process(
			msg,
			opts,
		)
		return
	}

	every := heartbeatEvery(opts)
	elements, err := frame.Unpack(
		msg,
		every,
	)
	if err != nil {
		poisonFrame(
			msg,
			opts,
			err,
		)
		return
	}

	stop := keepAlive(
		msg,
		every,
	)
	defer stop()
	for _, element := range elements {
		process(
			element,
			opts,
		)
	}
}
