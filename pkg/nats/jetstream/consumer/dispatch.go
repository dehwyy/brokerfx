package consumer

import (
	"github.com/nats-io/nats.go/jetstream"
)

func dispatch(
	opts Opts,
	inFlight *pool,
) jetstream.MessageHandler {
	if inFlight == nil {
		return func(msg jetstream.Msg) {
			go process(
				msg,
				opts,
			)
		}
	}

	return func(msg jetstream.Msg) {
		stop := keepAlive(
			msg,
			heartbeatEvery(opts),
		)
		inFlight.acquire()
		stop()

		go func() {
			defer inFlight.release()
			process(
				msg,
				opts,
			)
		}()
	}
}
