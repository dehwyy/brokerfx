package frame

import "github.com/nats-io/nats.go/jetstream"

func IsFrame(msg jetstream.Msg) bool {
	return msg.Headers().Get(HeaderVersion) != ""
}
