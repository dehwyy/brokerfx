package batch

import "github.com/nats-io/nats.go/jetstream"

type Group struct {
	Key  string
	Msgs []jetstream.Msg
}
