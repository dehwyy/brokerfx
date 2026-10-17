package frame

import "github.com/nats-io/nats.go/jetstream"

func (e *elementMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return e.frame.msg.Metadata()
}
