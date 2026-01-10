package consumer

import (
	cryptov1 "github.com/dehwyy/brokerfx/pkg/crypto/v1"
	"github.com/nats-io/nats.go/jetstream"
)

type Message[T any] struct {
	jetstream.Msg
}

func Decode[T any](msg jetstream.Msg) (T, error) {
	return cryptov1.Decode[T](msg.Data())
}
