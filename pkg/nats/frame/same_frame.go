package frame

import "github.com/nats-io/nats.go/jetstream"

func SameFrame(
	first jetstream.Msg,
	second jetstream.Msg,
) bool {
	firstElement, ok := first.(*elementMsg)
	if !ok {
		return false
	}
	secondElement, ok := second.(*elementMsg)

	return ok && firstElement.frame == secondElement.frame
}
