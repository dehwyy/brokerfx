package frame

import "github.com/nats-io/nats.go"

func (e *elementMsg) Headers() nats.Header {
	return e.headers
}
