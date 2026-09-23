package outbox

import (
	"errors"
	"net/textproto"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrReservedHeader = errors.New("outbox: header is reserved")

type Message struct {
	Subject string
	Payload []byte
}

type messageOptions struct {
	headers map[string]string
}

type MessageOption func(*messageOptions)

func WithHeaders(headers map[string]string) MessageOption {
	return func(o *messageOptions) {
		if o.headers == nil {
			o.headers = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			o.headers[k] = v
		}
	}
}

func newMessageOptions(opts []MessageOption) (messageOptions, error) {
	var options messageOptions
	for _, opt := range opts {
		opt(&options)
	}

	reserved := textproto.CanonicalMIMEHeaderKey(jetstream.MsgIDHeader)
	for k := range options.headers {
		if textproto.CanonicalMIMEHeaderKey(k) == reserved {
			return messageOptions{}, ErrReservedHeader
		}
	}

	return options, nil
}
