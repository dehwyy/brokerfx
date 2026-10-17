package frame

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func Unpack(
	msg jetstream.Msg,
	heartbeat time.Duration,
) ([]jetstream.Msg, error) {
	if !IsFrame(msg) {
		return []jetstream.Msg{msg}, nil
	}

	if version := msg.Headers().Get(HeaderVersion); version != Version {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrUnknownVersion,
			version,
		)
	}

	elements, err := Decode(msg.Data())
	if err != nil {
		return nil, err
	}
	if count := msg.Headers().Get(HeaderCount); count != strconv.Itoa(len(elements)) {
		return nil, fmt.Errorf(
			"%w: count header %q, decoded %d",
			ErrBadFrame,
			count,
			len(elements),
		)
	}

	state := &frameState{
		msg:      msg,
		pending:  len(elements),
		throttle: heartbeat / 2,
	}
	msgs := make(
		[]jetstream.Msg,
		0,
		len(elements),
	)
	for i, element := range elements {
		if element.Subject != msg.Subject() {
			return nil, fmt.Errorf(
				"%w: element %d subject %q, frame subject %q",
				ErrBadFrame,
				i,
				element.Subject,
				msg.Subject(),
			)
		}
		headers := nats.Header(element.Headers)
		if headers == nil {
			headers = nats.Header{}
		}
		msgs = append(
			msgs,
			&elementMsg{
				frame:   state,
				subject: element.Subject,
				headers: headers,
				data:    element.Payload,
			},
		)
	}

	return msgs, nil
}
