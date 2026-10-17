package frame

import (
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

func WireHeaders(ids []string) map[string]string {
	return map[string]string{
		HeaderVersion:         Version,
		HeaderCount:           strconv.Itoa(len(ids)),
		jetstream.MsgIDHeader: MsgID(ids),
	}
}
