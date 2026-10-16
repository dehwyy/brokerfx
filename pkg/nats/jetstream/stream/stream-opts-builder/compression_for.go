package streamoptsbuilder

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func CompressionFor(role Role) (jetstream.StoreCompression, error) {
	switch role {
	case RoleCommand, RoleReply:
		return jetstream.NoCompression, nil
	case RoleEvent, RoleResult, RoleDLQ:
		return jetstream.S2Compression, nil
	default:
		return jetstream.NoCompression, fmt.Errorf(
			"%w: %d",
			ErrUnknownRole,
			int(role),
		)
	}
}
