package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSaveMessageRejectsReservedMsgIDHeader(t *testing.T) {
	store := &OutboxStore{}

	err := store.SaveMessage(
		context.Background(),
		Message{
			Subject: "test.subject",
			Payload: []byte("payload"),
		},
		WithHeaders(map[string]string{
			jetstream.MsgIDHeader: "custom-id",
		}),
	)

	if !errors.Is(err, ErrReservedHeader) {
		t.Fatalf("expected ErrReservedHeader, got %v", err)
	}
}

func TestWithHeadersMergesAcrossCalls(t *testing.T) {
	var options messageOptions

	WithHeaders(map[string]string{"X-A": "1"})(&options)
	WithHeaders(map[string]string{"X-B": "2"})(&options)

	if options.headers["X-A"] != "1" || options.headers["X-B"] != "2" {
		t.Fatalf("expected both headers to be present, got %#v", options.headers)
	}
}

func TestNewMessageOptionsRejectsReservedHeaderBeforeTouchingStore(t *testing.T) {
	_, err := newMessageOptions([]MessageOption{
		WithHeaders(map[string]string{jetstream.MsgIDHeader: "x"}),
	})

	if !errors.Is(err, ErrReservedHeader) {
		t.Fatalf("expected ErrReservedHeader, got %v", err)
	}
}
