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

func TestNewMessageOptionsRejectsReservedHeaderCaseInsensitive(t *testing.T) {
	_, err := newMessageOptions([]MessageOption{
		WithHeaders(map[string]string{"nats-msg-id": "x"}),
	})

	if !errors.Is(err, ErrReservedHeader) {
		t.Fatalf("expected ErrReservedHeader, got %v", err)
	}
}

func TestKVPutBuildsSubjectAndPayload(t *testing.T) {
	msg := KVPut("orders", "42", []byte("value"))

	if msg.Subject != "$KV.orders.42" {
		t.Fatalf("expected subject $KV.orders.42, got %q", msg.Subject)
	}
	if string(msg.Payload) != "value" {
		t.Fatalf("expected payload value, got %q", msg.Payload)
	}
	if msg.err != nil {
		t.Fatalf("expected no error, got %v", msg.err)
	}
}

func TestKVPutWithNilValueUsesEmptyPayload(t *testing.T) {
	msg := KVPut("orders", "42", nil)

	if msg.Payload == nil || len(msg.Payload) != 0 {
		t.Fatalf("expected empty non-nil payload, got %#v", msg.Payload)
	}
}

func TestKVDeleteBuildsSubjectEmptyPayloadAndDeleteHeader(t *testing.T) {
	msg := KVDelete("orders", "42")

	if msg.Subject != "$KV.orders.42" {
		t.Fatalf("expected subject $KV.orders.42, got %q", msg.Subject)
	}
	if len(msg.Payload) != 0 {
		t.Fatalf("expected empty payload, got %q", msg.Payload)
	}
	if msg.headers[kvOperationHeader] != kvOperationDelete {
		t.Fatalf("expected KV-Operation: DEL header, got %#v", msg.headers)
	}
}

func TestKVPutRejectsInvalidBucket(t *testing.T) {
	msg := KVPut("bad bucket", "42", []byte("value"))

	if !errors.Is(msg.err, ErrInvalidKVBucket) {
		t.Fatalf("expected ErrInvalidKVBucket, got %v", msg.err)
	}
}

func TestKVPutRejectsInvalidKey(t *testing.T) {
	for _, key := range []string{"", ".leading", "trailing.", "with space"} {
		msg := KVPut("orders", key, []byte("value"))
		if !errors.Is(msg.err, ErrInvalidKVKey) {
			t.Fatalf("key %q: expected ErrInvalidKVKey, got %v", key, msg.err)
		}
	}
}

func TestKVDeleteRejectsInvalidBucketOrKey(t *testing.T) {
	if msg := KVDelete("bad bucket", "42"); !errors.Is(msg.err, ErrInvalidKVBucket) {
		t.Fatalf("expected ErrInvalidKVBucket, got %v", msg.err)
	}
	if msg := KVDelete("orders", ".bad"); !errors.Is(msg.err, ErrInvalidKVKey) {
		t.Fatalf("expected ErrInvalidKVKey, got %v", msg.err)
	}
}

func TestSaveMessageReturnsValidationErrorBeforeTouchingStore(t *testing.T) {
	store := &OutboxStore{}

	err := store.SaveMessage(context.Background(), KVPut("bad bucket", "42", []byte("value")))

	if !errors.Is(err, ErrInvalidKVBucket) {
		t.Fatalf("expected ErrInvalidKVBucket, got %v", err)
	}
}
