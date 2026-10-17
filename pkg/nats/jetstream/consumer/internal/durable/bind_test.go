package durable

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func request(js jetstream.JetStream, strict bool, cfg jetstream.ConsumerConfig) *BindRequest {
	return &BindRequest{
		JetStream: js,
		Stream:    "ORDERS",
		Config:    cfg,
		Strict:    strict,
		Rejected:  "rejected",
	}
}

func wanted() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:        "worker",
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		FilterSubjects: []string{"order.updated", "order.updated.>"},
	}
}

func live() *fakeConsumer {
	return &fakeConsumer{cfg: jetstream.ConsumerConfig{
		Durable:       "worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   42,
		FilterSubject: "order.updated",
	}}
}

var errAckPolicy = &jetstream.APIError{Code: 400, ErrorCode: 10012, Description: "consumer ack policy can not be updated"}

func TestBindStrictRejectedReturnsServerCode(t *testing.T) {
	js := &fakeJS{existing: live(), updateErrs: []error{errAckPolicy}}

	bound, err := Bind(t.Context(), request(js, true, wanted()))
	if !errors.Is(err, ErrUpdateRejected) || bound != nil {
		t.Fatalf("want rejected, got %v %v", bound, err)
	}
	for _, part := range []string{"ORDERS", "worker", "code 400", "err_code 10012"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q has no %q", err, part)
		}
	}
	if len(js.updated) != 1 {
		t.Fatalf("rejection must not be retried, updates %d", len(js.updated))
	}
}

func TestBindLaxRejectedKeepsExisting(t *testing.T) {
	existing := live()
	js := &fakeJS{existing: existing, updateErrs: []error{errAckPolicy}}

	bound, err := Bind(t.Context(), request(js, false, wanted()))
	if err != nil || bound != existing {
		t.Fatalf("want existing durable, got %v %v", bound, err)
	}
}

func TestBindKeepsStartPosition(t *testing.T) {
	js := &fakeJS{existing: live()}

	if _, err := Bind(t.Context(), request(js, true, wanted())); err != nil {
		t.Fatal(err)
	}
	sent := js.updated[0]
	if sent.DeliverPolicy != jetstream.DeliverByStartSequencePolicy || sent.OptStartSeq != 42 {
		t.Fatalf("start position not kept: %v %d", sent.DeliverPolicy, sent.OptStartSeq)
	}
	if sent.FilterSubject != "" || len(sent.FilterSubjects) != 2 {
		t.Fatalf("filters not sent: %q %v", sent.FilterSubject, sent.FilterSubjects)
	}
}

func TestBindStrictRetriesTransient(t *testing.T) {
	js := &fakeJS{
		existing:   live(),
		lookupErrs: []error{nats.ErrNoResponders},
		updateErrs: []error{nats.ErrTimeout, &jetstream.APIError{Code: 503, ErrorCode: 10039, Description: "jetstream not available"}},
	}

	bound, err := Bind(t.Context(), request(js, true, wanted()))
	if err != nil || bound == nil {
		t.Fatalf("want bound after retries, got %v", err)
	}
	if len(js.updated) != 3 {
		t.Fatalf("updates %d, want 3", len(js.updated))
	}
}

func TestBindStrictTransientGivesUp(t *testing.T) {
	window := retryWindow
	retryWindow = 20 * time.Millisecond
	t.Cleanup(func() { retryWindow = window })

	errs := make([]error, 1000)
	for i := range errs {
		errs[i] = nats.ErrTimeout
	}
	js := &fakeJS{existing: live(), updateErrs: errs}

	_, err := Bind(t.Context(), request(js, true, wanted()))
	if !errors.Is(err, ErrBindTransient) || !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("want transient give up, got %v", err)
	}
}

func TestBindLaxTransientNotRetried(t *testing.T) {
	js := &fakeJS{lookupErrs: []error{nats.ErrTimeout}}

	if _, err := Bind(t.Context(), request(js, false, wanted())); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("want timeout, got %v", err)
	}
}

func TestBindStrictFilterMismatch(t *testing.T) {
	js := &fakeJS{
		existing: live(),
		applied: func(cfg jetstream.ConsumerConfig) jetstream.ConsumerConfig {
			cfg.FilterSubjects = []string{"order.updated"}
			return cfg
		},
	}

	if _, err := Bind(t.Context(), request(js, true, wanted())); !errors.Is(err, ErrFilterMismatch) {
		t.Fatalf("want filter mismatch, got %v", err)
	}
}

func TestBindStrictCreateRaceUpdates(t *testing.T) {
	js := &fakeJS{createErrs: []error{jetstream.ErrConsumerExists}, raced: live()}

	bound, err := Bind(t.Context(), request(js, true, wanted()))
	if err != nil || bound == nil {
		t.Fatalf("want bound after create race, got %v", err)
	}
	if len(js.updated) != 1 {
		t.Fatalf("race must end in update, updates %d", len(js.updated))
	}
}

func TestBindLaxCreateRaceTakesExisting(t *testing.T) {
	raced := live()
	js := &fakeJS{createErrs: []error{jetstream.ErrConsumerExists}, raced: raced}

	bound, err := Bind(t.Context(), request(js, false, wanted()))
	if err != nil || bound != raced || len(js.updated) != 0 {
		t.Fatalf("want raced durable as is, got %v %v %d", bound, err, len(js.updated))
	}
}
