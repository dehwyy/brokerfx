package replywait

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeObserver struct {
	lateIDs  []string
	rejected []error
}

func (o *fakeObserver) ReplyLate(correlationID string) {
	o.lateIDs = append(o.lateIDs, correlationID)
}

func (o *fakeObserver) ReplyRejected(err error) {
	o.rejected = append(o.rejected, err)
}

type fakeConsumerCreator struct {
	err error
}

func (c *fakeConsumerCreator) OrderedConsumer(
	_ context.Context,
	_ string,
	_ jetstream.OrderedConsumerConfig,
) (jetstream.Consumer, error) {
	return nil, c.err
}

func testListenerConfig() Config {
	return Config{
		Stream:            "BALANCE_REPLY",
		ReplySubject:      "balance.result.balanceapi.pod-1",
		CorrelationFunc:   func(msg jetstream.Msg) (string, error) { return msg.Subject(), nil },
		DefaultTimeout:    time.Second,
		InactiveThreshold: time.Minute,
	}
}

func TestListenerHandleResolvesWaitingCorrelation(t *testing.T) {
	reg := newRegistry()
	w, err := reg.register("corr-1")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	cfg := testListenerConfig()
	cfg.CorrelationFunc = func(msg jetstream.Msg) (string, error) { return "corr-1", nil }

	obs := &fakeObserver{}
	l := newListener(&fakeConsumerCreator{}, cfg, reg, obs)

	msg := &fakeMsg{subject: cfg.ReplySubject}
	l.handle(msg)

	got, err := w.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if got.Subject() != msg.Subject() {
		t.Fatalf("expected resolved msg %q, got %q", msg.Subject(), got.Subject())
	}
	if len(obs.lateIDs) != 0 || len(obs.rejected) != 0 {
		t.Fatalf("expected no observer calls, got late=%v rejected=%v", obs.lateIDs, obs.rejected)
	}
}

func TestListenerHandleCorrelationFuncErrorReportsRejected(t *testing.T) {
	reg := newRegistry()
	cfg := testListenerConfig()
	wantErr := errors.New("boom")
	cfg.CorrelationFunc = func(msg jetstream.Msg) (string, error) { return "", wantErr }

	obs := &fakeObserver{}
	l := newListener(&fakeConsumerCreator{}, cfg, reg, obs)

	msg := &fakeMsg{subject: cfg.ReplySubject}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("handle must not panic on correlation func error, got: %v", r)
			}
		}()
		l.handle(msg)
	}()

	if len(obs.rejected) != 1 || !errors.Is(obs.rejected[0], wantErr) {
		t.Fatalf("expected ReplyRejected(%v), got %v", wantErr, obs.rejected)
	}
	if len(obs.lateIDs) != 0 {
		t.Fatalf("expected no late reports, got %v", obs.lateIDs)
	}
}

func TestListenerHandleUnknownCorrelationReportsLate(t *testing.T) {
	reg := newRegistry()
	cfg := testListenerConfig()
	cfg.CorrelationFunc = func(msg jetstream.Msg) (string, error) { return "corr-unknown", nil }

	obs := &fakeObserver{}
	l := newListener(&fakeConsumerCreator{}, cfg, reg, obs)

	msg := &fakeMsg{subject: cfg.ReplySubject}
	l.handle(msg)

	if len(obs.lateIDs) != 1 || obs.lateIDs[0] != "corr-unknown" {
		t.Fatalf("expected ReplyLate(corr-unknown), got %v", obs.lateIDs)
	}
	if len(obs.rejected) != 0 {
		t.Fatalf("expected no rejected reports, got %v", obs.rejected)
	}
}

func TestListenerStartMissingStreamReturnsErrorWithStreamName(t *testing.T) {
	reg := newRegistry()
	cfg := testListenerConfig()

	l := newListener(&fakeConsumerCreator{err: jetstream.ErrStreamNotFound}, cfg, reg, nil)

	err := l.start(context.Background())
	if err == nil {
		t.Fatalf("expected error when stream is missing")
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("expected error to wrap ErrStreamNotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), cfg.Stream) {
		t.Fatalf("expected error to mention stream name %q, got %q", cfg.Stream, err.Error())
	}
}
