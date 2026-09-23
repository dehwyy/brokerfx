package replywait

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeObserver struct {
	mu sync.Mutex

	startedIDs  []string
	resolvedIDs []string
	timedOutIDs []string
	drainedIDs  []string
	lateIDs     []string
	rejected    []error
	inflight    []int
}

func (o *fakeObserver) WaitStarted(correlationID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.startedIDs = append(o.startedIDs, correlationID)
}

func (o *fakeObserver) WaitResolved(correlationID string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.resolvedIDs = append(o.resolvedIDs, correlationID)
}

func (o *fakeObserver) WaitTimedOut(correlationID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timedOutIDs = append(o.timedOutIDs, correlationID)
}

func (o *fakeObserver) WaitDrained(correlationID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.drainedIDs = append(o.drainedIDs, correlationID)
}

func (o *fakeObserver) ReplyLate(correlationID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lateIDs = append(o.lateIDs, correlationID)
}

func (o *fakeObserver) ReplyRejected(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rejected = append(o.rejected, err)
}

func (o *fakeObserver) Inflight(count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inflight = append(o.inflight, count)
}

func (o *fakeObserver) snapshotStarted() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.startedIDs...)
}

func (o *fakeObserver) snapshotResolved() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.resolvedIDs...)
}

func (o *fakeObserver) snapshotTimedOut() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.timedOutIDs...)
}

func (o *fakeObserver) snapshotDrained() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.drainedIDs...)
}

func (o *fakeObserver) lastInflight() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.inflight) == 0 {
		return -1
	}
	return o.inflight[len(o.inflight)-1]
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
