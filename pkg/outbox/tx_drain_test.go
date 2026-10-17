package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTxDrainEnvAndDefaults(t *testing.T) {
	t.Setenv(
		envTxDrain,
		"true",
	)
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})
	if !r.config.TxDrain || r.config.TxDrainPublishTimeout != defaultTxDrainPublishTimeout {
		t.Fatalf("tx drain %v timeout %s", r.config.TxDrain, r.config.TxDrainPublishTimeout)
	}
	if r.startErr != nil {
		t.Fatal(r.startErr)
	}
}

func TestTxDrainOffByDefault(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
	})
	if r.config.TxDrain || r.config.TxDrainPublishTimeout != 0 {
		t.Fatalf("tx drain %v timeout %s", r.config.TxDrain, r.config.TxDrainPublishTimeout)
	}
}

func TestTxDrainTimeoutAt25sFailsStart(t *testing.T) {
	t.Setenv(
		envTxDrainTimeout,
		"25s",
	)
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:    ModeDeleteAfterSend,
			TxDrain: true,
		},
	})
	if !errors.Is(r.startErr, ErrTxDrainPublishTimeout) {
		t.Fatalf("start error %v", r.startErr)
	}

	r.Run(context.Background())
	select {
	case <-r.Done():
	default:
		t.Fatal("relay with a start error must stop at once")
	}
}

func TestUnknownOutcome(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			err:  context.Canceled,
			want: true,
		},
		{
			err:  natsgo.ErrTimeout,
			want: true,
		},
		{
			err:  jetstream.ErrAsyncPublishTimeout,
			want: true,
		},
		{
			err:  fmt.Errorf("publish: %w", jetstream.ErrTooManyStalledMsgs),
			want: true,
		},
		{
			err:  natsgo.ErrConnectionClosed,
			want: true,
		},
		{
			err:  jetstream.ErrNoStreamResponse,
			want: false,
		},
		{
			err:  errors.New("maximum messages exceeded"),
			want: false,
		},
	}

	for _, tc := range cases {
		if got := unknownOutcome(tc.err); got != tc.want {
			t.Fatalf("%v: want %v got %v", tc.err, tc.want, got)
		}
	}
}

type slowProducer struct {
	delay time.Duration
}

func (p slowProducer) Produce(_ context.Context, event ProducerEvent) error {
	if event.Subject == "slow" {
		time.Sleep(p.delay)
	}

	return nil
}

type slowBatchProducer struct {
	slowProducer
}

func (p slowBatchProducer) ProduceBatch(_ context.Context, events []ProducerEvent) []error {
	time.Sleep(p.delay)

	return make(
		[]error,
		len(events),
	)
}

func TestPublishWithinBoundsPublishTime(t *testing.T) {
	events := []ProducerEvent{
		{
			Subject: "fast",
		},
		{
			Subject: "slow",
		},
	}

	r := &OutboxRelay{
		producer: slowProducer{
			delay: time.Second,
		},
	}
	started := time.Now()
	errs := r.publishWithin(
		context.Background(),
		events,
		50*time.Millisecond,
	)
	if time.Since(started) > 500*time.Millisecond {
		t.Fatalf("publishWithin waited %s", time.Since(started))
	}
	if errs[0] != nil || !errors.Is(errs[1], context.DeadlineExceeded) {
		t.Fatalf("errs %v", errs)
	}

	r.producer = slowBatchProducer{
		slowProducer: slowProducer{
			delay: time.Second,
		},
	}
	errs = r.publishWithin(
		context.Background(),
		events,
		50*time.Millisecond,
	)
	if !errors.Is(errs[0], context.DeadlineExceeded) || !errors.Is(errs[1], context.DeadlineExceeded) {
		t.Fatalf("batch errs %v", errs)
	}
}
