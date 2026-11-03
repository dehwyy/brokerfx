//go:build integration

package consumer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/ordered"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrderedDeliversLastPerSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	js := testenv.NATS(t)

	if _, err := js.CreateStream(
		ctx,
		jetstream.StreamConfig{
			Name:     "ORDERED_SNAP",
			Subjects: []string{"snap.>"},
		},
	); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	for _, subject := range []string{"snap.a", "snap.b", "snap.a"} {
		if _, err := js.Publish(
			ctx,
			subject,
			[]byte(subject+"-"+time.Now().String()),
		); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	var mu sync.Mutex
	got := map[string]int{}
	done := make(chan struct{})

	consumeCtx, err := ordered.New(
		ctx,
		ordered.Opts{
			JetStream:      js,
			Stream:         "ORDERED_SNAP",
			FilterSubjects: []string{"snap.>"},
			DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
			HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
				mu.Lock()
				defer mu.Unlock()
				got[msg.Subject()]++
				if len(got) == 2 {
					select {
					case <-done:
					default:
						close(done)
					}
				}
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("ordered.New: %v", err)
	}
	defer consumeCtx.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for snapshots")
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if got["snap.a"] != 1 || got["snap.b"] != 1 {
		t.Fatalf("want one last snapshot per subject, got %v", got)
	}
}
