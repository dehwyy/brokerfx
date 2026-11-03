//go:build integration

package consumer_test

import (
	"context"
	"fmt"
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
		t.Fatalf(
			"create stream: %v",
			err,
		)
	}

	published := map[string][]string{}
	for i, subject := range []string{"snap.a", "snap.b", "snap.a"} {
		data := fmt.Sprintf(
			"%s-%d",
			subject,
			i,
		)
		published[subject] = append(
			published[subject],
			data,
		)

		if _, err := js.Publish(
			ctx,
			subject,
			[]byte(data),
		); err != nil {
			t.Fatalf(
				"publish: %v",
				err,
			)
		}
	}

	var mu sync.Mutex
	got := map[string]int{}
	last := map[string]string{}
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
				last[msg.Subject()] = string(msg.Data())
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
		t.Fatalf(
			"ordered.New: %v",
			err,
		)
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
		t.Fatalf(
			"want one last snapshot per subject, got %v",
			got,
		)
	}

	for subject, values := range published {
		want := values[len(values)-1]
		if last[subject] != want {
			t.Fatalf(
				"%s: want last value %q got %q",
				subject,
				want,
				last[subject],
			)
		}
	}
}

func TestOrderedOnAttachedEmptyStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	js := testenv.NATS(t)

	if _, err := js.CreateStream(
		ctx,
		jetstream.StreamConfig{
			Name:     "ORDERED_EMPTY",
			Subjects: []string{"empty.>"},
		},
	); err != nil {
		t.Fatalf(
			"create stream: %v",
			err,
		)
	}

	attached := make(
		chan *jetstream.ConsumerInfo,
		1,
	)

	consumeCtx, err := ordered.New(
		ctx,
		ordered.Opts{
			JetStream:      js,
			Stream:         "ORDERED_EMPTY",
			FilterSubjects: []string{"empty.>"},
			DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
			OnAttached: func(_ context.Context, info *jetstream.ConsumerInfo) {
				attached <- info
			},
			HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"ordered.New: %v",
			err,
		)
	}
	defer consumeCtx.Stop()

	select {
	case info := <-attached:
		if info.NumPending != 0 {
			t.Fatalf(
				"empty stream must report NumPending 0, got %d",
				info.NumPending,
			)
		}
	default:
		t.Fatalf("OnAttached was not called before New returned")
	}
}
