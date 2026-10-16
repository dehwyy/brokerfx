package timedactor

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestHoldDeadline(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		original time.Time
		want     time.Time
	}{
		{
			name:     "far timer keeps its deadline",
			original: now.Add(10 * time.Minute),
			want:     now.Add(10 * time.Minute),
		},
		{
			name:     "near timer is pushed to the hold",
			original: now.Add(5 * time.Second),
			want:     now.Add(15 * time.Second),
		},
		{
			name:     "overdue timer is pushed to the hold",
			original: now.Add(-time.Minute),
			want:     now.Add(15 * time.Second),
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got := holdDeadline(
					tc.original,
					now,
					15*time.Second,
				)
				if !got.Equal(tc.want) {
					t.Fatalf("want %v got %v", tc.want, got)
				}
			},
		)
	}
}

func TestHoldKeepsLaterOriginalDeadline(t *testing.T) {
	originalExpiry := time.Now().Add(10 * time.Minute)
	var updatedValue []byte

	kv := &mockKV{
		getFn: func(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
			return &mockEntry{
				key: key,
				value: mustMarshalPayload(
					t,
					originalExpiry,
					testMeta{
						Type: "TIMEOUT",
					},
				),
				revision:  1,
				operation: jetstream.KeyValuePut,
			}, nil
		},
		updateFn: func(_ context.Context, _ string, value []byte, _ uint64) (uint64, error) {
			updatedValue = value
			return 2, nil
		},
	}
	actor, cancel := newTestActor(kv)
	defer cancel()
	actor.config.MaxHoldDuration = 15 * time.Second

	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()

	if err := actor.Hold(
		holdCtx,
		"key",
	); err != nil {
		t.Fatal(err)
	}

	held, err := unmarshalPayload[testMeta](updatedValue)
	if err != nil {
		t.Fatal(err)
	}
	if held.ExpiresAt != originalExpiry.UnixNano() {
		t.Fatalf(
			"hold moved deadline from %v to %v",
			originalExpiry,
			time.Unix(
				0,
				held.ExpiresAt,
			),
		)
	}
}
