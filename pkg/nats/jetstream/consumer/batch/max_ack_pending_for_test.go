package batch

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestMaxAckPendingFor(t *testing.T) {
	cases := []struct {
		name     string
		replicas int
		pipeline int
		maxSize  int
		want     int
	}{
		{
			name:     "no replicas keeps pipeline times max",
			pipeline: 2,
			maxSize:  16,
			want:     32,
		},
		{
			name:     "three replicas",
			replicas: 3,
			pipeline: 2,
			maxSize:  16,
			want:     144,
		},
		{
			name:     "odd product rounds up",
			replicas: 1,
			pipeline: 1,
			maxSize:  5,
			want:     8,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got := MaxAckPendingFor(
					tc.replicas,
					tc.pipeline,
					tc.maxSize,
				)
				if got != tc.want {
					t.Fatalf("want %d got %d", tc.want, got)
				}
			},
		)
	}
}

func TestValidateReplicasMaxAckPending(t *testing.T) {
	s := settings{
		max:         16,
		pipeline:    2,
		maxAttempts: 5,
		replicas:    3,
	}
	cfg := jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   30 * time.Second,
	}

	filled, err := validate(
		cfg,
		s,
	)
	if err != nil {
		t.Fatal(err)
	}
	if filled.MaxAckPending != 144 {
		t.Fatalf("want 144 got %d", filled.MaxAckPending)
	}

	cfg.MaxAckPending = 100
	_, err = validate(
		cfg,
		s,
	)
	if !errors.Is(
		err,
		ErrMaxAckPending,
	) {
		t.Fatalf("want ErrMaxAckPending got %v", err)
	}

	cfg.MaxAckPending = 32
	_, err = validate(
		cfg,
		s.perPod(),
	)
	if err != nil {
		t.Fatalf("durable left by an older pod must pass, got %v", err)
	}
}

func TestResolveReplicas(t *testing.T) {
	t.Setenv(
		envReplicas,
		"4",
	)
	s, err := resolve(Opts{
		Handler: noopHandler,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.replicas != 4 {
		t.Fatalf("want 4 got %d", s.replicas)
	}

	s, err = resolve(Opts{
		Handler:  noopHandler,
		Replicas: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.replicas != 2 {
		t.Fatalf("want 2 got %d", s.replicas)
	}

	_, err = resolve(Opts{
		Handler:  noopHandler,
		Replicas: -1,
	})
	if !errors.Is(
		err,
		ErrInvalidOpts,
	) {
		t.Fatalf("want ErrInvalidOpts got %v", err)
	}
}
