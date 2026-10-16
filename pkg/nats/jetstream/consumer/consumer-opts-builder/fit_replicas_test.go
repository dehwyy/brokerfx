package consumeroptsbuilder

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestWithReplicas(t *testing.T) {
	if got := NewDefault().Build().Replicas; got != 0 {
		t.Fatalf(
			"default replicas %d",
			got,
		)
	}
	if got := NewDefault().WithReplicas(1).Build().Replicas; got != 1 {
		t.Fatalf(
			"replicas %d",
			got,
		)
	}
}

func TestFitReplicas(t *testing.T) {
	cases := []struct {
		name      string
		replicas  int
		retention jetstream.RetentionPolicy
		stream    int
		want      int
	}{
		{
			name:      "zero inherits",
			retention: jetstream.WorkQueuePolicy,
			stream:    3,
		},
		{
			name:      "limits stream accepts fewer replicas",
			replicas:  1,
			retention: jetstream.LimitsPolicy,
			stream:    3,
			want:      1,
		},
		{
			name:      "limits stream rejects more replicas than the stream",
			replicas:  3,
			retention: jetstream.LimitsPolicy,
			stream:    1,
		},
		{
			name:      "workqueue stream resets a different count",
			replicas:  1,
			retention: jetstream.WorkQueuePolicy,
			stream:    3,
		},
		{
			name:      "interest stream resets a different count",
			replicas:  1,
			retention: jetstream.InterestPolicy,
			stream:    3,
		},
		{
			name:      "workqueue stream keeps the matching count",
			replicas:  3,
			retention: jetstream.WorkQueuePolicy,
			stream:    3,
			want:      3,
		},
		{
			name:      "negative is reset",
			replicas:  -1,
			retention: jetstream.LimitsPolicy,
			stream:    3,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg := FitReplicas(
					NewDefault().
						WithName(
							"c",
							true,
						).
						WithReplicas(tc.replicas).
						Build(),
					jetstream.StreamConfig{
						Name:      "S",
						Retention: tc.retention,
						Replicas:  tc.stream,
					},
				)
				if cfg.Replicas != tc.want {
					t.Fatalf(
						"replicas = %d, want %d",
						cfg.Replicas,
						tc.want,
					)
				}
				if cfg.Durable != "c" {
					t.Fatalf(
						"durable = %q, want c",
						cfg.Durable,
					)
				}
			},
		)
	}
}
