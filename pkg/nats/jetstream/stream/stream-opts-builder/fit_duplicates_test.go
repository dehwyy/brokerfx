package streamoptsbuilder

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestBuildFitsDuplicatesIntoMaxAge(t *testing.T) {
	cases := []struct {
		name       string
		builder    *StreamOptsBuilder
		want       time.Duration
		wantMaxAge time.Duration
	}{
		{
			name:       "default window stays inside the default max age",
			builder:    NewDefault(),
			want:       15 * time.Minute,
			wantMaxAge: 12 * time.Hour,
		},
		{
			name:       "default window is clamped to a short max age",
			builder:    NewDefault().WithMaxAge(5 * time.Minute),
			want:       5 * time.Minute,
			wantMaxAge: 5 * time.Minute,
		},
		{
			name:    "unlimited max age keeps the default window",
			builder: NewDefault().WithMaxAge(0),
			want:    15 * time.Minute,
		},
		{
			name: "explicit window is clamped to max age",
			builder: NewDefault().
				WithMaxAge(10 * time.Minute).
				WithDuplicates(20 * time.Minute),
			want:       10 * time.Minute,
			wantMaxAge: 10 * time.Minute,
		},
		{
			name: "valid explicit window is kept",
			builder: NewDefault().
				WithMaxAge(10 * time.Minute).
				WithDuplicates(2 * time.Minute),
			want:       2 * time.Minute,
			wantMaxAge: 10 * time.Minute,
		},
		{
			name: "disabled dedup stays disabled",
			builder: NewDefault().
				WithMaxAge(time.Minute).
				WithDuplicates(0),
			wantMaxAge: time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg := tc.builder.
					WithName("S").
					WithSubjects([]string{"s.>"}).
					Build()
				if cfg.Duplicates != tc.want || cfg.MaxAge != tc.wantMaxAge {
					t.Fatalf(
						"duplicates = %s max age = %s, want %s and %s",
						cfg.Duplicates,
						cfg.MaxAge,
						tc.want,
						tc.wantMaxAge,
					)
				}
			},
		)
	}
}

func TestFitDuplicatesRawConfig(t *testing.T) {
	cfg := FitDuplicates(jetstream.StreamConfig{
		Name:       "RAW",
		MaxAge:     2 * time.Minute,
		Duplicates: 15 * time.Minute,
	})
	if cfg.Duplicates != 2*time.Minute {
		t.Fatalf(
			"duplicates = %s, want 2m",
			cfg.Duplicates,
		)
	}
}
