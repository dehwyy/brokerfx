package replywait

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnsureReplyStream_InvalidOpts(t *testing.T) {
	t.Parallel()

	base := ReplyStreamOpts{
		Name:     "BALANCE_REPLY",
		Subjects: []string{"balance.result.balanceapi.>"},
		Replicas: 1,
		MaxAge:   time.Hour,
	}

	tests := map[string]ReplyStreamOpts{
		"empty name": func() ReplyStreamOpts {
			o := base
			o.Name = ""
			return o
		}(),
		"empty subjects": func() ReplyStreamOpts {
			o := base
			o.Subjects = nil
			return o
		}(),
		"zero replicas": func() ReplyStreamOpts {
			o := base
			o.Replicas = 0
			return o
		}(),
		"negative replicas": func() ReplyStreamOpts {
			o := base
			o.Replicas = -1
			return o
		}(),
		"zero max age": func() ReplyStreamOpts {
			o := base
			o.MaxAge = 0
			return o
		}(),
		"negative max age": func() ReplyStreamOpts {
			o := base
			o.MaxAge = -time.Second
			return o
		}(),
	}

	for name, opts := range tests {
		opts := opts
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := EnsureReplyStream(context.Background(), nil, opts)
			if !errors.Is(err, ErrInvalidStreamOpts) {
				t.Fatalf("expected ErrInvalidStreamOpts, got %v", err)
			}
		})
	}
}

func TestEnsureReplyStream_ValidOptsPassValidation(t *testing.T) {
	t.Parallel()

	opts := ReplyStreamOpts{
		Name:     "BALANCE_REPLY",
		Subjects: []string{"balance.result.balanceapi.>"},
		Replicas: 1,
		MaxAge:   time.Hour,
	}

	if err := opts.validate(); err != nil {
		t.Fatalf("expected valid opts to pass, got %v", err)
	}
}

func TestReplyStreamConfigMatches(t *testing.T) {
	t.Parallel()

	opts := ReplyStreamOpts{
		Name:     "BALANCE_REPLY",
		Subjects: []string{"balance.result.balanceapi.>"},
		Replicas: 1,
		MaxAge:   time.Hour,
	}

	matching := streamConfigFromOpts(opts)
	if !replyStreamConfigMatches(matching, opts) {
		t.Fatalf("expected matching config to be reported as matching")
	}

	diffReplicas := matching
	diffReplicas.Replicas = 3
	if replyStreamConfigMatches(diffReplicas, opts) {
		t.Fatalf("expected differing replicas to be reported as not matching")
	}

	diffMaxAge := matching
	diffMaxAge.MaxAge = 2 * time.Hour
	if replyStreamConfigMatches(diffMaxAge, opts) {
		t.Fatalf("expected differing max age to be reported as not matching")
	}

	diffSubjects := matching
	diffSubjects.Subjects = []string{"balance.result.balanceapi.other.>"}
	if replyStreamConfigMatches(diffSubjects, opts) {
		t.Fatalf("expected differing subjects to be reported as not matching")
	}
}
