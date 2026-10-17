package durable

import (
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const DefaultPinnedTTL = 2 * time.Minute

var groupName = regexp.MustCompile(`^[a-zA-Z0-9/_=-]{1,16}$`)

func ValidatePinned(
	cfg jetstream.ConsumerConfig,
	minTTL time.Duration,
) error {
	if cfg.PriorityPolicy != jetstream.PriorityPolicyPinned {
		return nil
	}

	if len(cfg.PriorityGroups) != 1 {
		return fmt.Errorf(
			"%w: %d priority groups %v, want one",
			ErrInvalidPinned,
			len(cfg.PriorityGroups),
			cfg.PriorityGroups,
		)
	}
	if !groupName.MatchString(cfg.PriorityGroups[0]) {
		return fmt.Errorf(
			"%w: group %q, want 1..16 of [a-zA-Z0-9/_=-]",
			ErrInvalidPinned,
			cfg.PriorityGroups[0],
		)
	}

	ttl := cfg.PinnedTTL
	if ttl <= 0 {
		ttl = DefaultPinnedTTL
	}
	if ttl <= minTTL {
		return fmt.Errorf(
			"%w: PinnedTTL %s, want above %s",
			ErrInvalidPinned,
			ttl,
			minTTL,
		)
	}

	return nil
}
