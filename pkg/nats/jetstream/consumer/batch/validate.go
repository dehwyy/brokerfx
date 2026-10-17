package batch

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func validate(
	cfg jetstream.ConsumerConfig,
	s settings,
) (jetstream.ConsumerConfig, error) {
	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		return cfg, fmt.Errorf(
			"%w: AckPolicy %s, want explicit",
			ErrInvalidOpts,
			cfg.AckPolicy,
		)
	}

	inFlight := s.pipeline * s.max
	if cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = inFlight
	}
	if cfg.MaxAckPending != -1 && cfg.MaxAckPending < inFlight {
		return cfg, fmt.Errorf(
			"%w: MaxAckPending %d < Pipeline*Max %d",
			ErrMaxAckPending,
			cfg.MaxAckPending,
			inFlight,
		)
	}

	if s.lingerMax > 0 {
		ackWait := cfg.AckWait
		if ackWait <= 0 {
			ackWait = defaultAckWait
		}
		if s.lingerMax >= ackWait/4 {
			return cfg, fmt.Errorf(
				"%w: LingerMax %s, want below AckWait/4 %s",
				ErrInvalidOpts,
				s.lingerMax,
				ackWait/4,
			)
		}
	}

	if cfg.MaxDeliver > 0 && (s.maxAttempts < 0 || cfg.MaxDeliver <= s.maxAttempts) {
		return cfg, fmt.Errorf(
			"%w: MaxDeliver %d, MaxAttempts %d",
			ErrMaxDeliver,
			cfg.MaxDeliver,
			s.maxAttempts,
		)
	}

	return cfg, nil
}
