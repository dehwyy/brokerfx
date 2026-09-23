package replywait

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type CorrelationFunc func(msg jetstream.Msg) (string, error)

type Config struct {
	Stream            string
	ReplySubject      string
	CorrelationFunc   CorrelationFunc
	DefaultTimeout    time.Duration
	InactiveThreshold time.Duration
	DrainTimeout      time.Duration
}

func (c Config) Validate() error {
	if c.Stream == "" {
		return fmt.Errorf("%w: stream is required", ErrInvalidConfig)
	}

	if err := validateReplySubject(c.ReplySubject); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	if c.CorrelationFunc == nil {
		return fmt.Errorf("%w: correlation func is required", ErrInvalidConfig)
	}

	if c.InactiveThreshold <= c.DefaultTimeout {
		return fmt.Errorf("%w: inactive threshold must exceed default timeout", ErrInvalidConfig)
	}

	if c.DrainTimeout <= 0 {
		return fmt.Errorf("%w: drain timeout must be positive", ErrInvalidConfig)
	}

	return nil
}

const minReplySubjectTokens = 4

func validateReplySubject(subject string) error {
	if subject == "" {
		return fmt.Errorf("reply subject is required")
	}

	tokens := strings.Split(subject, ".")
	if len(tokens) < minReplySubjectTokens {
		return fmt.Errorf("reply subject must have at least %d tokens", minReplySubjectTokens)
	}

	for _, token := range tokens {
		if token == "" {
			return fmt.Errorf("reply subject token must not be empty")
		}
		if strings.ContainsAny(token, "*> ") {
			return fmt.Errorf("reply subject token must not contain wildcards or spaces")
		}
	}

	return nil
}

func InstanceFromEnv() (string, error) {
	instance := os.Getenv("POD_NAME")
	if instance == "" {
		return "", fmt.Errorf("%w: POD_NAME is empty", ErrInvalidInstance)
	}
	if strings.Contains(instance, ".") {
		return "", fmt.Errorf("%w: POD_NAME must not contain '.'", ErrInvalidInstance)
	}

	return instance, nil
}
