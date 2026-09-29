package replay

import (
	"fmt"
	"time"
)

type Config struct {
	RatePerSecond   int
	PublishAttempts int
	RetryDelay      time.Duration
	FinalizeTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		RatePerSecond:   200,
		PublishAttempts: 5,
		RetryDelay:      time.Second,
		FinalizeTimeout: 5 * time.Second,
	}
}

func (c Config) Validate() error {
	if c.RatePerSecond <= 0 {
		return fmt.Errorf("replay: RatePerSecond must be positive, got %d", c.RatePerSecond)
	}
	if c.PublishAttempts <= 0 {
		return fmt.Errorf("replay: PublishAttempts must be positive, got %d", c.PublishAttempts)
	}
	if c.RetryDelay <= 0 {
		return fmt.Errorf("replay: RetryDelay must be positive, got %s", c.RetryDelay)
	}
	if c.FinalizeTimeout <= 0 {
		return fmt.Errorf("replay: FinalizeTimeout must be positive, got %s", c.FinalizeTimeout)
	}
	return nil
}
