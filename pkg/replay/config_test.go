package replay

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	c := DefaultConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.RatePerSecond != 200 || c.PublishAttempts != 5 || c.RetryDelay != time.Second || c.FinalizeTimeout != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestConfigRejectsNonPositive(t *testing.T) {
	mutations := map[string]func(*Config){
		"rate zero":       func(c *Config) { c.RatePerSecond = 0 },
		"rate negative":   func(c *Config) { c.RatePerSecond = -1 },
		"attempts zero":   func(c *Config) { c.PublishAttempts = 0 },
		"retry zero":      func(c *Config) { c.RetryDelay = 0 },
		"finalize negate": func(c *Config) { c.FinalizeTimeout = -time.Second },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := DefaultConfig()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
