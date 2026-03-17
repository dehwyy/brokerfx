package timedactor

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
)

// Config holds settings for the TimedActor distributed timer.
type Config struct {
	// BucketName is the NATS KV bucket name used to store timer entries.
	// Default: "TimedActorBucket".
	BucketName string

	// CheckInterval is the safety-net polling interval. Even if a Watch event
	// is missed (e.g. due to reconnect), the actor will re-scan for overdue
	// keys at this frequency. Default: 30s.
	CheckInterval time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BucketName:    "TimedActorBucket",
		CheckInterval: 30 * time.Second,
	}
}

// Deps defines the dependencies needed by TimedActor, injected via Uber fx.
type Deps struct {
	fx.In

	JS     jetstream.JetStream
	Config Config `optional:"true"`
}
