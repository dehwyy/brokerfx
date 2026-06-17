package streamoptsbuilder

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type StreamOptsBuilder struct {
	config jetstream.StreamConfig
}

func NewDefault() *StreamOptsBuilder {
	return &StreamOptsBuilder{
		config: jetstream.StreamConfig{
			Storage:           jetstream.FileStorage,
			MaxBytes:          2 << 30,
			MaxAge:            12 * time.Hour,
			Retention:         jetstream.WorkQueuePolicy,
			MaxMsgsPerSubject: 1_000,
			Compression:       jetstream.S2Compression,
			Replicas:          1,
			// Duplicates enables JetStream server-side dedup via Nats-Msg-Id: within this
			// window the server suppresses a second publish with the same id. Must be smaller
			// than MaxAge so old dedup records don't outlive the messages they protect, and
			// MUST be >= 2x the outbox relay StallThreshold (default 5m) so a re-published
			// stalled row always lands inside a live dedup window — see outbox.Config.
			Duplicates: 15 * time.Minute,
		},
	}
}

func (b *StreamOptsBuilder) Build() jetstream.StreamConfig {
	if b.config.Name == "" {
		panic("name is required")
	}
	if b.config.Subjects == nil {
		panic("subjects are required")
	}

	return b.config
}

// REQUIRED
func (b *StreamOptsBuilder) WithName(
	name string,
) *StreamOptsBuilder {
	b.config.Name = name
	return b
}

func (b *StreamOptsBuilder) WithSubjects(
	subjects []string,
) *StreamOptsBuilder {
	b.config.Subjects = subjects
	return b
}

// -----------

// OPTIONAL
func (b *StreamOptsBuilder) WithReplicas(
	replicas int,
) *StreamOptsBuilder {
	b.config.Replicas = replicas
	return b
}

func (b *StreamOptsBuilder) WithDescription(
	description string,
) *StreamOptsBuilder {
	b.config.Description = description
	return b
}

func (b *StreamOptsBuilder) WithAllowRollup(
	allowRollup bool,
) *StreamOptsBuilder {
	b.config.AllowRollup = allowRollup
	return b
}

func (b *StreamOptsBuilder) WithAllowDirect(
	allowDirect bool,
) *StreamOptsBuilder {
	b.config.AllowDirect = allowDirect
	return b
}

func (b *StreamOptsBuilder) WithMaxMsgsPerSubject(
	maxMsgsPerSubject int64,
) *StreamOptsBuilder {
	b.config.MaxMsgsPerSubject = maxMsgsPerSubject
	return b
}

func (b *StreamOptsBuilder) WithCompression(
	compression jetstream.StoreCompression,
) *StreamOptsBuilder {
	b.config.Compression = compression
	return b
}

func (b *StreamOptsBuilder) WithStorage(
	storage jetstream.StorageType,
) *StreamOptsBuilder {
	b.config.Storage = storage
	return b
}

func (b *StreamOptsBuilder) WithMaxBytes(
	maxBytes int64,
) *StreamOptsBuilder {
	b.config.MaxBytes = maxBytes
	return b
}

func (b *StreamOptsBuilder) WithMaxAge(
	maxAge time.Duration,
) *StreamOptsBuilder {
	b.config.MaxAge = maxAge
	return b
}

func (b *StreamOptsBuilder) WithRetentionPolicy(
	retention jetstream.RetentionPolicy,
) *StreamOptsBuilder {
	b.config.Retention = retention
	return b
}

// WithDuplicates sets the deduplication window: publishes sharing a Nats-Msg-Id value
// within this window are coalesced server-side. Pass 0 to disable dedup entirely.
func (b *StreamOptsBuilder) WithDuplicates(
	window time.Duration,
) *StreamOptsBuilder {
	b.config.Duplicates = window
	return b
}
