package streamoptsbuilder

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	DefaultMaxBytes int64 = 256 << 20

	EnvMaxBytes = "BROKERFX_STREAM_MAX_BYTES"
)

var ErrInvalidMaxBytesEnv = errors.New("streamoptsbuilder: invalid " + EnvMaxBytes)

type StreamOptsBuilder struct {
	config jetstream.StreamConfig
	err    error
}

func MaxBytesFromEnv() (int64, error) {
	raw := strings.TrimSpace(os.Getenv(EnvMaxBytes))
	if raw == "" {
		return DefaultMaxBytes, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return DefaultMaxBytes, fmt.Errorf("%w: %q is not a positive integer number of bytes", ErrInvalidMaxBytesEnv, raw)
	}

	return value, nil
}

func NewDefault() *StreamOptsBuilder {
	maxBytes, err := MaxBytesFromEnv()

	return &StreamOptsBuilder{
		err: err,
		config: jetstream.StreamConfig{
			Storage:           jetstream.FileStorage,
			MaxBytes:          maxBytes,
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

func (b *StreamOptsBuilder) Err() error {
	return b.err
}

func (b *StreamOptsBuilder) Build() jetstream.StreamConfig {
	if b.err != nil {
		panic(b.err)
	}
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
