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
	// DefaultMaxBytes is the max_bytes of RoleEvent streams (the default role).
	DefaultMaxBytes int64 = 256 << 20
	// DefaultCriticalMaxBytes is the max_bytes of command, result and reply streams.
	DefaultCriticalMaxBytes int64 = 1 << 30
	// DefaultDLQMaxBytes is the max_bytes of DLQ streams.
	DefaultDLQMaxBytes int64 = 256 << 20

	// EnvMaxBytes overrides DefaultMaxBytes (event streams).
	EnvMaxBytes = "BROKERFX_STREAM_MAX_BYTES"
	// EnvCriticalMaxBytes overrides DefaultCriticalMaxBytes (command, result, reply streams).
	EnvCriticalMaxBytes = "BROKERFX_STREAM_MAX_BYTES_CRITICAL"
	// EnvDLQMaxBytes overrides DefaultDLQMaxBytes (DLQ streams).
	EnvDLQMaxBytes = "BROKERFX_STREAM_MAX_BYTES_DLQ"
)

var (
	ErrInvalidMaxBytesEnv = errors.New("streamoptsbuilder: invalid max bytes env")
	ErrUnknownRole        = errors.New("streamoptsbuilder: unknown stream role")
)

// Role is the explicit purpose of a stream. It selects the limit policy: losing a message on a
// critical stream is a failure, while an event stream may shed its oldest data.
type Role int

const (
	// RoleEvent is the zero value and the default: 256 MiB, DiscardOld.
	RoleEvent Role = iota
	// RoleCommand, RoleResult and RoleReply are critical: 1 GiB, DiscardNew. RoleDLQ is 256 MiB,
	// DiscardNew. A full stream rejects the publish and the outbox retries it instead of
	// silently dropping data.
	RoleCommand
	RoleResult
	RoleReply
	RoleDLQ
)

func (r Role) valid() bool {
	return r >= RoleEvent && r <= RoleDLQ
}

// Critical reports whether the role gets DiscardNew (command, result, reply, DLQ).
func (r Role) Critical() bool {
	return r.valid() && r != RoleEvent
}

func (r Role) String() string {
	switch r {
	case RoleEvent:
		return "event"
	case RoleCommand:
		return "command"
	case RoleResult:
		return "result"
	case RoleReply:
		return "reply"
	case RoleDLQ:
		return "dlq"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// Limits is the role-dependent part of a stream config.
type Limits struct {
	MaxBytes int64
	Discard  jetstream.DiscardPolicy
}

type StreamOptsBuilder struct {
	config jetstream.StreamConfig
	err    error

	role             Role
	eventMaxBytes    int64
	criticalMaxBytes int64
	dlqMaxBytes      int64
	maxBytesExplicit bool
	discardExplicit  bool
	replicasExplicit bool
}

func bytesFromEnv(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return fallback, fmt.Errorf("%w: %s=%q is not a positive integer number of bytes", ErrInvalidMaxBytesEnv, name, raw)
	}

	return value, nil
}

// MaxBytesFromEnv returns the event-stream limit: BROKERFX_STREAM_MAX_BYTES or DefaultMaxBytes.
func MaxBytesFromEnv() (int64, error) {
	return bytesFromEnv(EnvMaxBytes, DefaultMaxBytes)
}

// CriticalMaxBytesFromEnv returns the critical-stream limit: BROKERFX_STREAM_MAX_BYTES_CRITICAL
// or DefaultCriticalMaxBytes.
func CriticalMaxBytesFromEnv() (int64, error) {
	return bytesFromEnv(EnvCriticalMaxBytes, DefaultCriticalMaxBytes)
}

// DLQMaxBytesFromEnv returns the DLQ-stream limit: BROKERFX_STREAM_MAX_BYTES_DLQ or
// DefaultDLQMaxBytes.
func DLQMaxBytesFromEnv() (int64, error) {
	return bytesFromEnv(EnvDLQMaxBytes, DefaultDLQMaxBytes)
}

// LimitsFor returns max_bytes and discard policy of a role, for services that build a raw
// jetstream.StreamConfig instead of using the builder. All env variables are validated
// regardless of the role.
func LimitsFor(role Role) (Limits, error) {
	eventMax, eventErr := MaxBytesFromEnv()
	criticalMax, criticalErr := CriticalMaxBytesFromEnv()
	dlqMax, dlqErr := DLQMaxBytesFromEnv()
	if err := errors.Join(eventErr, criticalErr, dlqErr); err != nil {
		return Limits{}, err
	}

	return limitsFor(role, eventMax, criticalMax, dlqMax)
}

func limitsFor(role Role, eventMax, criticalMax, dlqMax int64) (Limits, error) {
	switch {
	case !role.valid():
		return Limits{}, fmt.Errorf("%w: %d", ErrUnknownRole, int(role))
	case role == RoleDLQ:
		return Limits{MaxBytes: dlqMax, Discard: jetstream.DiscardNew}, nil
	case role.Critical():
		return Limits{MaxBytes: criticalMax, Discard: jetstream.DiscardNew}, nil
	default:
		return Limits{MaxBytes: eventMax, Discard: jetstream.DiscardOld}, nil
	}
}

func NewDefault() *StreamOptsBuilder {
	eventMax, eventErr := MaxBytesFromEnv()
	criticalMax, criticalErr := CriticalMaxBytesFromEnv()
	dlqMax, dlqErr := DLQMaxBytesFromEnv()

	b := &StreamOptsBuilder{
		err:              errors.Join(eventErr, criticalErr, dlqErr),
		eventMaxBytes:    eventMax,
		criticalMaxBytes: criticalMax,
		dlqMaxBytes:      dlqMax,
		config: jetstream.StreamConfig{
			Storage:           jetstream.FileStorage,
			MaxAge:            12 * time.Hour,
			Retention:         jetstream.WorkQueuePolicy,
			MaxMsgsPerSubject: 1_000,
			Compression:       jetstream.S2Compression,
			Replicas:          DefaultReplicas,
			// Duplicates enables JetStream server-side dedup via Nats-Msg-Id: within this
			// window the server suppresses a second publish with the same id. Must be smaller
			// than MaxAge so old dedup records don't outlive the messages they protect, and
			// MUST be >= 2x the outbox relay StallThreshold (default 5m) so a re-published
			// stalled row always lands inside a live dedup window — see outbox.Config.
			Duplicates: 15 * time.Minute,
		},
	}

	return b.WithRole(RoleEvent)
}

// WithRole sets the stream role and applies its limits. An explicit WithMaxBytes or
// WithDiscard wins over the role in either call order. The last WithRole call wins.
func (b *StreamOptsBuilder) WithRole(role Role) *StreamOptsBuilder {
	limits, err := limitsFor(role, b.eventMaxBytes, b.criticalMaxBytes, b.dlqMaxBytes)
	if err != nil {
		b.err = errors.Join(b.err, err)
		return b
	}

	b.role = role
	if !b.replicasExplicit {
		replicas, err := ReplicasForRole(role)
		if err != nil {
			b.err = errors.Join(b.err, err)
		} else {
			b.config.Replicas = replicas
		}
	}
	if !b.maxBytesExplicit {
		b.config.MaxBytes = limits.MaxBytes
	}
	if !b.discardExplicit {
		b.config.Discard = limits.Discard
	}

	return b
}

// WithDiscard sets the discard policy explicitly, overriding the role's policy.
func (b *StreamOptsBuilder) WithDiscard(discard jetstream.DiscardPolicy) *StreamOptsBuilder {
	b.config.Discard = discard
	b.discardExplicit = true
	return b
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
	b.replicasExplicit = true
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
	b.maxBytesExplicit = true
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
