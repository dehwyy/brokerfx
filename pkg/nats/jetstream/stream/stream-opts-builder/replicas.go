package streamoptsbuilder

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"errors"
)

const (
	EnvReplicas = "NATS_JS_REPLICAS"

	DefaultReplicas = 1
	MaxReplicas     = 5
)

var ErrInvalidReplicasEnv = errors.New("streamoptsbuilder: invalid replicas env")

type ReplicaScope string

const (
	ScopeEvent   ReplicaScope = "EVENT"
	ScopeCommand ReplicaScope = "COMMAND"
	ScopeResult  ReplicaScope = "RESULT"
	ScopeReply   ReplicaScope = "REPLY"
	ScopeDLQ     ReplicaScope = "DLQ"
	ScopeKV      ReplicaScope = "KV"
)

func EnvReplicasFor(scope ReplicaScope) string {
	return EnvReplicas + "_" + string(scope)
}

func replicasFromEnv(name string) (int, bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, false, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > MaxReplicas {
		return 0, false, fmt.Errorf("%w: %s=%q must be an integer from 1 to %d", ErrInvalidReplicasEnv, name, raw, MaxReplicas)
	}

	return value, true, nil
}

// ReplicasFor resolves the replica count of a scope: NATS_JS_REPLICAS_<SCOPE>, then
// NATS_JS_REPLICAS, then DefaultReplicas. A malformed value is an error.
func ReplicasFor(scope ReplicaScope) (int, error) {
	if value, ok, err := replicasFromEnv(EnvReplicasFor(scope)); err != nil || ok {
		return value, err
	}

	if value, ok, err := replicasFromEnv(EnvReplicas); err != nil || ok {
		return value, err
	}

	return DefaultReplicas, nil
}

func (r Role) scope() ReplicaScope {
	switch r {
	case RoleCommand:
		return ScopeCommand
	case RoleResult:
		return ScopeResult
	case RoleReply:
		return ScopeReply
	case RoleDLQ:
		return ScopeDLQ
	default:
		return ScopeEvent
	}
}

// ReplicasForRole is ReplicasFor for the scope of a stream role.
func ReplicasForRole(role Role) (int, error) {
	return ReplicasFor(role.scope())
}
