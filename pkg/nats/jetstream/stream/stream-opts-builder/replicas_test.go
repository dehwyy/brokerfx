package streamoptsbuilder

import (
	"errors"
	"testing"
)

func clearReplicaEnv(t *testing.T) {
	t.Helper()
	t.Setenv(EnvReplicas, "")
	for _, scope := range []ReplicaScope{ScopeEvent, ScopeCommand, ScopeResult, ScopeReply, ScopeDLQ, ScopeKV} {
		t.Setenv(EnvReplicasFor(scope), "")
	}
}

func TestReplicasDefaultIsOne(t *testing.T) {
	clearReplicaEnv(t)

	got, err := ReplicasFor(ScopeKV)
	if err != nil || got != 1 {
		t.Fatalf("got %d, %v", got, err)
	}
}

func TestReplicasGlobalEnv(t *testing.T) {
	clearReplicaEnv(t)
	t.Setenv(EnvReplicas, "3")

	for _, scope := range []ReplicaScope{ScopeEvent, ScopeCommand, ScopeDLQ, ScopeKV} {
		got, err := ReplicasFor(scope)
		if err != nil || got != 3 {
			t.Fatalf("%s: got %d, %v", scope, got, err)
		}
	}
}

func TestReplicasScopeEnvBeatsGlobal(t *testing.T) {
	clearReplicaEnv(t)
	t.Setenv(EnvReplicas, "3")
	t.Setenv(EnvReplicasFor(ScopeKV), "1")

	if got, err := ReplicasFor(ScopeKV); err != nil || got != 1 {
		t.Fatalf("kv: got %d, %v", got, err)
	}
	if got, err := ReplicasFor(ScopeCommand); err != nil || got != 3 {
		t.Fatalf("command: got %d, %v", got, err)
	}
}

func TestReplicasInvalidEnv(t *testing.T) {
	for _, raw := range []string{"0", "-1", "6", "abc", "1.5"} {
		clearReplicaEnv(t)
		t.Setenv(EnvReplicas, raw)

		if _, err := ReplicasFor(ScopeEvent); !errors.Is(err, ErrInvalidReplicasEnv) {
			t.Fatalf("%q: err %v", raw, err)
		}
	}
}

func TestBuilderUsesRoleReplicas(t *testing.T) {
	clearReplicaEnv(t)
	t.Setenv(EnvReplicas, "3")
	t.Setenv(EnvReplicasFor(ScopeDLQ), "1")

	event := NewDefault().WithName("E").WithSubjects([]string{"e.>"}).Build()
	command := NewDefault().WithRole(RoleCommand).WithName("C").WithSubjects([]string{"c.>"}).Build()
	dlq := NewDefault().WithRole(RoleDLQ).WithName("D").WithSubjects([]string{"d.>"}).Build()

	if event.Replicas != 3 || command.Replicas != 3 || dlq.Replicas != 1 {
		t.Fatalf("event=%d command=%d dlq=%d", event.Replicas, command.Replicas, dlq.Replicas)
	}
}

func TestBuilderExplicitReplicasWinOverEnvInEitherOrder(t *testing.T) {
	clearReplicaEnv(t)
	t.Setenv(EnvReplicas, "3")

	before := NewDefault().WithReplicas(1).WithRole(RoleCommand).WithName("A").WithSubjects([]string{"a.>"}).Build()
	after := NewDefault().WithRole(RoleCommand).WithReplicas(1).WithName("B").WithSubjects([]string{"b.>"}).Build()

	if before.Replicas != 1 || after.Replicas != 1 {
		t.Fatalf("before=%d after=%d", before.Replicas, after.Replicas)
	}
}

func TestBuilderInvalidReplicasEnvIsBuilderError(t *testing.T) {
	clearReplicaEnv(t)
	t.Setenv(EnvReplicas, "nope")

	b := NewDefault().WithName("S").WithSubjects([]string{"s.>"})
	if !errors.Is(b.Err(), ErrInvalidReplicasEnv) {
		t.Fatalf("err %v", b.Err())
	}
}
