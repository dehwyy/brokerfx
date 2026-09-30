package stream

import (
	"errors"
	"testing"

	streamoptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream/stream-opts-builder"
	"github.com/nats-io/nats.go/jetstream"
)

func info(maxBytes int64, stateBytes uint64) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: maxBytes},
		State:  jetstream.StreamState{Bytes: stateBytes},
	}
}

func TestGuardMaxBytes(t *testing.T) {
	const mib = 1 << 20

	tests := []struct {
		name     string
		current  *jetstream.StreamInfo
		desired  int64
		wantMax  int64
		wantKept bool
	}{
		{"empty stream shrinks", info(2048*mib, 0), 256 * mib, 256 * mib, false},
		{"small data shrinks", info(2048*mib, 100*mib), 256 * mib, 256 * mib, false},
		{"exactly half shrinks", info(2048*mib, 128*mib), 256 * mib, 256 * mib, false},
		{"one byte over half is kept", info(2048*mib, 128*mib+1), 256 * mib, 2048 * mib, true},
		{"large data is kept", info(2048*mib, 1024*mib), 256 * mib, 2048 * mib, true},
		{"same limit unchanged", info(256*mib, 255*mib), 256 * mib, 256 * mib, false},
		{"increase applied", info(256*mib, 255*mib), 512 * mib, 512 * mib, false},
		{"unlimited to limited with small data", info(-1, mib), 256 * mib, 256 * mib, false},
		{"unlimited to limited with large data keeps unlimited", info(-1, 200*mib), 256 * mib, -1, true},
		{"limited to unlimited applied", info(256*mib, 255*mib), -1, -1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMax, gotKept := guardMaxBytes(tt.current, tt.desired)
			if gotMax != tt.wantMax || gotKept != tt.wantKept {
				t.Fatalf("got (%d, %v), want (%d, %v)", gotMax, gotKept, tt.wantMax, tt.wantKept)
			}
		})
	}
}

func TestNewFailsOnInvalidEnvBeforeTouchingJetStream(t *testing.T) {
	t.Setenv(streamoptsbuilder.EnvMaxBytes, "not-a-number")

	builder := streamoptsbuilder.NewDefault().WithName("S").WithSubjects([]string{"s.>"})

	got, err := New(Opts{StreamOptsBuilder: builder})
	if got != nil {
		t.Fatalf("stream %v, want nil", got)
	}
	if !errors.Is(err, streamoptsbuilder.ErrInvalidMaxBytesEnv) {
		t.Fatalf("err %v, want ErrInvalidMaxBytesEnv", err)
	}
}
