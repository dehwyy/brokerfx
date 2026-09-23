//go:build integration

package replywait_test

import (
	"context"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/replywait"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type drainWaitResult struct {
	msg jetstream.Msg
	err error
}

func TestModuleDrainSucceedsWhenReplyArrivesBeforeLimit(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	cfg := testConfig(stream, "pod-drain-ok", 5*time.Second, 10*time.Second, 2*time.Second)
	rw, app := startModule(t, js, cfg, nil)

	id := "drain-ok-" + uuid.NewString()

	waitDone := make(chan drainWaitResult, 1)
	go func() {
		msg, err := rw.Request(context.Background(), id, func(context.Context) error { return nil })
		waitDone <- drainWaitResult{msg: msg, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = fakeExecutorReply(context.Background(), js, rw.ReplySubject(), id)
	}()

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))

	select {
	case res := <-waitDone:
		require.NoError(t, res.err)
		require.Equal(t, id, string(res.msg.Data()))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for in-flight request to resolve during drain")
	}
}

func TestModuleDrainReturnsErrDrainedWhenLimitExceeded(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	cfg := testConfig(stream, "pod-drain-fail", 5*time.Second, 10*time.Second, 200*time.Millisecond)
	rw, app := startModule(t, js, cfg, nil)

	id := "drain-fail-" + uuid.NewString()

	waitDone := make(chan drainWaitResult, 1)
	go func() {
		msg, err := rw.Request(context.Background(), id, func(context.Context) error { return nil })
		waitDone <- drainWaitResult{msg: msg, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := app.Stop(stopCtx)
	require.Error(t, err)
	require.ErrorIs(t, err, replywait.ErrDrained)

	select {
	case res := <-waitDone:
		require.ErrorIs(t, res.err, replywait.ErrDrained)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for in-flight request to observe drain failure")
	}
}
