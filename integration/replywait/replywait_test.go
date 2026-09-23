//go:build integration

package replywait_test

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/replywait"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"go.uber.org/fx"
)

const correlationHeader = "Correlation-Id"

func uniqueStreamName(t *testing.T) string {
	t.Helper()
	return "REPLYWAIT_" + fmt.Sprintf("%X", uuid.New())
}

func replySubject(instance string) string {
	return "domain.result.replywait." + instance
}

func replySubjectWildcard() string {
	return "domain.result.replywait.>"
}

func correlationFromHeader(msg jetstream.Msg) (string, error) {
	id := msg.Headers().Get(correlationHeader)
	if id == "" {
		return "", fmt.Errorf("replywait: missing %s header", correlationHeader)
	}
	return id, nil
}

func testConfig(stream, instance string, defaultTimeout, inactiveThreshold, drainTimeout time.Duration) replywait.Config {
	return replywait.Config{
		Stream:            stream,
		ReplySubject:      replySubject(instance),
		CorrelationFunc:   correlationFromHeader,
		DefaultTimeout:    defaultTimeout,
		InactiveThreshold: inactiveThreshold,
		DrainTimeout:      drainTimeout,
	}
}

func ensureStream(t *testing.T, js jetstream.JetStream, name string, maxAge time.Duration) {
	t.Helper()

	_, err := replywait.EnsureReplyStream(context.Background(), js, replywait.ReplyStreamOpts{
		Name:     name,
		Subjects: []string{replySubjectWildcard()},
		Replicas: 1,
		MaxAge:   maxAge,
	})
	require.NoError(t, err)
}

func fakeExecutorReply(ctx context.Context, js jetstream.JetStream, subject, correlationID string) error {
	msg := &natsgo.Msg{
		Subject: subject,
		Header:  natsgo.Header{correlationHeader: []string{correlationID}},
		Data:    []byte(correlationID),
	}

	_, err := js.PublishMsg(ctx, msg)
	return err
}

func startModule(t *testing.T, js jetstream.JetStream, cfg replywait.Config, observer replywait.Observer) (*replywait.ReplyWaiter, *fx.App) {
	t.Helper()

	var rw *replywait.ReplyWaiter

	opts := []fx.Option{
		fx.NopLogger,
		fx.Supply(fx.Annotate(js,
			fx.As(new(jetstream.JetStream)),
		)),
		fx.Supply(cfg),
		replywait.Module,
		fx.Populate(&rw),
	}
	if observer != nil {
		opts = append(opts, fx.Supply(fx.Annotate(observer,
			fx.As(new(replywait.Observer)),
		)))
	}

	app := fx.New(opts...)

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	return rw, app
}

func stopModule(t *testing.T, app *fx.App) {
	t.Helper()

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Stop(stopCtx))
}

func startNATSContainer(t *testing.T) (*tcnats.NATSContainer, string) {
	t.Helper()
	ctx := context.Background()

	container, err := tcnats.Run(ctx, "nats:2.10-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	return container, url
}

func dialJetStream(t *testing.T, url string, opts ...natsgo.Option) (jetstream.JetStream, *natsgo.Conn) {
	t.Helper()

	var conn *natsgo.Conn
	require.Eventually(t, func() bool {
		var connErr error
		conn, connErr = natsgo.Connect(url, opts...)
		return connErr == nil
	}, 30*time.Second, 500*time.Millisecond)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	return js, conn
}

type fixedPortNATSContainer struct {
	id  string
	url string
}

func freeTCPPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	return port
}

func startFixedPortNATSContainer(t *testing.T) *fixedPortNATSContainer {
	t.Helper()

	port := freeTCPPort(t)
	out, err := exec.Command(
		"docker", "run", "-d", "--rm",
		"-p", "127.0.0.1:"+port+":4222",
		"nats:2.10-alpine", "-DV", "-js",
	).CombinedOutput()
	require.NoErrorf(t, err, "docker run nats failed: %s", out)

	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", id).Run()
	})

	url := "nats://127.0.0.1:" + port

	require.Eventually(t, func() bool {
		conn, connErr := natsgo.Connect(url, natsgo.Timeout(2*time.Second))
		if connErr != nil {
			return false
		}
		conn.Close()
		return true
	}, 30*time.Second, 500*time.Millisecond, "expected fixed-port NATS container to accept connections")

	return &fixedPortNATSContainer{id: id, url: url}
}

func (c *fixedPortNATSContainer) restart(t *testing.T) {
	t.Helper()

	out, err := exec.Command("docker", "restart", c.id).CombinedOutput()
	require.NoErrorf(t, err, "docker restart nats failed: %s", out)
}

func consumerCount(t *testing.T, js jetstream.JetStream, streamName string) int {
	t.Helper()

	stream, err := js.Stream(context.Background(), streamName)
	require.NoError(t, err)

	lister := stream.ListConsumers(context.Background())
	count := 0
	for range lister.Info() {
		count++
	}
	require.NoError(t, lister.Err())

	return count
}

type fakeObserver struct {
	mu      sync.Mutex
	late    []string
	rejects []error
}

func (f *fakeObserver) WaitStarted(string)                {}
func (f *fakeObserver) WaitResolved(string, time.Duration) {}
func (f *fakeObserver) WaitTimedOut(string)                {}
func (f *fakeObserver) WaitDrained(string)                 {}
func (f *fakeObserver) Inflight(int)                       {}

func (f *fakeObserver) ReplyLate(correlationID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.late = append(f.late, correlationID)
}

func (f *fakeObserver) ReplyRejected(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejects = append(f.rejects, err)
}

func (f *fakeObserver) lateSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.late))
	copy(out, f.late)
	return out
}

func TestEnsureReplyStreamIdempotentAndUpdatesMaxAge(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	name := uniqueStreamName(t)
	ctx := context.Background()

	first, err := replywait.EnsureReplyStream(ctx, js, replywait.ReplyStreamOpts{
		Name:     name,
		Subjects: []string{replySubjectWildcard()},
		Replicas: 1,
		MaxAge:   time.Minute,
	})
	require.NoError(t, err)

	firstInfo, err := first.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, time.Minute, firstInfo.Config.MaxAge)

	second, err := replywait.EnsureReplyStream(ctx, js, replywait.ReplyStreamOpts{
		Name:     name,
		Subjects: []string{replySubjectWildcard()},
		Replicas: 1,
		MaxAge:   time.Minute,
	})
	require.NoError(t, err)

	secondInfo, err := second.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, firstInfo.Config, secondInfo.Config)

	updated, err := replywait.EnsureReplyStream(ctx, js, replywait.ReplyStreamOpts{
		Name:     name,
		Subjects: []string{replySubjectWildcard()},
		Replicas: 1,
		MaxAge:   2 * time.Minute,
	})
	require.NoError(t, err)

	updatedInfo, err := updated.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, updatedInfo.Config.MaxAge)

	streamCount := 0
	lister := js.StreamNames(ctx)
	for range lister.Name() {
		streamCount++
	}
	require.NoError(t, lister.Err())
	require.Equal(t, 1, streamCount)
}

func TestTwoWaitersIsolatedByPodInstance(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	cfgA := testConfig(stream, "pod-a", 5*time.Second, 10*time.Second, time.Second)
	cfgB := testConfig(stream, "pod-b", 5*time.Second, 10*time.Second, time.Second)

	rwA, appA := startModule(t, js, cfgA, nil)
	rwB, appB := startModule(t, js, cfgB, nil)
	t.Cleanup(func() { stopModule(t, appA) })
	t.Cleanup(func() { stopModule(t, appB) })

	execute := func(rw *replywait.ReplyWaiter, id string) (jetstream.Msg, error) {
		return rw.Request(context.Background(), id, func(ctx context.Context) error {
			return fakeExecutorReply(ctx, js, rw.ReplySubject(), id)
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var msgA, msgB jetstream.Msg
	var errA, errB error

	go func() {
		defer wg.Done()
		msgA, errA = execute(rwA, "corr-a")
	}()
	go func() {
		defer wg.Done()
		msgB, errB = execute(rwB, "corr-b")
	}()
	wg.Wait()

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Equal(t, rwA.ReplySubject(), msgA.Subject())
	require.Equal(t, rwB.ReplySubject(), msgB.Subject())
	require.Equal(t, "corr-a", string(msgA.Data()))
	require.Equal(t, "corr-b", string(msgB.Data()))
}

func TestFiveHundredConcurrentRequestsNoLossNoCrossTalk(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	cfg := testConfig(stream, "pod-bulk", 10*time.Second, 20*time.Second, time.Second)
	rw, app := startModule(t, js, cfg, nil)
	t.Cleanup(func() { stopModule(t, app) })

	const total = 500

	var wg sync.WaitGroup
	wg.Add(total)

	results := make([]string, total)
	errs := make([]error, total)

	for i := 0; i < total; i++ {
		go func(i int) {
			defer wg.Done()

			id := fmt.Sprintf("bulk-%d-%s", i, uuid.NewString())
			msg, err := rw.Request(context.Background(), id, func(ctx context.Context) error {
				return fakeExecutorReply(ctx, js, rw.ReplySubject(), id)
			})
			if err != nil {
				errs[i] = err
				return
			}

			results[i] = string(msg.Data())
			if results[i] != id {
				errs[i] = fmt.Errorf("cross-talk: expected %s got %s", id, results[i])
			}
		}(i)
	}
	wg.Wait()

	seen := make(map[string]int, total)
	for i, id := range results {
		require.NoErrorf(t, errs[i], "request %d failed", i)
		seen[id]++
	}
	require.Len(t, seen, total)
	for id, count := range seen {
		require.Equalf(t, 1, count, "correlation %s delivered %d times", id, count)
	}
}

func TestLateReplyAfterTimeoutIsReportedAsReplyLate(t *testing.T) {
	t.Parallel()

	js := testenv.NATS(t)
	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	observer := &fakeObserver{}
	cfg := testConfig(stream, "pod-late", 150*time.Millisecond, time.Second, time.Second)
	rw, app := startModule(t, js, cfg, observer)
	t.Cleanup(func() { stopModule(t, app) })

	id := "late-" + uuid.NewString()

	_, err := rw.Request(context.Background(), id, func(context.Context) error { return nil })
	require.ErrorIs(t, err, replywait.ErrTimeout)

	require.NoError(t, fakeExecutorReply(context.Background(), js, rw.ReplySubject(), id))

	require.Eventually(t, func() bool {
		for _, lateID := range observer.lateSnapshot() {
			if lateID == id {
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond, "expected observer.ReplyLate for %s", id)
}

func TestNATSRestartMidWaitSurfacesTimeoutWithoutHanging(t *testing.T) {
	t.Parallel()

	container := startFixedPortNATSContainer(t)
	js, _ := dialJetStream(t, container.url, natsgo.MaxReconnects(-1), natsgo.ReconnectWait(200*time.Millisecond))

	stream := uniqueStreamName(t)
	ensureStream(t, js, stream, time.Minute)

	cfg := testConfig(stream, "pod-restart", 3*time.Second, 10*time.Second, time.Second)
	rw, app := startModule(t, js, cfg, nil)
	t.Cleanup(func() { stopModule(t, app) })

	id := "restart-" + uuid.NewString()

	waitDone := make(chan error, 1)
	go func() {
		_, err := rw.Request(context.Background(), id, func(context.Context) error { return nil })
		waitDone <- err
	}()

	time.Sleep(300 * time.Millisecond)

	container.restart(t)

	select {
	case err := <-waitDone:
		require.ErrorIs(t, err, replywait.ErrTimeout)
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight wait neither resolved nor surfaced a timeout after NATS restart")
	}
}

func TestEphemeralConsumerRemovedAfterInactiveThresholdOnCrash(t *testing.T) {
	t.Parallel()

	_, url := startNATSContainer(t)
	verifyJS, _ := dialJetStream(t, url)
	podJS, podConn := dialJetStream(t, url)

	stream := uniqueStreamName(t)
	ensureStream(t, verifyJS, stream, time.Minute)

	inactiveThreshold := 500 * time.Millisecond
	cfg := testConfig(stream, "pod-crash", 100*time.Millisecond, inactiveThreshold, time.Second)

	_, app := startModule(t, podJS, cfg, nil)
	t.Cleanup(func() {
		_ = app.Stop(context.Background())
	})

	require.Eventually(t, func() bool {
		return consumerCount(t, verifyJS, stream) == 1
	}, 3*time.Second, 20*time.Millisecond, "expected ephemeral consumer to appear")

	podConn.Close()

	require.Eventually(t, func() bool {
		return consumerCount(t, verifyJS, stream) == 0
	}, 2*inactiveThreshold+5*time.Second, 50*time.Millisecond, "expected ephemeral consumer to be removed after InactiveThreshold")
}
