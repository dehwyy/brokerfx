package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/middleware"
)

type flightMsg struct {
	jetstream.Msg

	delivered  uint64
	metaErr    error
	inProgress atomic.Int32

	mu     sync.Mutex
	calls  []string
	delays []time.Duration
}

func (m *flightMsg) Subject() string { return "test.flight" }

func (m *flightMsg) Headers() nats.Header { return nil }

func (m *flightMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{
		NumDelivered: m.delivered,
	}, nil
}

func (m *flightMsg) InProgress() error {
	m.inProgress.Add(1)
	return nil
}

func (m *flightMsg) record(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(
		m.calls,
		name,
	)
}

func (m *flightMsg) Ack() error {
	m.record("ack")
	return nil
}

func (m *flightMsg) Nak() error {
	m.record("nak")
	return nil
}

func (m *flightMsg) NakWithDelay(delay time.Duration) error {
	m.record("nakdelay")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delays = append(
		m.delays,
		delay,
	)
	return nil
}

func (m *flightMsg) snapshot() ([]string, []time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.calls), slices.Clone(m.delays)
}

type fakeConsumeCtx struct {
	jetstream.ConsumeContext

	once    sync.Once
	closed  chan struct{}
	drained atomic.Int32
}

func newFakeConsumeCtx() *fakeConsumeCtx {
	return &fakeConsumeCtx{
		closed: make(chan struct{}),
	}
}

func (c *fakeConsumeCtx) Drain() {
	c.drained.Add(1)
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *fakeConsumeCtx) Closed() <-chan struct{} {
	return c.closed
}

func waitFor(
	t *testing.T,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDispatchBoundsInFlight(t *testing.T) {
	const limit = 3
	inFlight := newPool(limit)

	var running, peak atomic.Int32
	opts := Opts{
		HandlerFunc: func(context.Context, jetstream.Msg) error {
			now := running.Add(1)
			for {
				seen := peak.Load()
				if now <= seen || peak.CompareAndSwap(
					seen,
					now,
				) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			running.Add(-1)
			return nil
		},
	}
	handle := dispatch(
		opts,
		inFlight,
	)

	msgs := make(
		[]*flightMsg,
		12,
	)
	for i := range msgs {
		msgs[i] = &flightMsg{
			delivered: 1,
		}
		handle(msgs[i])
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()
	if err := inFlight.wait(ctx); err != nil {
		t.Fatal(err)
	}

	if got := peak.Load(); got != limit {
		t.Fatalf(
			"peak in flight = %d, want %d",
			got,
			limit,
		)
	}
	for i, msg := range msgs {
		calls, _ := msg.snapshot()
		if !slices.Equal(
			calls,
			[]string{"ack"},
		) {
			t.Fatalf(
				"msg %d calls = %v, want [ack]",
				i,
				calls,
			)
		}
	}
}

func TestDispatchZeroKeepsUnboundedGoroutines(t *testing.T) {
	const total = 12
	release := make(chan struct{})
	var running atomic.Int32
	opts := Opts{
		HandlerFunc: func(context.Context, jetstream.Msg) error {
			running.Add(1)
			<-release
			return nil
		},
	}
	handle := dispatch(
		opts,
		newPool(0),
	)

	for range total {
		handle(&flightMsg{
			delivered: 1,
		})
	}
	waitFor(
		t,
		func() bool {
			return running.Load() == total
		},
	)
	close(release)
}

func TestDispatchWaitingMessageKeepsHeartbeat(t *testing.T) {
	restore := inProgressIntervalForTest(10 * time.Millisecond)
	defer restore()

	inFlight := newPool(1)
	release := make(chan struct{})
	first := &flightMsg{
		delivered: 1,
	}
	second := &flightMsg{
		delivered: 1,
	}
	var secondStarted atomic.Bool
	opts := Opts{
		HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
			if msg == first {
				<-release
				return nil
			}
			secondStarted.Store(true)
			return nil
		},
	}
	handle := dispatch(
		opts,
		inFlight,
	)

	handle(first)
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		handle(second)
	}()

	waitFor(
		t,
		func() bool {
			return second.inProgress.Load() >= 3
		},
	)
	if secondStarted.Load() {
		t.Fatal("second handler started while the only slot was busy")
	}

	close(release)
	<-waiting
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()
	if err := inFlight.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if !secondStarted.Load() {
		t.Fatal("second handler never started")
	}
}

func TestDrainNaksMessageWaitingForSlot(t *testing.T) {
	restore := inProgressIntervalForTest(10 * time.Millisecond)
	defer restore()

	inFlight := newPool(1)
	release := make(chan struct{})
	first := &flightMsg{
		delivered: 1,
	}
	second := &flightMsg{
		delivered: 1,
	}
	var secondStarted atomic.Bool
	opts := Opts{
		HandlerFunc: func(_ context.Context, msg jetstream.Msg) error {
			if msg == first {
				<-release
				return nil
			}
			secondStarted.Store(true)
			return nil
		},
	}
	handle := dispatch(
		opts,
		inFlight,
	)

	handle(first)
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		handle(second)
	}()
	waitFor(
		t,
		func() bool {
			return second.inProgress.Load() >= 1
		},
	)

	drainer := NewDrainer()
	drainer.Append(Consumer{
		consumeCtx: newFakeConsumeCtx(),
		inFlight:   inFlight,
	})
	drained := make(
		chan error,
		1,
	)
	go func() {
		drained <- drainer.DrainContext(context.Background())
	}()

	<-waiting
	calls, _ := second.snapshot()
	if !slices.Equal(
		calls,
		[]string{"nak"},
	) {
		t.Fatalf(
			"waiting msg calls = %v, want [nak]",
			calls,
		)
	}
	beats := second.inProgress.Load()
	select {
	case err := <-drained:
		t.Fatalf(
			"drain returned %v while the first handler was running",
			err,
		)
	case <-time.After(50 * time.Millisecond):
	}
	if second.inProgress.Load() != beats {
		t.Fatal("waiting msg kept its heartbeat after the NAK")
	}

	close(release)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if secondStarted.Load() {
		t.Fatal("waiting msg handler started after the consumer stopped")
	}
	firstCalls, _ := first.snapshot()
	if !slices.Equal(
		firstCalls,
		[]string{"ack"},
	) {
		t.Fatalf(
			"first msg calls = %v, want [ack]",
			firstCalls,
		)
	}
}

func TestProcessNakBackoffGrowsWithDelivery(t *testing.T) {
	cases := []struct {
		delivered uint64
		want      time.Duration
	}{
		{
			delivered: 1,
			want:      time.Second,
		},
		{
			delivered: 2,
			want:      5 * time.Second,
		},
		{
			delivered: 3,
			want:      30 * time.Second,
		},
		{
			delivered: 5,
			want:      5 * time.Minute,
		},
		{
			delivered: 40,
			want:      5 * time.Minute,
		},
	}

	for _, tc := range cases {
		msg := &flightMsg{
			delivered: tc.delivered,
		}
		process(
			msg,
			Opts{
				HandlerFunc: func(context.Context, jetstream.Msg) error {
					return errors.New("transient")
				},
				NakBackoff: DefaultNakDelays,
			},
		)

		calls, delays := msg.snapshot()
		if !slices.Equal(
			calls,
			[]string{"nakdelay"},
		) || !slices.Equal(
			delays,
			[]time.Duration{tc.want},
		) {
			t.Fatalf(
				"delivered %d: calls %v delays %v, want nakdelay %s",
				tc.delivered,
				calls,
				delays,
				tc.want,
			)
		}
	}
}

func TestProcessNakBackoffCoversPanicAndMiddleware(t *testing.T) {
	ladder := []time.Duration{
		time.Second,
		2 * time.Second,
	}
	cases := []struct {
		name string
		msg  *flightMsg
		opts Opts
		want time.Duration
	}{
		{
			name: "panic",
			msg: &flightMsg{
				delivered: 2,
			},
			opts: Opts{
				HandlerFunc: func(context.Context, jetstream.Msg) error {
					panic("boom")
				},
				NakBackoff: ladder,
			},
			want: 2 * time.Second,
		},
		{
			name: "before middleware error",
			msg: &flightMsg{
				delivered: 1,
			},
			opts: Opts{
				HandlerFunc: func(context.Context, jetstream.Msg) error {
					return nil
				},
				BeforeHandlerMiddleware: []middleware.Middleware{
					func(ctx context.Context, _ jetstream.Msg) (context.Context, error) {
						return ctx, errors.New("mw")
					},
				},
				NakBackoff: ladder,
			},
			want: time.Second,
		},
		{
			name: "metadata error uses the first step",
			msg: &flightMsg{
				metaErr: errors.New("no metadata"),
			},
			opts: Opts{
				HandlerFunc: func(context.Context, jetstream.Msg) error {
					return errors.New("transient")
				},
				NakBackoff: ladder,
			},
			want: time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				captureLog(t)
				process(
					tc.msg,
					tc.opts,
				)
				calls, delays := tc.msg.snapshot()
				if !slices.Equal(
					calls,
					[]string{"nakdelay"},
				) || !slices.Equal(
					delays,
					[]time.Duration{tc.want},
				) {
					t.Fatalf(
						"calls %v delays %v, want nakdelay %s",
						calls,
						delays,
						tc.want,
					)
				}
			},
		)
	}
}

func TestProcessWithoutBackoffKeepsPlainNak(t *testing.T) {
	captureLog(t)
	msg := &flightMsg{
		delivered: 3,
	}
	process(
		msg,
		Opts{
			HandlerFunc: func(context.Context, jetstream.Msg) error {
				return errors.New("transient")
			},
		},
	)
	calls, _ := msg.snapshot()
	if !slices.Equal(
		calls,
		[]string{"nak"},
	) {
		t.Fatalf(
			"calls = %v, want [nak]",
			calls,
		)
	}
}

func TestResolveOpts(t *testing.T) {
	cases := []struct {
		name         string
		opts         Opts
		envInFlight  string
		envBackoff   string
		wantInFlight int
		wantBackoff  []time.Duration
		wantErr      bool
	}{
		{
			name: "empty keeps the old behaviour",
		},
		{
			name:         "env sets in flight and default ladder",
			envInFlight:  "32",
			envBackoff:   "true",
			wantInFlight: 32,
			wantBackoff:  DefaultNakDelays,
		},
		{
			name:       "env ladder list",
			envBackoff: "1s, 2s,5s",
			wantBackoff: []time.Duration{
				time.Second,
				2 * time.Second,
				5 * time.Second,
			},
		},
		{
			name: "explicit opts win over env",
			opts: Opts{
				MaxInFlight: 4,
				NakBackoff: []time.Duration{
					time.Second,
				},
			},
			envInFlight:  "32",
			envBackoff:   "true",
			wantInFlight: 4,
			wantBackoff: []time.Duration{
				time.Second,
			},
		},
		{
			name:        "garbage env falls back to defaults",
			envInFlight: "lots",
			envBackoff:  "soon",
		},
		{
			name:        "negative env and zero step fall back",
			envInFlight: "-2",
			envBackoff:  "1s,0s",
		},
		{
			name:       "off keeps immediate nak",
			envBackoff: "off",
		},
		{
			name: "negative explicit in flight is rejected",
			opts: Opts{
				MaxInFlight: -1,
			},
			wantErr: true,
		},
		{
			name: "non positive explicit step is rejected",
			opts: Opts{
				NakBackoff: []time.Duration{
					0,
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				captureLog(t)
				t.Setenv(
					envMaxInFlight,
					tc.envInFlight,
				)
				t.Setenv(
					envNakBackoff,
					tc.envBackoff,
				)

				resolved, err := resolveOpts(tc.opts)
				if tc.wantErr {
					if !errors.Is(
						err,
						ErrInvalidOpts,
					) {
						t.Fatalf(
							"err = %v, want ErrInvalidOpts",
							err,
						)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if resolved.MaxInFlight != tc.wantInFlight {
					t.Fatalf(
						"MaxInFlight = %d, want %d",
						resolved.MaxInFlight,
						tc.wantInFlight,
					)
				}
				if !slices.Equal(
					resolved.NakBackoff,
					tc.wantBackoff,
				) {
					t.Fatalf(
						"NakBackoff = %v, want %v",
						resolved.NakBackoff,
						tc.wantBackoff,
					)
				}
			},
		)
	}
}

func TestHeartbeatForAckWait(t *testing.T) {
	cases := []struct {
		ackWait time.Duration
		want    time.Duration
	}{
		{
			ackWait: 30 * time.Second,
			want:    5 * time.Second,
		},
		{
			ackWait: 0,
			want:    5 * time.Second,
		},
		{
			ackWait: 2 * time.Second,
			want:    2 * time.Second / 3,
		},
		{
			ackWait: 150 * time.Millisecond,
			want:    minHeartbeat,
		},
	}

	for _, tc := range cases {
		if got := heartbeatForAckWait(tc.ackWait); got != tc.want {
			t.Fatalf(
				"heartbeatForAckWait(%s) = %s, want %s",
				tc.ackWait,
				got,
				tc.want,
			)
		}
	}
}

func TestPullMaxMessagesBoundedPullsOne(t *testing.T) {
	if got := pullMaxMessages(0); got != defaultPullMaxMessages {
		t.Fatalf(
			"pullMaxMessages(0) = %d, want %d",
			got,
			defaultPullMaxMessages,
		)
	}
	if got := pullMaxMessages(8); got != boundedPullMaxMessages {
		t.Fatalf(
			"pullMaxMessages(8) = %d, want %d",
			got,
			boundedPullMaxMessages,
		)
	}
}

func TestDrainContextWaitsForInFlight(t *testing.T) {
	inFlight := newPool(2)
	consumeCtx := newFakeConsumeCtx()
	drainer := NewDrainer()
	drainer.Append(Consumer{
		consumeCtx: consumeCtx,
		inFlight:   inFlight,
	})

	if !inFlight.acquire() {
		t.Fatal("acquire failed on an idle pool")
	}
	released := make(
		chan time.Time,
		1,
	)
	go func() {
		time.Sleep(100 * time.Millisecond)
		released <- time.Now()
		inFlight.release()
	}()

	if err := drainer.DrainContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	finished := time.Now()
	if finished.Before(<-released) {
		t.Fatal("drain returned before the in-flight handler finished")
	}
	if consumeCtx.drained.Load() != 1 {
		t.Fatalf(
			"drain calls = %d, want 1",
			consumeCtx.drained.Load(),
		)
	}
}

func TestDrainContextStopsAtDeadline(t *testing.T) {
	inFlight := newPool(1)
	drainer := NewDrainer()
	drainer.Append(Consumer{
		consumeCtx: newFakeConsumeCtx(),
		inFlight:   inFlight,
	})
	if !inFlight.acquire() {
		t.Fatal("acquire failed on an idle pool")
	}
	defer inFlight.release()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()

	if err := drainer.DrainContext(ctx); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf(
			"err = %v, want deadline exceeded",
			err,
		)
	}
}

func TestDrainWithoutPoolKeepsOldBehaviour(t *testing.T) {
	consumeCtx := newFakeConsumeCtx()
	drainer := NewDrainer()
	drainer.Append(Consumer{
		consumeCtx: consumeCtx,
	})

	drainer.Drain()

	if consumeCtx.drained.Load() != 1 {
		t.Fatalf(
			"drain calls = %d, want 1",
			consumeCtx.drained.Load(),
		)
	}
}
