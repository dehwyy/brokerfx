package replay

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakePub struct {
	mu       sync.Mutex
	msgs     []*nats.Msg
	ids      map[string]bool
	failKeys map[string]int
	delay    map[string]time.Duration
	always   map[string]bool
	onAck    func(n int)
}

func newFakePub() *fakePub {
	return &fakePub{ids: map[string]bool{}, failKeys: map[string]int{}, delay: map[string]time.Duration{}, always: map[string]bool{}}
}

func (f *fakePub) PublishMsg(ctx context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	id := m.Header.Get(nats.MsgIdHdr)
	f.mu.Lock()
	if f.always[id] {
		f.mu.Unlock()
		return nil, errors.New("boom")
	}
	if n := f.failKeys[id]; n > 0 {
		f.failKeys[id] = n - 1
		f.mu.Unlock()
		return nil, errors.New("transient")
	}
	d := f.delay[id]
	f.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	dup := f.ids[id]
	f.ids[id] = true
	if !dup {
		f.msgs = append(f.msgs, m)
	}
	n := len(f.msgs)
	if f.onAck != nil {
		f.onAck(n)
	}
	return &jetstream.PubAck{Duplicate: dup}, nil
}

type sliceSource struct {
	batches [][]Item
	i       int
	failAt  int
}

func (s *sliceSource) Next(context.Context) ([]Item, error) {
	if s.failAt > 0 && s.i+1 == s.failAt {
		return nil, errors.New("source down")
	}
	if s.i >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.i]
	s.i++
	return b, nil
}

func items(from, to int) []Item {
	var out []Item
	for i := from; i <= to; i++ {
		out = append(out, Item{Key: "k" + strconv.Itoa(i), Version: "1", Payload: []byte("p")})
	}
	return out
}

func fastCfg() Config {
	return Config{RatePerSecond: 100000, PublishAttempts: 3, RetryDelay: time.Millisecond, FinalizeTimeout: time.Second}
}

func newTestPub(t *testing.T, f *fakePub, cfg Config, max int64) *Publisher {
	t.Helper()
	p, err := newPublisher(f, func() int64 { return max }, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var testReq = Request{ReplayID: "r1", Subject: "t.replay.r1", EndPayload: []byte("end")}

func lastMeta(t *testing.T, f *fakePub) Meta {
	t.Helper()
	m, err := ParseMeta(f.msgs[len(f.msgs)-1].Header)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRunBatchesThenComplete(t *testing.T) {
	f := newFakePub()
	p := newTestPub(t, f, fastCfg(), 1<<20)
	res, err := p.Run(context.Background(), testReq, &sliceSource{batches: [][]Item{items(1, 2), items(3, 4), items(5, 6)}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 6 || res.Status != StatusComplete {
		t.Fatalf("result %+v", res)
	}
	if len(f.msgs) != 7 {
		t.Fatalf("msgs %d", len(f.msgs))
	}
	for i := 0; i < 6; i++ {
		m, _ := ParseMeta(f.msgs[i].Header)
		if m.Seq != int64(i+1) || m.End {
			t.Fatalf("msg %d meta %+v", i, m)
		}
	}
	m := lastMeta(t, f)
	if !m.End || m.Count != 6 || m.Status != StatusComplete {
		t.Fatalf("marker %+v", m)
	}
	if string(f.msgs[6].Data) != "end" || f.msgs[6].Header.Get(nats.MsgIdHdr) != "r1:end:complete" {
		t.Fatalf("marker msg %+v", f.msgs[6])
	}
	if f.msgs[0].Header.Get(nats.MsgIdHdr) != "r1:k1:1" {
		t.Fatalf("id %s", f.msgs[0].Header.Get(nats.MsgIdHdr))
	}
}

func TestRunEmptySourceCompletes(t *testing.T) {
	f := newFakePub()
	res, err := newTestPub(t, f, fastCfg(), 1<<20).Run(context.Background(), testReq, &sliceSource{})
	if err != nil || res.Status != StatusComplete {
		t.Fatalf("%+v %v", res, err)
	}
	if m := lastMeta(t, f); m.Count != 0 || m.Status != StatusComplete {
		t.Fatalf("%+v", m)
	}
}

func TestRunMarkerAfterSlowAndRetriedSnapshots(t *testing.T) {
	f := newFakePub()
	f.delay["r1:k3:1"] = 30 * time.Millisecond
	f.failKeys["r1:k5:1"] = 1
	var early bool
	f.onAck = func(int) {
		for _, m := range f.msgs {
			if h, _ := ParseMeta(m.Header); h.End && len(f.ids) < 7 {
				early = true
			}
		}
	}
	res, err := newTestPub(t, f, fastCfg(), 1<<20).Run(context.Background(), testReq, &sliceSource{batches: [][]Item{items(1, 6)}})
	if err != nil || res.Published != 6 {
		t.Fatalf("%+v %v", res, err)
	}
	if early {
		t.Fatal("marker before last snapshot ack")
	}
	if !f.ids["r1:k5:1"] || !f.ids["r1:k6:1"] || !f.ids["r1:end:complete"] {
		t.Fatal("missing acks")
	}
	if lastMeta(t, f).End != true {
		t.Fatal("marker not last")
	}
}

func TestRunFailureProducesFailedMarker(t *testing.T) {
	cases := map[string]func(*fakePub, *sliceSource, Request) (int64, *sliceSource){
		"publish": func(f *fakePub, s *sliceSource, r Request) (int64, *sliceSource) {
			f.always["r1:k4:1"] = true
			return 1 << 20, &sliceSource{batches: [][]Item{items(1, 5)}}
		},
		"source": func(f *fakePub, s *sliceSource, r Request) (int64, *sliceSource) {
			return 1 << 20, &sliceSource{batches: [][]Item{items(1, 3), items(4, 6)}, failAt: 2}
		},
		"large": func(f *fakePub, s *sliceSource, r Request) (int64, *sliceSource) {
			return 200, &sliceSource{batches: [][]Item{{{Key: "big", Version: "1", Payload: make([]byte, 500)}}}}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakePub()
			max, src := setup(f, nil, testReq)
			res, err := newTestPub(t, f, fastCfg(), max).Run(context.Background(), testReq, src)
			if err == nil || res.Status != StatusFailed {
				t.Fatalf("%+v %v", res, err)
			}
			if name == "large" && !errors.Is(err, ErrPayloadTooLarge) {
				t.Fatalf("err %v", err)
			}
			m := lastMeta(t, f)
			if !m.End || m.Status != StatusFailed || m.Error == "" {
				t.Fatalf("marker %+v", m)
			}
			if f.ids["r1:end:complete"] {
				t.Fatal("complete published")
			}
		})
	}
}

func TestRunCancelPublishesFailed(t *testing.T) {
	f := newFakePub()
	ctx, cancel := context.WithCancel(context.Background())
	f.onAck = func(n int) {
		if n == 3 {
			cancel()
		}
	}
	res, err := newTestPub(t, f, fastCfg(), 1<<20).Run(ctx, testReq, &sliceSource{batches: [][]Item{items(1, 10)}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("%+v", res)
	}
	if m := lastMeta(t, f); !m.End || m.Status != StatusFailed || m.Count != 3 {
		t.Fatalf("marker %+v", m)
	}
}

func TestRunCountsDuplicates(t *testing.T) {
	f := newFakePub()
	f.ids["r1:k1:1"] = true
	f.ids["r1:k2:1"] = true
	res, err := newTestPub(t, f, fastCfg(), 1<<20).Run(context.Background(), testReq, &sliceSource{batches: [][]Item{items(1, 6)}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicates != 2 || res.Published != 4 {
		t.Fatalf("%+v", res)
	}
	if m := lastMeta(t, f); m.Count != 6 {
		t.Fatalf("%+v", m)
	}
}

func TestRunRateLimited(t *testing.T) {
	f := newFakePub()
	cfg := fastCfg()
	cfg.RatePerSecond = 100
	start := time.Now()
	if _, err := newTestPub(t, f, cfg, 1<<20).Run(context.Background(), testReq, &sliceSource{batches: [][]Item{items(1, 50)}}); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 450*time.Millisecond {
		t.Fatalf("too fast: %s", el)
	}
}

func TestRunRejectsInvalid(t *testing.T) {
	f := newFakePub()
	p := newTestPub(t, f, fastCfg(), 1<<20)
	if _, err := p.Run(context.Background(), Request{ReplayID: "", Subject: "a.b"}, &sliceSource{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	bad := &sliceSource{batches: [][]Item{{{Key: "k", Version: "1", Headers: map[string]string{"Nats-Msg-Id": "x"}}}}}
	_, err := p.Run(context.Background(), testReq, bad)
	if !errors.Is(err, ErrInvalidItem) {
		t.Fatal(err)
	}
	if m := lastMeta(t, f); m.Status != StatusFailed {
		t.Fatalf("%+v", m)
	}
}

type fakeStreams struct {
	nameErr error
}

func (f fakeStreams) StreamNameBySubject(context.Context, string) (string, error) {
	return "", f.nameErr
}

func (f fakeStreams) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, f.nameErr
}

func TestCheckStreamClassifiesErrors(t *testing.T) {
	cases := map[string]struct {
		err        error
		unsuitable bool
	}{
		"not found": {jetstream.ErrStreamNotFound, true},
		"canceled":  {context.Canceled, false},
		"deadline":  {context.DeadlineExceeded, false},
		"timeout":   {nats.ErrTimeout, false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakePub()
			p := newTestPub(t, f, fastCfg(), 1<<20)
			p.streams = fakeStreams{nameErr: c.err}
			src := &sliceSource{batches: [][]Item{items(1, 2)}}
			_, err := p.Run(context.Background(), testReq, src)
			if err == nil {
				t.Fatal("expected error")
			}
			if got := errors.Is(err, ErrStreamUnsuitable); got != c.unsuitable {
				t.Fatalf("unsuitable=%v err=%v", got, err)
			}
			if !errors.Is(err, c.err) {
				t.Fatalf("cause lost: %v", err)
			}
		})
	}
}

func TestCheckStreamCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := newTestPub(t, newFakePub(), fastCfg(), 1<<20)
	p.streams = fakeStreams{nameErr: ctx.Err()}
	_, err := p.Run(ctx, testReq, &sliceSource{batches: [][]Item{items(1, 2)}})
	if errors.Is(err, ErrStreamUnsuitable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}
