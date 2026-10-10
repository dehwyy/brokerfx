package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/gorm"
)

type fakeTx struct {
	mu        sync.Mutex
	dos       int
	pending   []string
	committed []string
}

func (f *fakeTx) Do(ctx context.Context, _ string, fn func(ctx context.Context) error) error {
	f.mu.Lock()
	f.dos++
	f.pending = nil
	f.mu.Unlock()

	err := fn(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		f.committed = append(f.committed, f.pending...)
	}
	f.pending = nil
	return err
}

func (f *fakeTx) GetConnection(context.Context) *gorm.DB    { return nil }
func (f *fakeTx) GetRawConnection(context.Context) *gorm.DB { return nil }

func (f *fakeTx) record(msgs []jetstream.Msg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range msgs {
		f.pending = append(f.pending, string(msg.Data()))
	}
}

type claimSpy struct {
	calls [][]consumer.ProcessedMessage
	fresh func(rows []consumer.ProcessedMessage) map[string]struct{}
}

func stubClaim(t *testing.T, spy *claimSpy) {
	t.Helper()
	prev := claimKeys
	claimKeys = func(_ *gorm.DB, rows []consumer.ProcessedMessage) (map[string]struct{}, error) {
		spy.calls = append(spy.calls, rows)
		if spy.fresh != nil {
			return spy.fresh(rows), nil
		}
		all := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			all[row.MessageID] = struct{}{}
		}
		return all, nil
	}
	t.Cleanup(func() { claimKeys = prev })
}

func withID(msg *fakeMsg, id string) *fakeMsg {
	msg.headers.Set(jetstream.MsgIDHeader, id)
	return msg
}

func idMsgs(data ...string) []jetstream.Msg {
	out := make([]*fakeMsg, len(data))
	for i, d := range data {
		out[i] = withID(newMsg(d, 1), "id-"+d)
	}
	return asMsgs(out...)
}

type pgErr struct{ code string }

func (e pgErr) Error() string    { return "pg " + e.code }
func (e pgErr) SQLState() string { return e.code }

func mustInTx(t *testing.T, opts InTxOpts) Handler {
	t.Helper()
	handler, err := InTx(opts)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func expectVerdicts(t *testing.T, results []Result, verdict Verdict) {
	t.Helper()
	for i, result := range results {
		if result.Verdict != verdict {
			t.Fatalf("result %d %+v", i, result)
		}
	}
}

func TestInTxOneTransactionPerBatch(t *testing.T) {
	spy := &claimSpy{}
	stubClaim(t, spy)
	tx := &fakeTx{}
	var applied []string
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied = dataOf(msgs)
			tx.record(msgs)
			return nil
		},
	})

	results := handler(context.Background(), idMsgs("a", "b", "c", "d", "e"))

	if tx.dos != 1 || len(spy.calls) != 1 || len(spy.calls[0]) != 5 || len(applied) != 5 {
		t.Fatalf("dos %d claims %d applied %v", tx.dos, len(spy.calls), applied)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxDedupRedelivery(t *testing.T) {
	spy := &claimSpy{fresh: func([]consumer.ProcessedMessage) map[string]struct{} {
		return map[string]struct{}{"id-a": {}, "id-c": {}}
	}}
	stubClaim(t, spy)
	tx := &fakeTx{}
	var applied []string
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied = dataOf(msgs)
			return nil
		},
	})

	results := handler(context.Background(), idMsgs("a", "b", "c"))

	if len(applied) != 2 || applied[0] != "a" || applied[1] != "c" {
		t.Fatalf("applied %v", applied)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxDuplicateKeyInsideBatch(t *testing.T) {
	spy := &claimSpy{}
	stubClaim(t, spy)
	var applied []string
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied = dataOf(msgs)
			return nil
		},
	})

	msgs := asMsgs(withID(newMsg("first", 1), "same"), withID(newMsg("second", 1), "same"))
	results := handler(context.Background(), msgs)

	if len(applied) != 1 || applied[0] != "first" || len(spy.calls[0]) != 1 {
		t.Fatalf("applied %v claims %v", applied, spy.calls)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxBisectsToPoison(t *testing.T) {
	stubClaim(t, &claimSpy{})
	tx := &fakeTx{}
	bad := errors.New("constraint violated")
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			tx.record(msgs)
			for _, msg := range msgs {
				if string(msg.Data()) == "m5" {
					return bad
				}
			}
			return nil
		},
	})

	results := handler(context.Background(), idMsgs("m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"))

	for i, result := range results {
		if i == 5 {
			if result.Verdict != VerdictNak || !errors.Is(result.Err, bad) {
				t.Fatalf("poison result %+v", result)
			}
			continue
		}
		if result.Verdict != VerdictAck {
			t.Fatalf("result %d %+v", i, result)
		}
	}
	counts := map[string]int{}
	for _, d := range tx.committed {
		counts[d]++
	}
	for _, d := range []string{"m0", "m1", "m2", "m3", "m4", "m6", "m7"} {
		if counts[d] != 1 {
			t.Fatalf("committed %v", tx.committed)
		}
	}
	if counts["m5"] != 0 || tx.dos > 7 {
		t.Fatalf("committed %v dos %d", tx.committed, tx.dos)
	}
}

func TestInTxTransientRetriesWithinBudget(t *testing.T) {
	stubClaim(t, &claimSpy{})
	tx := &fakeTx{}
	calls := 0
	handler := mustInTx(t, InTxOpts{
		TxManager:   tx,
		RetryBudget: 2,
		Apply: func(context.Context, []jetstream.Msg) error {
			calls++
			if calls <= 2 {
				return pgErr{code: "40001"}
			}
			return nil
		},
	})

	results := handler(context.Background(), idMsgs("a", "b", "c"))

	if tx.dos != 3 {
		t.Fatalf("dos %d", tx.dos)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxTransientBudgetExhausted(t *testing.T) {
	stubClaim(t, &claimSpy{})
	tx := &fakeTx{}
	handler := mustInTx(t, InTxOpts{
		TxManager:      tx,
		RetryBudget:    2,
		TransientDelay: 3 * time.Second,
		Apply: func(context.Context, []jetstream.Msg) error {
			return pgErr{code: "40P01"}
		},
	})

	results := handler(context.Background(), idMsgs("a", "b", "c"))

	if tx.dos != 3 {
		t.Fatalf("dos %d", tx.dos)
	}
	for i, result := range results {
		if result.Verdict != VerdictNakDelay || result.Delay != 3*time.Second {
			t.Fatalf("result %d %+v", i, result)
		}
	}
}

func TestInTxSingleNonTransientKeepsErr(t *testing.T) {
	stubClaim(t, &claimSpy{})
	tx := &fakeTx{}
	e := errors.New("bad payload")
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Apply: func(context.Context, []jetstream.Msg) error {
			return Poison(e)
		},
	})

	results := handler(context.Background(), idMsgs("a"))

	if tx.dos != 1 || results[0].Verdict != VerdictNak || !errors.Is(results[0].Err, ErrPoison) || !errors.Is(results[0].Err, e) {
		t.Fatalf("dos %d result %+v", tx.dos, results[0])
	}
}

func TestInTxNoDedupSkipsClaim(t *testing.T) {
	spy := &claimSpy{}
	stubClaim(t, spy)
	var applied []string
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		NoDedup:   true,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied = dataOf(msgs)
			return nil
		},
	})

	results := handler(context.Background(), asMsgs(withID(newMsg("a", 1), "same"), withID(newMsg("b", 1), "same")))

	if len(spy.calls) != 0 || len(applied) != 2 {
		t.Fatalf("claims %d applied %v", len(spy.calls), applied)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxSameContentWithoutMsgIDIsDuplicate(t *testing.T) {
	stubClaim(t, &claimSpy{})
	var applied []string
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			applied = dataOf(msgs)
			return nil
		},
	})

	results := handler(context.Background(), asMsgs(newMsg("same", 1), newMsg("same", 1)))

	if len(applied) != 1 {
		t.Fatalf("applied %v", applied)
	}
	expectVerdicts(t, results, VerdictAck)
}

func TestInTxEmptyBatch(t *testing.T) {
	tx := &fakeTx{}
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Apply:     func(context.Context, []jetstream.Msg) error { return nil },
	})

	if results := handler(context.Background(), nil); len(results) != 0 || tx.dos != 0 {
		t.Fatalf("results %v dos %d", results, tx.dos)
	}
}

func TestInTxRejectsNilDeps(t *testing.T) {
	if _, err := InTx(InTxOpts{Apply: func(context.Context, []jetstream.Msg) error { return nil }}); !errors.Is(err, ErrInvalidOpts) {
		t.Fatalf("nil tx: %v", err)
	}
	if _, err := InTx(InTxOpts{TxManager: &fakeTx{}}); !errors.Is(err, ErrInvalidOpts) {
		t.Fatalf("nil apply: %v", err)
	}
}
