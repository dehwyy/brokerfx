package batch

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/gorm"
)

type unclaimSpy struct {
	keys [][]string
}

func stubUnclaim(t *testing.T, spy *unclaimSpy) {
	t.Helper()
	prev := unclaimKeys
	unclaimKeys = func(_ *gorm.DB, keys []string) error {
		spy.keys = append(
			spy.keys,
			append([]string(nil), keys...),
		)
		return nil
	}
	t.Cleanup(func() { unclaimKeys = prev })
}

type observerSpy struct {
	fetches []FetchEvent
	applies []ApplyEvent
	bisects []BisectEvent
	settles []SettleEvent
}

func (o *observerSpy) OnFetch(event FetchEvent)   { o.fetches = append(o.fetches, event) }
func (o *observerSpy) OnApply(event ApplyEvent)   { o.applies = append(o.applies, event) }
func (o *observerSpy) OnBisect(event BisectEvent) { o.bisects = append(o.bisects, event) }
func (o *observerSpy) OnSettle(event SettleEvent) { o.settles = append(o.settles, event) }

func TestInTxRequiresExactlyOneApply(t *testing.T) {
	apply := func(context.Context, []jetstream.Msg) error { return nil }
	applyResults := func(context.Context, []jetstream.Msg) ([]Result, error) { return nil, nil }

	for name, opts := range map[string]InTxOpts{
		"neither": {
			TxManager: &fakeTx{},
		},
		"both": {
			TxManager:    &fakeTx{},
			Apply:        apply,
			ApplyResults: applyResults,
		},
	} {
		if _, err := InTx(opts); !errors.Is(err, ErrInvalidOpts) {
			t.Fatalf("%s: err %v", name, err)
		}
	}
}

func TestInTxApplyResultsVerdictsAndClaims(t *testing.T) {
	stubClaim(t, &claimSpy{})
	unclaimed := &unclaimSpy{}
	stubUnclaim(t, unclaimed)
	tx := &fakeTx{}
	observer := &observerSpy{}

	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Observer:  observer,
		ApplyResults: func(_ context.Context, msgs []jetstream.Msg) ([]Result, error) {
			tx.record(msgs)
			return []Result{
				Ack(),
				Skip(),
				NakDelay(time.Second, errors.New("later")),
				Term(errors.New("bad")),
			}, nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("a", "b", "c", "d"),
	)

	want := []Verdict{VerdictAck, VerdictSkip, VerdictNakDelay, VerdictTerm}
	for i, result := range results {
		if result.Verdict != want[i] {
			t.Fatalf("result %d %+v", i, result)
		}
	}
	if tx.dos != 1 {
		t.Fatalf("transactions %d", tx.dos)
	}
	if len(tx.committed) != 4 {
		t.Fatalf("committed %v", tx.committed)
	}
	if len(unclaimed.keys) != 1 {
		t.Fatalf("unclaim calls %v", unclaimed.keys)
	}
	released := unclaimed.keys[0]
	sort.Strings(released)
	if !reflect.DeepEqual(released, []string{"id-c", "id-d"}) {
		t.Fatalf("released %v", released)
	}
	if len(observer.applies) != 1 || observer.applies[0].Fresh != 4 || observer.applies[0].Err != nil {
		t.Fatalf("applies %+v", observer.applies)
	}
}

func TestInTxApplyResultsDuplicateIsAcked(t *testing.T) {
	stubClaim(t, &claimSpy{
		fresh: func(rows []consumer.ProcessedMessage) map[string]struct{} {
			return map[string]struct{}{
				"id-b": {},
			}
		},
	})
	stubUnclaim(t, &unclaimSpy{})
	var seen []string
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		ApplyResults: func(_ context.Context, msgs []jetstream.Msg) ([]Result, error) {
			seen = dataOf(msgs)
			return []Result{
				NakDelay(time.Second, errors.New("later")),
			}, nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("a", "b"),
	)
	if !reflect.DeepEqual(seen, []string{"b"}) {
		t.Fatalf("applied %v", seen)
	}
	if results[0].Verdict != VerdictAck || results[1].Verdict != VerdictNakDelay {
		t.Fatalf("results %+v", results)
	}
}

func TestInTxApplyResultsLengthMismatchRollsBack(t *testing.T) {
	stubClaim(t, &claimSpy{})
	stubUnclaim(t, &unclaimSpy{})
	tx := &fakeTx{}
	observer := &observerSpy{}
	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		Observer:  observer,
		ApplyResults: func(_ context.Context, msgs []jetstream.Msg) ([]Result, error) {
			tx.record(msgs)
			return []Result{}, nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("a", "b"),
	)
	for i, result := range results {
		if result.Verdict != VerdictNak || !errors.Is(result.Err, ErrMissingResult) {
			t.Fatalf("result %d %+v", i, result)
		}
	}
	if len(tx.committed) != 0 {
		t.Fatalf("committed %v", tx.committed)
	}
	if len(observer.bisects) != 1 {
		t.Fatalf("bisects %+v", observer.bisects)
	}
}

func TestSettleSkipAcksAndObserverCounts(t *testing.T) {
	observer := &observerSpy{}
	c := testConsumer(nil, 0, nil)
	c.observer = observer

	msgs := []*fakeMsg{
		newMsg("a", 1),
		newMsg("b", 1),
		newMsg("c", 1),
		newMsg("d", 1),
	}
	c.settle(
		context.Background(),
		asMsgs(msgs...),
		[]Result{
			Ack(),
			Skip(),
			NakDelay(time.Second, errors.New("later")),
			Term(errors.New("bad")),
		},
	)

	expectSettles(t, msgs[0], "ack")
	expectSettles(t, msgs[1], "ack")
	expectSettles(t, msgs[2], "nakdelay:1s")
	expectSettles(t, msgs[3], "term")
	if len(observer.settles) != 1 {
		t.Fatalf("settles %+v", observer.settles)
	}
	got := observer.settles[0]
	if got.Ack != 1 || got.Skip != 1 || got.Retry != 1 || got.Poison != 1 {
		t.Fatalf("tally %+v", got)
	}
}
