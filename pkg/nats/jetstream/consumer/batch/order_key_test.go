package batch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func orderKeyOfData(msg jetstream.Msg) string {
	return string(msg.Data()[:1])
}

func TestOrderKeyCommitsOtherGroupsAndBlocksTail(t *testing.T) {
	stubClaim(t, &claimSpy{})
	stubUnclaim(t, &unclaimSpy{})
	tx := &fakeTx{}
	failure := errors.New("A2 fails")

	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		OrderKey:  orderKeyOfData,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				if string(msg.Data()) == "A2" {
					return failure
				}
			}
			tx.record(msgs)
			return nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("A1", "B1", "A2", "A3"),
	)

	if results[0].Verdict != VerdictAck || results[1].Verdict != VerdictAck {
		t.Fatalf("A1 and B1 must commit: %+v", results)
	}
	if results[2].Verdict != VerdictNak || !errors.Is(results[2].Err, failure) {
		t.Fatalf("A2 %+v", results[2])
	}
	if results[3].Verdict != VerdictNakDelay || !errors.Is(results[3].Err, ErrOrderBlocked) {
		t.Fatalf("A3 %+v", results[3])
	}
	committed := map[string]bool{}
	for _, data := range tx.committed {
		committed[data] = true
	}
	if !committed["A1"] || !committed["B1"] || committed["A2"] || committed["A3"] {
		t.Fatalf("committed %v", tx.committed)
	}
}

func TestOrderKeyBlockedTailDoesNotCarryPoison(t *testing.T) {
	stubClaim(t, &claimSpy{})
	stubUnclaim(t, &unclaimSpy{})
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		OrderKey:  orderKeyOfData,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				if string(msg.Data()) == "A1" {
					return Poison(errors.New("bad"))
				}
			}
			return nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("A1", "A2"),
	)
	if !errors.Is(results[0].Err, ErrPoison) {
		t.Fatalf("A1 %+v", results[0])
	}
	if errors.Is(results[1].Err, ErrPoison) || !errors.Is(results[1].Err, ErrOrderBlocked) {
		t.Fatalf("A2 %+v", results[1])
	}
}

func TestOrderKeyAckAfterRetryOfSameKeyIsNotCommitted(t *testing.T) {
	stubClaim(t, &claimSpy{})
	unclaimed := &unclaimSpy{}
	stubUnclaim(t, unclaimed)
	tx := &fakeTx{}
	calls := 0

	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		OrderKey:  orderKeyOfData,
		ApplyResults: func(_ context.Context, msgs []jetstream.Msg) ([]Result, error) {
			calls++
			tx.record(msgs)
			results := make(
				[]Result,
				len(msgs),
			)
			for i, msg := range msgs {
				switch string(msg.Data()) {
				case "A1":
					results[i] = NakDelay(time.Second, errors.New("not yet"))
				default:
					results[i] = Ack()
				}
			}
			return results, nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("A1", "B1", "A2", "A3"),
	)

	if results[0].Verdict != VerdictNakDelay || errors.Is(results[0].Err, ErrOrderBlocked) {
		t.Fatalf("A1 %+v", results[0])
	}
	if results[1].Verdict != VerdictAck {
		t.Fatalf("B1 %+v", results[1])
	}
	for _, index := range []int{2, 3} {
		if !errors.Is(results[index].Err, ErrOrderBlocked) {
			t.Fatalf("result %d %+v", index, results[index])
		}
	}
	if calls != 2 {
		t.Fatalf("apply calls %d", calls)
	}
	committed := map[string]bool{}
	for _, data := range tx.committed {
		committed[data] = true
	}
	if committed["A2"] || committed["A3"] || !committed["B1"] {
		t.Fatalf("committed %v", tx.committed)
	}
	if len(unclaimed.keys) != 1 || len(unclaimed.keys[0]) != 1 || unclaimed.keys[0][0] != "id-A1" {
		t.Fatalf("unclaimed %v", unclaimed.keys)
	}
}

func TestOrderKeyEmptyKeepsIndexBisect(t *testing.T) {
	stubClaim(t, &claimSpy{})
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				if string(msg.Data()) == "A2" {
					return errors.New("A2 fails")
				}
			}
			return nil
		},
	})

	results := handler(
		context.Background(),
		idMsgs("A1", "B1", "A2", "A3"),
	)
	if results[3].Verdict != VerdictAck {
		t.Fatalf("without OrderKey A3 commits as before: %+v", results[3])
	}
}

func TestSettleHoldsBlockedWithoutPoison(t *testing.T) {
	poison := &poisonRecorder{}
	c := testConsumer(nil, 2, poison)
	observer := &observerSpy{}
	c.observer = observer

	msg := newMsg("A2", 5)
	c.settle(
		context.Background(),
		asMsgs(msg),
		[]Result{
			NakDelay(0, ErrOrderBlocked),
		},
	)

	if len(poison.snapshot()) != 0 {
		t.Fatal("a blocked message must not reach poison")
	}
	settles := msg.settles()
	if len(settles) != 1 || !strings.HasPrefix(settles[0], "nakdelay:") {
		t.Fatalf("settles %v", settles)
	}
	if observer.settles[0].Blocked != 1 {
		t.Fatalf("tally %+v", observer.settles[0])
	}
}
