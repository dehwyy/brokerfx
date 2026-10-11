package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestInTxTransientFollowsLadderUntilPoison(t *testing.T) {
	stubClaim(t, &claimSpy{})
	poison := &poisonRecorder{}
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		Apply: func(context.Context, []jetstream.Msg) error {
			return pgErr{code: "08006"}
		},
	})
	c := testConsumer(handler, len(DefaultDelays)+1, poison)

	want := []string{"nakdelay:1s", "nakdelay:5s", "nakdelay:30s", "nakdelay:2m0s", "nakdelay:5m0s"}
	for i, settle := range want {
		msg := withID(newMsg("a", uint64(i+1)), "id-a")

		c.processBatch(context.Background(), asMsgs(msg))

		expectSettles(t, msg, settle)
		if calls := poison.snapshot(); len(calls) != 0 {
			t.Fatalf("delivery %d poisoned early: %+v", i+1, calls)
		}
	}

	last := withID(newMsg("a", uint64(len(DefaultDelays)+1)), "id-a")

	c.processBatch(context.Background(), asMsgs(last))

	calls := poison.snapshot()
	var transient pgErr
	if len(calls) != 1 || calls[0].msg != last || !errors.As(calls[0].err, &transient) {
		t.Fatalf("poison calls %+v", calls)
	}
}

func TestInTxTransientBudgetExhaustedDefaultsToLadder(t *testing.T) {
	stubClaim(t, &claimSpy{})
	handler := mustInTx(t, InTxOpts{
		TxManager:   &fakeTx{},
		RetryBudget: 1,
		Apply: func(context.Context, []jetstream.Msg) error {
			return pgErr{code: "40P01"}
		},
	})

	results := handler(context.Background(), idMsgs("a", "b"))

	for i, result := range results {
		var transient pgErr
		if result.Verdict != VerdictNak || result.Delay != 0 || !errors.As(result.Err, &transient) {
			t.Fatalf("result %d %+v", i, result)
		}
	}
}
