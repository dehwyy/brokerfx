package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestInTxPanicRollsBackAndKeepsOrderKey(t *testing.T) {
	cases := []struct {
		name          string
		orderKey      KeyFunc
		wantCommitted []string
		wantTail      error
	}{
		{
			name:     "with OrderKey the tail is held",
			orderKey: orderKeyOfData,
			wantTail: ErrOrderBlocked,
		},
		{
			name:          "without OrderKey the rest commits",
			wantCommitted: []string{"A2"},
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				stubClaim(t, &claimSpy{})
				stubUnclaim(t, &unclaimSpy{})
				tx := &fakeTx{}
				handler := mustInTx(t, InTxOpts{
					TxManager: tx,
					OrderKey:  tc.orderKey,
					Apply: func(_ context.Context, msgs []jetstream.Msg) error {
						tx.record(msgs)
						for _, msg := range msgs {
							if string(msg.Data()) == "A1" {
								panic("nil deref on A1")
							}
						}
						return nil
					},
				})

				head := withID(newMsg("A1", 1), "id-A1")
				tail := withID(newMsg("A2", 1), "id-A2")
				c := testConsumer(handler, 5, &poisonRecorder{})
				c.processBatch(
					context.Background(),
					asMsgs(head, tail),
				)

				if len(tx.committed) != len(tc.wantCommitted) || (len(tc.wantCommitted) > 0 && tx.committed[0] != tc.wantCommitted[0]) {
					t.Fatalf("committed %v, want %v", tx.committed, tc.wantCommitted)
				}
				if got := nakDelayOf(t, head); got != time.Second {
					t.Fatalf("head delay %s", got)
				}
				if tc.wantTail != nil {
					if got := nakDelayOf(t, tail); got < time.Second {
						t.Fatalf("tail delay %s", got)
					}
					return
				}
				expectSettles(t, tail, "ack")
			},
		)
	}
}

func TestInTxPanicIsHandlerPanicError(t *testing.T) {
	stubClaim(t, &claimSpy{})
	handler := mustInTx(t, InTxOpts{
		TxManager: &fakeTx{},
		OrderKey:  orderKeyOfData,
		ApplyResults: func(context.Context, []jetstream.Msg) ([]Result, error) {
			panic("boom")
		},
	})

	results := handler(
		context.Background(),
		idMsgs("A1", "A2"),
	)
	if !errors.Is(results[0].Err, ErrHandlerPanic) {
		t.Fatalf("A1 %+v", results[0])
	}
	if !errors.Is(results[1].Err, ErrOrderBlocked) {
		t.Fatalf("A2 %+v", results[1])
	}
}
