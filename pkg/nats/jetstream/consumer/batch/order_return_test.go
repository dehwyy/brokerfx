package batch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func nakDelayOf(t *testing.T, msg *fakeMsg) time.Duration {
	t.Helper()

	settles := msg.settles()
	if len(settles) != 1 || !strings.HasPrefix(settles[0], "nakdelay:") {
		t.Fatalf("%s settles %v", msg.Data(), settles)
	}
	delay, err := time.ParseDuration(strings.TrimPrefix(settles[0], "nakdelay:"))
	if err != nil {
		t.Fatal(err)
	}

	return delay
}

func TestOrderKeyTailReturnsNotBeforeHead(t *testing.T) {
	cases := []struct {
		name         string
		opts         func(tx *fakeTx) InTxOpts
		headDelivery uint64
		wantHead     time.Duration
	}{
		{
			name: "head NakDelay from ApplyResults",
			opts: func(tx *fakeTx) InTxOpts {
				return InTxOpts{
					TxManager: tx,
					OrderKey:  orderKeyOfData,
					ApplyResults: func(_ context.Context, msgs []jetstream.Msg) ([]Result, error) {
						out := make(
							[]Result,
							len(msgs),
						)
						for i, msg := range msgs {
							out[i] = Ack()
							if string(msg.Data()) == "A1" {
								out[i] = NakDelay(
									10*time.Second,
									errors.New("hold not visible yet"),
								)
							}
						}
						return out, nil
					},
				}
			},
			headDelivery: 1,
			wantHead:     10 * time.Second,
		},
		{
			name: "head ladder on its third delivery",
			opts: func(tx *fakeTx) InTxOpts {
				return InTxOpts{
					TxManager: tx,
					OrderKey:  orderKeyOfData,
					Apply: func(_ context.Context, msgs []jetstream.Msg) error {
						for _, msg := range msgs {
							if string(msg.Data()) == "A1" {
								return errors.New("A1 fails")
							}
						}
						tx.record(msgs)
						return nil
					},
				}
			},
			headDelivery: 3,
			wantHead:     30 * time.Second,
		},
		{
			name: "transient budget spent for the whole key",
			opts: func(tx *fakeTx) InTxOpts {
				return InTxOpts{
					TxManager:   tx,
					OrderKey:    orderKeyOfData,
					RetryBudget: -1,
					Apply: func(context.Context, []jetstream.Msg) error {
						return context.DeadlineExceeded
					},
				}
			},
			headDelivery: 3,
			wantHead:     30 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				stubClaim(t, &claimSpy{})
				stubUnclaim(t, &unclaimSpy{})
				tx := &fakeTx{}
				handler := mustInTx(t, tc.opts(tx))

				head := withID(newMsg("A1", tc.headDelivery), "id-A1")
				tail := withID(newMsg("A2", 1), "id-A2")
				msgs := asMsgs(head, tail)
				c := testConsumer(handler, 5, &poisonRecorder{})
				c.settle(
					context.Background(),
					msgs,
					handler(
						context.Background(),
						msgs,
					),
				)

				if got := nakDelayOf(t, head); got != tc.wantHead {
					t.Fatalf("head delay %s, want %s", got, tc.wantHead)
				}
				if got := nakDelayOf(t, tail); got < tc.wantHead {
					t.Fatalf("tail delay %s returns before head %s", got, tc.wantHead)
				}
			},
		)
	}
}

func TestOrderKeyTailNotRedeliveredWithoutHead(t *testing.T) {
	stubClaim(t, &claimSpy{})
	stubUnclaim(t, &unclaimSpy{})
	tx := &fakeTx{}

	handler := mustInTx(t, InTxOpts{
		TxManager: tx,
		OrderKey:  orderKeyOfData,
		Apply: func(_ context.Context, msgs []jetstream.Msg) error {
			for _, msg := range msgs {
				metadata, err := msg.Metadata()
				if err != nil {
					return err
				}
				if string(msg.Data()) == "A1" && metadata.NumDelivered <= 3 {
					return errors.New("A1 fails until its fourth delivery")
				}
			}
			tx.record(msgs)
			return nil
		},
	})
	c := testConsumer(handler, 10, &poisonRecorder{})

	order := []string{"A1", "A2"}
	delivered := map[string]uint64{
		"A1": 2,
		"A2": 1,
	}
	arrival := map[string]time.Duration{
		"A1": 0,
		"A2": 0,
	}
	done := map[string]bool{}

	for round := 0; len(done) < len(order); round++ {
		if round > 10 {
			t.Fatalf("not settled after %d rounds", round)
		}

		now := time.Duration(-1)
		for _, data := range order {
			if !done[data] && (now < 0 || arrival[data] < now) {
				now = arrival[data]
			}
		}

		var batch []*fakeMsg
		for _, data := range order {
			if done[data] || arrival[data] > now {
				continue
			}
			batch = append(
				batch,
				withID(newMsg(data, delivered[data]), "id-"+data),
			)
		}
		if string(batch[0].Data()) == "A2" && !done["A1"] {
			t.Fatalf("round %d: A2 redelivered at %s without A1 at %s", round, now, arrival["A1"])
		}

		msgs := asMsgs(batch...)
		c.settle(
			context.Background(),
			msgs,
			handler(
				context.Background(),
				msgs,
			),
		)

		for _, msg := range batch {
			data := string(msg.Data())
			if msg.count("ack") == 1 {
				done[data] = true
				continue
			}
			arrival[data] = now + nakDelayOf(t, msg)
			delivered[data]++
		}
	}

	if len(tx.committed) != 2 || tx.committed[0] != "A1" || tx.committed[1] != "A2" {
		t.Fatalf("committed %v", tx.committed)
	}
}
