package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestHoldAtMaxDeliverGoesToPoison(t *testing.T) {
	cases := []struct {
		name       string
		delivered  uint64
		wantPoison bool
	}{
		{
			name:      "below MaxDeliver is held",
			delivered: 5,
		},
		{
			name:       "at MaxDeliver goes to OnPoison",
			delivered:  6,
			wantPoison: true,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				poison := &poisonRecorder{}
				c := testConsumer(nil, 5, poison)
				c.maxDeliver = 6
				observer := &observerSpy{}
				c.observer = observer

				msg := newMsg("A2", tc.delivered)
				c.settle(
					context.Background(),
					asMsgs(msg),
					[]Result{
						NakDelay(0, ErrOrderBlocked),
					},
				)

				calls := poison.snapshot()
				if !tc.wantPoison {
					if len(calls) != 0 || observer.settles[0].Blocked != 1 {
						t.Fatalf("poison %v tally %+v", calls, observer.settles[0])
					}
					nakDelayOf(t, msg)
					return
				}
				if len(calls) != 1 || !errors.Is(calls[0].err, ErrOrderBlocked) || !errors.Is(calls[0].err, ErrAttemptsExhausted) {
					t.Fatalf("poison %+v", calls)
				}
				expectSettles(t, msg, "term")
				if observer.settles[0].Poison != 1 {
					t.Fatalf("tally %+v", observer.settles[0])
				}
			},
		)
	}
}

func TestOrderKeyHeldTailIsNeverDroppedAtMaxDeliver(t *testing.T) {
	const maxDeliver = 6

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
				switch {
				case string(msg.Data()) == "A1":
					return errors.New("A1 always fails")
				case string(msg.Data()) == "A2" && metadata.NumDelivered >= maxDeliver:
					return errors.New("A2 fails on the last delivery")
				}
			}
			tx.record(msgs)
			return nil
		},
	})
	poison := &poisonRecorder{}
	c := testConsumer(handler, 5, poison)
	c.maxDeliver = maxDeliver

	order := []string{"A1", "A2", "A3"}
	delivered := map[string]uint64{}
	arrival := map[string]time.Duration{}
	done := map[string]string{}
	for _, data := range order {
		delivered[data] = 1
	}

	for round := 0; len(done) < len(order); round++ {
		if round > 20 {
			t.Fatalf("not settled after %d rounds: %v", round, done)
		}

		now := time.Duration(-1)
		for _, data := range order {
			if done[data] == "" && (now < 0 || arrival[data] < now) {
				now = arrival[data]
			}
		}

		var batch []*fakeMsg
		for _, data := range order {
			if done[data] != "" || arrival[data] > now {
				continue
			}
			batch = append(
				batch,
				withID(newMsg(data, delivered[data]), "id-"+data),
			)
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
			switch {
			case msg.count("ack") == 1:
				done[data] = "ack"
			case msg.count("term") == 1:
				done[data] = "poison"
			default:
				if delivered[data] >= maxDeliver {
					t.Fatalf("%s NAKed on delivery %d = MaxDeliver, the server drops it", data, delivered[data])
				}
				arrival[data] = now + nakDelayOf(t, msg)
				delivered[data]++
			}
		}
	}

	for _, data := range order {
		if done[data] != "poison" {
			t.Fatalf("%s ended as %s, want poison: %v", data, done[data], done)
		}
	}
	if len(poison.snapshot()) != len(order) || len(tx.committed) != 0 {
		t.Fatalf("poison %d committed %v", len(poison.snapshot()), tx.committed)
	}
}
