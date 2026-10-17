package frame

import (
	"bytes"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRoundTripAcrossArrayHeaderSizes(t *testing.T) {
	for _, n := range []int{1, 15, 16, 300} {
		msg := framed("cmd.a", n)
		elements, err := Decode(msg.data)
		if err != nil {
			t.Fatalf("n %d: %v", n, err)
		}
		if len(elements) != n || elements[n-1].Payload[0] != byte(n-1) || elements[0].Subject != "cmd.a" {
			t.Fatalf("n %d: decoded %d", n, len(elements))
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	good := framed("cmd.a", 2).data
	cases := map[string][]byte{
		"empty":    nil,
		"not list": []byte{0xa1, 'x'},
		"zero":     Join(nil),
		"trailing": append(slices.Clone(good), 0x01),
		"cut":      good[:len(good)-1],
	}
	for name, data := range cases {
		if _, err := Decode(data); !errors.Is(err, ErrBadFrame) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestUnpackPlainMessageAsIs(t *testing.T) {
	plain := &fakeMsg{subject: "cmd.a", headers: nats.Header{}, data: []byte("x")}
	msgs, err := Unpack(plain, time.Second)
	if err != nil || len(msgs) != 1 || msgs[0] != plain || IsFrame(plain) {
		t.Fatalf("plain message changed: %v %v", msgs, err)
	}
}

func TestUnpackRejectsBadFrames(t *testing.T) {
	version := framed("cmd.a", 2)
	version.headers.Set(HeaderVersion, "2")
	if _, err := Unpack(version, time.Second); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("version: %v", err)
	}

	count := framed("cmd.a", 2)
	count.headers.Set(HeaderCount, "3")
	if _, err := Unpack(count, time.Second); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("count: %v", err)
	}

	subject := framed("cmd.a", 2)
	subject.subject = "cmd.b"
	if _, err := Unpack(subject, time.Second); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("subject: %v", err)
	}
}

func TestUnpackElementsKeepTheirOwnFields(t *testing.T) {
	msg := framed("cmd.a", 3)
	elements, err := Unpack(msg, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i, element := range elements {
		if element.Subject() != "cmd.a" || !bytes.Equal(element.Data(), []byte{byte(i)}) || element.Headers().Get(jetstream.MsgIDHeader) != string(rune('a'+i)) {
			t.Fatalf("element %d: %q %v %v", i, element.Subject(), element.Data(), element.Headers())
		}
		metadata, err := element.Metadata()
		if err != nil || metadata.NumDelivered != 3 || element.Reply() != "$JS.ACK.test" {
			t.Fatalf("element %d metadata: %v %v", i, metadata, err)
		}
	}
}

func TestFrameOutcome(t *testing.T) {
	cases := []struct {
		name   string
		settle func(elements []jetstream.Msg)
		want   []string
		delay  time.Duration
	}{
		{
			name: "all ack",
			settle: func(e []jetstream.Msg) {
				must(e[0].Ack())
				must(e[1].Ack())
				must(e[2].Ack())
			},
			want: []string{"ack"},
		},
		{
			name: "ack and term",
			settle: func(e []jetstream.Msg) {
				must(e[0].Term())
				must(e[1].Ack())
				must(e[2].TermWithReason("poison"))
			},
			want: []string{"ack"},
		},
		{
			name: "mixed naks take the largest delay",
			settle: func(e []jetstream.Msg) {
				must(e[0].NakWithDelay(time.Second))
				must(e[1].Ack())
				must(e[2].NakWithDelay(3 * time.Second))
			},
			want:  []string{"nak_delay"},
			delay: 3 * time.Second,
		},
		{
			name: "plain nak",
			settle: func(e []jetstream.Msg) {
				must(e[0].Ack())
				must(e[1].Nak())
				must(e[2].Ack())
			},
			want: []string{"nak"},
		},
		{
			name: "double ack on the last element",
			settle: func(e []jetstream.Msg) {
				must(e[0].Ack())
				must(e[1].Ack())
				must(e[2].DoubleAck(t.Context()))
			},
			want: []string{"double_ack"},
		},
	}

	for _, tc := range cases {
		msg := framed("cmd.a", 3)
		elements, err := Unpack(msg, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		tc.settle(elements)
		if got := msg.snapshot(); !slices.Equal(got, tc.want) {
			t.Fatalf("%s: frame calls %v", tc.name, got)
		}
		if tc.delay > 0 && msg.delays[0] != tc.delay {
			t.Fatalf("%s: delay %v", tc.name, msg.delays)
		}
	}
}

func TestSecondSettleIsAlreadyAcked(t *testing.T) {
	msg := framed("cmd.a", 2)
	elements, err := Unpack(msg, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	must(elements[0].Ack())
	if err := elements[0].Nak(); !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		t.Fatalf("second settle: %v", err)
	}
	if err := elements[0].InProgress(); !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		t.Fatalf("progress after settle: %v", err)
	}
	if len(msg.snapshot()) != 0 {
		t.Fatalf("frame settled early: %v", msg.snapshot())
	}
	must(elements[1].Ack())
	if got := msg.snapshot(); !slices.Equal(got, []string{"ack"}) {
		t.Fatalf("frame calls %v", got)
	}
}

func TestInProgressIsThrottled(t *testing.T) {
	msg := framed("cmd.a", 4)
	elements, err := Unpack(msg, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		for _, element := range elements {
			must(element.InProgress())
		}
	}
	if msg.progress != 1 {
		t.Fatalf("forwarded %d", msg.progress)
	}

	unthrottled := framed("cmd.a", 2)
	elements, err = Unpack(unthrottled, 0)
	if err != nil {
		t.Fatal(err)
	}
	must(elements[0].InProgress())
	must(elements[1].InProgress())
	if unthrottled.progress != 2 {
		t.Fatalf("forwarded %d", unthrottled.progress)
	}
}

func TestConcurrentSettleFinishesOnce(t *testing.T) {
	msg := framed("cmd.a", 64)
	elements, err := Unpack(msg, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i, element := range elements {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if i%7 == 0 {
				must(element.NakWithDelay(time.Duration(i) * time.Millisecond))
				return
			}
			must(element.Ack())
		}()
		go func() {
			defer wg.Done()
			if err := element.InProgress(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
				panic(err)
			}
		}()
	}
	wg.Wait()
	if got := msg.snapshot(); !slices.Equal(got, []string{"nak_delay"}) || msg.delays[0] != 63*time.Millisecond {
		t.Fatalf("frame calls %v %v", got, msg.delays)
	}
}

func TestMsgIDIgnoresOrder(t *testing.T) {
	if MsgID([]string{"a", "b"}) != MsgID([]string{"b", "a"}) || MsgID([]string{"a"}) == MsgID([]string{"a", "b"}) {
		t.Fatal("msg id must be the hash of the sorted ids")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
