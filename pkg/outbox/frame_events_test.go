package outbox

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go/jetstream"
)

type sizedProducer struct {
	maxPayload int64
}

func (p *sizedProducer) Produce(context.Context, ProducerEvent) error { return nil }
func (p *sizedProducer) MaxPayload() int64                            { return p.maxPayload }

func framingRelay(t *testing.T, subjects string, producer Producer) *OutboxRelay {
	t.Helper()
	return NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Producer: producer,
		Config: Config{
			Mode:          ModeDeleteAfterSend,
			Batched:       true,
			FrameSubjects: subjects,
		},
	})
}

func event(subject string, id int, headers ...string) ProducerEvent {
	h := map[string]string{jetstream.MsgIDHeader: "id-" + strconv.Itoa(id)}
	for i := 0; i+1 < len(headers); i += 2 {
		h[headers[i]] = headers[i+1]
	}
	return ProducerEvent{Subject: subject, Headers: h, Payload: []byte("p" + strconv.Itoa(id))}
}

func decoded(t *testing.T, wire ProducerEvent) []string {
	t.Helper()
	elements, err := frame.Decode(wire.Payload)
	if err != nil {
		t.Fatal(err)
	}
	payloads := make([]string, 0, len(elements))
	for _, element := range elements {
		payloads = append(payloads, string(element.Payload))
	}
	return payloads
}

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		pattern string
		subject string
		want    bool
	}{
		{"payin.command.>", "payin.command.create", true},
		{"payin.command.>", "payin.command", false},
		{"payin.command.>", "payin.commandquote.x", false},
		{"payin.*.create", "payin.command.create", true},
		{"payin.*.create", "payin.command.create.x", false},
		{"payin.command.create", "payin.command.create", true},
		{"payin.command.create", "payin.command", false},
		{">", "a", true},
	}
	for _, tc := range cases {
		if got := subjectMatches(tc.pattern, tc.subject); got != tc.want {
			t.Fatalf("%s vs %s: %v", tc.pattern, tc.subject, got)
		}
	}
	for _, bad := range []string{"", "a..b", "a.>.b", "a.b*", " a"} {
		if validSubjectPattern(bad) {
			t.Fatalf("pattern %q must be invalid", bad)
		}
	}
}

func TestFrameEventsGroupsBySubjectInRowOrder(t *testing.T) {
	r := framingRelay(t, "cmd.>", nil)
	events := []ProducerEvent{
		event("cmd.a", 1),
		event("cmd.a", 2),
		event("cmd.b", 3),
		event("other.x", 4),
		event("cmd.a", 5),
		event("cmd.b", 6),
		event("cmd.c", 7),
	}

	wire, members, framed := r.frameEvents(events)
	if !framed {
		t.Fatal("want framed")
	}
	if !slices.EqualFunc(members, [][]int{{0, 1, 4}, {2, 5}, {3}, {6}}, slices.Equal) {
		t.Fatalf("members %v", members)
	}
	if got := decoded(t, wire[0]); !slices.Equal(got, []string{"p1", "p2", "p5"}) {
		t.Fatalf("frame a %v", got)
	}
	if wire[0].Subject != "cmd.a" || wire[0].Headers[frame.HeaderVersion] != frame.Version || wire[0].Headers[frame.HeaderCount] != "3" {
		t.Fatalf("frame headers %v", wire[0].Headers)
	}
	if wire[0].Headers[jetstream.MsgIDHeader] != frame.MsgID([]string{"id-1", "id-2", "id-5"}) {
		t.Fatal("frame msg id must hash the element ids")
	}
	if wire[2].Subject != "other.x" || wire[3].Headers[jetstream.MsgIDHeader] != "id-7" {
		t.Fatalf("plain events changed: %v %v", wire[2], wire[3])
	}
}

func TestFrameEventsKeepsServerHeadersAndKVOut(t *testing.T) {
	r := framingRelay(t, ">", nil)
	events := []ProducerEvent{
		event("cmd.a", 1),
		event("cmd.a", 2, "Nats-Expected-Last-Subject-Sequence", "4"),
		event("cmd.a", 3),
		event("cmd.a", 4),
		event("$KV.b.k", 5),
		event("$KV.b.k", 6),
		event("cmd.a", 7, "Nats-TTL", "1m"),
		event("cmd.a", 8, "nats-rollup", "sub"),
	}

	_, members, framed := r.frameEvents(events)
	if !framed || !slices.EqualFunc(members, [][]int{{0}, {1}, {2, 3}, {4}, {5}, {6}, {7}}, slices.Equal) {
		t.Fatalf("members %v", members)
	}
}

func TestFrameEventsKeepsEventsWithoutMsgIDOut(t *testing.T) {
	r := framingRelay(t, "cmd.>", nil)
	bare := event("cmd.a", 2)
	delete(bare.Headers, jetstream.MsgIDHeader)
	events := []ProducerEvent{event("cmd.a", 1), bare, event("cmd.a", 3), event("cmd.a", 4)}

	_, members, framed := r.frameEvents(events)
	if !framed || !slices.EqualFunc(members, [][]int{{0}, {1}, {2, 3}}, slices.Equal) {
		t.Fatalf("members %v", members)
	}
}

func TestFrameEventsSkipsLatestOnlySubjects(t *testing.T) {
	r := NewRelay(RelayDeps{
		Store: &OutboxStore{},
		Config: Config{
			Mode:               ModeDeleteAfterSend,
			Batched:            true,
			FrameSubjects:      "snap.>",
			LatestOnlyPrefixes: "snap.",
		},
	})
	if _, _, framed := r.frameEvents([]ProducerEvent{event("snap.a", 1), event("snap.a", 2)}); framed {
		t.Fatal("latest-only subjects are never framed")
	}
}

func TestFrameEventsLimits(t *testing.T) {
	t.Setenv(envFrameMaxElements, "2")
	r := framingRelay(t, "cmd.>", nil)
	_, members, _ := r.frameEvents([]ProducerEvent{event("cmd.a", 1), event("cmd.a", 2), event("cmd.a", 3)})
	if !slices.EqualFunc(members, [][]int{{0, 1}, {2}}, slices.Equal) {
		t.Fatalf("max elements: %v", members)
	}

	t.Setenv(envFrameMaxElements, "")
	t.Setenv(envFrameMaxBytes, "1024")
	r = framingRelay(t, "cmd.>", nil)
	big := event("cmd.a", 2)
	big.Payload = make([]byte, 1100)
	half := event("cmd.a", 3)
	half.Payload = make([]byte, 600)
	_, members, _ = r.frameEvents([]ProducerEvent{event("cmd.a", 1), big, event("cmd.a", 4), half, half})
	if !slices.EqualFunc(members, [][]int{{0}, {1}, {2, 3}, {4}}, slices.Equal) {
		t.Fatalf("max bytes: %v", members)
	}
}

func TestFrameMaxBytesClampedBelowMaxPayload(t *testing.T) {
	r := framingRelay(t, "cmd.>", &sizedProducer{maxPayload: 64 * 1024})
	if got := r.frameMaxBytes(); got != 64*1024-frameHeaderReserve {
		t.Fatalf("clamped %d", got)
	}
	r = framingRelay(t, "cmd.>", &sizedProducer{maxPayload: 8 << 20})
	if got := r.frameMaxBytes(); got != defaultFrameMaxBytes {
		t.Fatalf("not clamped %d", got)
	}
}

func TestFramingOffByDefaultAndByEmptyEnv(t *testing.T) {
	r := framingRelay(t, "", nil)
	events := []ProducerEvent{event("cmd.a", 1), event("cmd.a", 2)}
	if _, _, framed := r.frameEvents(events); framed {
		t.Fatal("frames must be off by default")
	}

	t.Setenv(envFrameSubjects, "")
	r = framingRelay(t, "cmd.>", nil)
	if _, _, framed := r.frameEvents(events); framed {
		t.Fatal("an empty env must turn frames off")
	}

	t.Setenv(envFrameSubjects, "cmd.>")
	r = framingRelay(t, "", nil)
	if _, _, framed := r.frameEvents(events); !framed {
		t.Fatal("env must turn frames on")
	}
}

func TestPublishFramedSpreadsFrameErrorsToRows(t *testing.T) {
	r := framingRelay(t, "cmd.>", nil)
	failed := errors.New("publish failed")
	events := []ProducerEvent{event("cmd.a", 1), event("cmd.b", 2), event("cmd.a", 3)}

	errs := r.publishFramed(events, func(wire []ProducerEvent) []error {
		if len(wire) != 2 {
			t.Fatalf("wire %d", len(wire))
		}
		return []error{failed, nil}
	})
	if !errors.Is(errs[0], failed) || errs[1] != nil || !errors.Is(errs[2], failed) {
		t.Fatalf("errs %v", errs)
	}
}
