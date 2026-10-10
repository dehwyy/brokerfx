package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func firstLetter(msg jetstream.Msg) string {
	return string(msg.Data()[:1])
}

func lettersBatch() []jetstream.Msg {
	return asMsgs(newMsg("a1", 1), newMsg("b1", 1), newMsg("a2", 1), newMsg("c1", 1), newMsg("b2", 1), newMsg("a3", 1))
}

func dataOf(msgs []jetstream.Msg) []string {
	out := make([]string, len(msgs))
	for i, msg := range msgs {
		out[i] = string(msg.Data())
	}
	return out
}

func TestGroupByKeyKeepsOrderInsideGroup(t *testing.T) {
	groups := GroupByKey(lettersBatch(), firstLetter)

	want := []struct {
		key  string
		data []string
	}{
		{key: "a", data: []string{"a1", "a2", "a3"}},
		{key: "b", data: []string{"b1", "b2"}},
		{key: "c", data: []string{"c1"}},
	}
	if len(groups) != len(want) {
		t.Fatalf("groups %d", len(groups))
	}
	for i, w := range want {
		got := dataOf(groups[i].Msgs)
		if groups[i].Key != w.key || len(got) != len(w.data) {
			t.Fatalf("group %d: %s %v", i, groups[i].Key, got)
		}
		for j := range got {
			if got[j] != w.data[j] {
				t.Fatalf("group %d: %v", i, got)
			}
		}
	}
}

func TestCoalesceVerdictAppliesToWholeGroup(t *testing.T) {
	e := errors.New("bad group")
	handler := CoalesceByKey(firstLetter, func(_ context.Context, groups []Group) []Result {
		return []Result{Ack(), Term(e), Ack()}
	})

	results := handler(context.Background(), lettersBatch())

	if len(results) != 6 {
		t.Fatalf("results %d", len(results))
	}
	for i, result := range results {
		wantTerm := i == 1 || i == 4
		if wantTerm && (result.Verdict != VerdictTerm || !errors.Is(result.Err, e)) {
			t.Fatalf("result %d %+v", i, result)
		}
		if !wantTerm && result.Verdict != VerdictAck {
			t.Fatalf("result %d %+v", i, result)
		}
	}
}

func TestCoalesceShortResultsNakRestGroups(t *testing.T) {
	handler := CoalesceByKey(firstLetter, func(_ context.Context, groups []Group) []Result {
		return []Result{Ack()}
	})

	results := handler(context.Background(), lettersBatch())

	for i, result := range results {
		isA := i == 0 || i == 2 || i == 5
		if isA && result.Verdict != VerdictAck {
			t.Fatalf("result %d %+v", i, result)
		}
		if !isA && (result.Verdict != VerdictNak || !errors.Is(result.Err, ErrMissingResult)) {
			t.Fatalf("result %d %+v", i, result)
		}
	}
}

func TestCoalesceEmptyBatch(t *testing.T) {
	called := false
	handler := CoalesceByKey(firstLetter, func(_ context.Context, groups []Group) []Result {
		called = true
		return nil
	})

	results := handler(context.Background(), nil)

	if called || len(results) != 0 {
		t.Fatalf("called %v results %d", called, len(results))
	}
}

func TestCoalescePanicsOnNilDeps(t *testing.T) {
	for _, tc := range []struct {
		key   KeyFunc
		apply func(context.Context, []Group) []Result
	}{
		{key: nil, apply: func(context.Context, []Group) []Result { return nil }},
		{key: firstLetter, apply: nil},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			CoalesceByKey(tc.key, tc.apply)
		}()
	}
}
