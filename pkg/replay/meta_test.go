package replay

import (
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestRequestValidate(t *testing.T) {
	cases := []struct {
		name string
		req  Request
		ok   bool
	}{
		{"valid", Request{ReplayID: "r-1", Subject: "payin.replay.r-1"}, true},
		{"empty id", Request{Subject: "a.b"}, false},
		{"long id", Request{ReplayID: strings.Repeat("a", 65), Subject: "a.b"}, false},
		{"max id", Request{ReplayID: strings.Repeat("a", 64), Subject: "a.b"}, true},
		{"bad id char", Request{ReplayID: "r.1", Subject: "a.b"}, false},
		{"id space", Request{ReplayID: "r 1", Subject: "a.b"}, false},
		{"empty subject", Request{ReplayID: "r"}, false},
		{"star", Request{ReplayID: "r", Subject: "a.*"}, false},
		{"gt", Request{ReplayID: "r", Subject: "a.>"}, false},
		{"space", Request{ReplayID: "r", Subject: "a. b"}, false},
		{"empty token", Request{ReplayID: "r", Subject: "a..b"}, false},
		{"trailing dot", Request{ReplayID: "r", Subject: "a.b."}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("want ErrInvalidRequest, got %v", err)
			}
		})
	}
}

func TestItemValidate(t *testing.T) {
	cases := []struct {
		name string
		item Item
		ok   bool
	}{
		{"valid", Item{Key: "k", Version: "1", Headers: map[string]string{"X-Custom": "v"}}, true},
		{"empty key", Item{Version: "1"}, false},
		{"empty version", Item{Key: "k"}, false},
		{"nats header", Item{Key: "k", Version: "1", Headers: map[string]string{"Nats-Msg-Id": "x"}}, false},
		{"nats lower", Item{Key: "k", Version: "1", Headers: map[string]string{"nats-expected-stream": "x"}}, false},
		{"replay header", Item{Key: "k", Version: "1", Headers: map[string]string{"X-Replay-Seq": "1"}}, false},
		{"replay upper", Item{Key: "k", Version: "1", Headers: map[string]string{"X-REPLAY-ID": "1"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.item.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidItem) {
				t.Fatalf("want ErrInvalidItem, got %v", err)
			}
		})
	}
}

func TestParseMeta(t *testing.T) {
	if _, err := ParseMeta(nats.Header{}); !errors.Is(err, ErrNotReplay) {
		t.Fatalf("want ErrNotReplay, got %v", err)
	}

	snap := nats.Header{}
	snap.Set(HeaderReplayID, "r-1")
	snap.Set(HeaderSeq, "7")
	m, err := ParseMeta(snap)
	if err != nil {
		t.Fatal(err)
	}
	if m.ReplayID != "r-1" || m.Seq != 7 || m.End {
		t.Fatalf("unexpected snapshot meta: %+v", m)
	}

	end := nats.Header{}
	end.Set(HeaderReplayID, "r-1")
	end.Set(HeaderEnd, "true")
	end.Set(HeaderCount, "42")
	end.Set(HeaderStatus, StatusFailed)
	end.Set(HeaderError, "boom")
	m, err = ParseMeta(end)
	if err != nil {
		t.Fatal(err)
	}
	if !m.End || m.Count != 42 || m.Status != StatusFailed || m.Error != "boom" {
		t.Fatalf("unexpected end meta: %+v", m)
	}

	bad := map[string]string{HeaderCount: "x", HeaderSeq: "-1", HeaderStatus: "weird"}
	for k, v := range bad {
		h := nats.Header{}
		h.Set(HeaderReplayID, "r-1")
		h.Set(k, v)
		if _, err := ParseMeta(h); err == nil {
			t.Fatalf("expected error for %s=%s", k, v)
		}
	}
}
