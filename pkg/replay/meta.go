package replay

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

const (
	HeaderReplayID = "X-Replay-Id"
	HeaderSeq      = "X-Replay-Seq"
	HeaderEnd      = "X-Replay-End"
	HeaderCount    = "X-Replay-Count"
	HeaderStatus   = "X-Replay-Status"
	HeaderError    = "X-Replay-Error"

	StatusComplete = "complete"
	StatusFailed   = "failed"

	maxReplayIDLen = 64
	endTrue        = "true"
)

type Item struct {
	Key     string
	Version string
	Payload []byte
	Headers map[string]string
}

type Source interface {
	Next(ctx context.Context) ([]Item, error)
}

type Request struct {
	ReplayID   string
	Subject    string
	EndPayload []byte
}

func (r Request) Validate() error {
	if r.ReplayID == "" || len(r.ReplayID) > maxReplayIDLen {
		return fmt.Errorf("%w: replay id must be 1..%d characters", ErrInvalidRequest, maxReplayIDLen)
	}
	for _, c := range r.ReplayID {
		if !isIDChar(c) {
			return fmt.Errorf("%w: replay id contains %q", ErrInvalidRequest, c)
		}
	}
	if r.Subject == "" {
		return fmt.Errorf("%w: subject is empty", ErrInvalidRequest)
	}
	if strings.ContainsAny(r.Subject, "*> \t\r\n") {
		return fmt.Errorf("%w: subject %q has wildcard or whitespace", ErrInvalidRequest, r.Subject)
	}
	for _, tok := range strings.Split(r.Subject, ".") {
		if tok == "" {
			return fmt.Errorf("%w: subject %q has empty token", ErrInvalidRequest, r.Subject)
		}
	}
	return nil
}

func isIDChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

func (i Item) Validate() error {
	if i.Key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidItem)
	}
	if i.Version == "" {
		return fmt.Errorf("%w: empty version for key %q", ErrInvalidItem, i.Key)
	}
	for name := range i.Headers {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "nats-") || strings.HasPrefix(lower, "x-replay-") {
			return fmt.Errorf("%w: reserved header %q on key %q", ErrInvalidItem, name, i.Key)
		}
	}
	return nil
}

type Meta struct {
	ReplayID string
	Seq      int64
	End      bool
	Count    int64
	Status   string
	Error    string
}

func ParseMeta(h nats.Header) (Meta, error) {
	id := h.Get(HeaderReplayID)
	if id == "" {
		return Meta{}, ErrNotReplay
	}
	m := Meta{ReplayID: id, Error: h.Get(HeaderError)}
	if v := h.Get(HeaderSeq); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return Meta{}, fmt.Errorf("replay: invalid %s %q", HeaderSeq, v)
		}
		m.Seq = n
	}
	if v := h.Get(HeaderCount); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return Meta{}, fmt.Errorf("replay: invalid %s %q", HeaderCount, v)
		}
		m.Count = n
	}
	m.End = h.Get(HeaderEnd) == endTrue
	switch s := h.Get(HeaderStatus); s {
	case "", StatusComplete, StatusFailed:
		m.Status = s
	default:
		return Meta{}, fmt.Errorf("replay: unknown %s %q", HeaderStatus, s)
	}
	return m, nil
}
