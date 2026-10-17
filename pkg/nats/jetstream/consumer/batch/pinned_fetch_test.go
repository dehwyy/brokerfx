package batch

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type optsFetcher struct {
	opts []int
	err  error
}

func (f *optsFetcher) Fetch(count int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	f.opts = append(f.opts, len(opts))
	return batchOf([]*fakeMsg{newMsg("p0", 1)}, f.err), nil
}

func TestPinnedFetchAddsTheGroupAndTreatsPinMismatchAsEmpty(t *testing.T) {
	fetcher := &optsFetcher{err: jetstream.ErrPinIDMismatch}
	c := &Consumer{fetcher: fetcher, group: "p"}

	msgs, err := c.collect(4, jetstream.FetchMaxWait(1))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("msgs %d err %v", len(msgs), err)
	}

	plain := &optsFetcher{}
	if _, err := (&Consumer{fetcher: plain}).collect(4, jetstream.FetchMaxWait(1)); err != nil {
		t.Fatal(err)
	}
	if fetcher.opts[0] != 2 || plain.opts[0] != 1 {
		t.Fatalf("fetch opts pinned %v plain %v", fetcher.opts, plain.opts)
	}
}
