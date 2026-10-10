package batch

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestHeartbeatOneTickerForBatch(t *testing.T) {
	msgs := make([]*fakeMsg, 16)
	for i := range msgs {
		msgs[i] = newMsg("a", 1)
	}
	base := runtime.NumGoroutine()
	var during int
	c := testConsumer(func(_ context.Context, batch []jetstream.Msg) []Result {
		time.Sleep(10 * time.Millisecond)
		during = runtime.NumGoroutine()
		time.Sleep(60 * time.Millisecond)
		results := make([]Result, len(batch))
		for i := range results {
			results[i] = Ack()
		}
		return results
	}, 5, nil)
	c.heartbeatEvery = 20 * time.Millisecond

	c.processBatch(context.Background(), asMsgs(msgs...))

	if during > base+2 {
		t.Fatalf("goroutines during batch %d, base %d", during, base)
	}
	counts := make([]int, len(msgs))
	for i, msg := range msgs {
		counts[i] = msg.count("progress")
		if counts[i] < 2 {
			t.Fatalf("msg %d progress %d", i, counts[i])
		}
	}
	time.Sleep(60 * time.Millisecond)
	for i, msg := range msgs {
		if msg.count("progress") != counts[i] {
			t.Fatalf("progress after settle grew for msg %d", i)
		}
	}
}
