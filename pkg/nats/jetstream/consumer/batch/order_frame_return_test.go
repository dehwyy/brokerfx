package batch

import (
	"context"
	"testing"
	"time"
)

func TestOrderKeyTailFrameReturnsNotBeforeHeadFrame(t *testing.T) {
	head := newMsg("h", 2)
	middle := frameMsg(t, 1, "b1", "a1")
	tail := frameMsg(t, 1, "a2")
	held := func(after int) Result {
		result := NakDelay(time.Second, ErrOrderBlocked)
		result.after = after
		return result
	}
	c := testConsumer(
		resultsHandler(
			NakDelay(5*time.Second, errBusiness),
			held(1),
			NakDelay(time.Second, errBusiness),
			held(3),
		),
		5,
		&poisonRecorder{},
	)

	c.processBatch(context.Background(), c.unpack(context.Background(), asMsgs(head, middle, tail)))

	expectSettles(t, head, "nakdelay:5s")
	expectSettles(t, middle, "nakdelay:5s")
	expectSettles(t, tail, "nakdelay:5s")
}
