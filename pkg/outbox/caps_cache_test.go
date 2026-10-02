package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func scriptedDetect(seq []schemaCaps, calls *int) detectFunc {
	return func(context.Context, *gorm.DB) (schemaCaps, error) {
		i := *calls
		if i >= len(seq) {
			i = len(seq) - 1
		}
		*calls++

		return seq[i], nil
	}
}

func TestCapsCacheDoesNotPinLegacySchema(t *testing.T) {
	calls := 0
	detect := scriptedDetect([]schemaCaps{{V2: false}, {V2: true, Retries: true}}, &calls)
	c := &capsCache{}

	caps, resolved, err := c.get(context.Background(), nil, detect)
	if err != nil || resolved || caps.V2 {
		t.Fatalf("first detection must be legacy and unresolved, got %+v resolved=%v err=%v", caps, resolved, err)
	}

	caps, resolved, err = c.get(context.Background(), nil, detect)
	if err != nil || !resolved || !caps.V2 || !caps.Retries {
		t.Fatalf("after migration caps must flip to v2, got %+v resolved=%v err=%v", caps, resolved, err)
	}
}

func TestCapsCachePinsCompleteSchema(t *testing.T) {
	calls := 0
	detect := scriptedDetect([]schemaCaps{{V2: true, Retries: true}}, &calls)
	c := &capsCache{}

	for i := 0; i < 3; i++ {
		if _, resolved, err := c.get(context.Background(), nil, detect); err != nil || !resolved {
			t.Fatalf("iteration %d: resolved=%v err=%v", i, resolved, err)
		}
	}

	if calls != 1 {
		t.Fatalf("complete schema must be detected once, got %d", calls)
	}
}

func TestCapsCacheThrottlesRecheck(t *testing.T) {
	calls := 0
	detect := scriptedDetect([]schemaCaps{{V2: false}}, &calls)
	c := &capsCache{recheckWait: time.Hour}

	for i := 0; i < 5; i++ {
		if _, _, err := c.get(context.Background(), nil, detect); err != nil {
			t.Fatal(err)
		}
	}

	if calls != 1 {
		t.Fatalf("recheck must be throttled, got %d detections", calls)
	}
}

func TestCapsCacheErrorIsNotCached(t *testing.T) {
	calls := 0
	detect := func(context.Context, *gorm.DB) (schemaCaps, error) {
		calls++
		if calls == 1 {
			return schemaCaps{}, errors.New("db down")
		}

		return schemaCaps{V2: true, Retries: true}, nil
	}
	c := &capsCache{}

	if _, _, err := c.get(context.Background(), nil, detect); err == nil {
		t.Fatal("expected error")
	}

	if caps, resolved, err := c.get(context.Background(), nil, detect); err != nil || !resolved || !caps.V2 {
		t.Fatalf("expected recovery, got %+v %v %v", caps, resolved, err)
	}
}
