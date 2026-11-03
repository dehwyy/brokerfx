package ordered

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeConsumeContext struct {
	jetstream.ConsumeContext
}

type fakeConsumer struct {
	jetstream.Consumer

	events  *[]string
	info    *jetstream.ConsumerInfo
	infoErr error
	deliver bool
}

func (c *fakeConsumer) Info(_ context.Context) (*jetstream.ConsumerInfo, error) {
	*c.events = append(
		*c.events,
		"info",
	)

	return c.info, c.infoErr
}

func (c *fakeConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	*c.events = append(
		*c.events,
		"consume",
	)
	if c.deliver {
		handler(&fakeMsg{headers: nats.Header{}})
	}

	return &fakeConsumeContext{}, nil
}

type fakeJetStream struct {
	jetstream.JetStream

	consumer *fakeConsumer
}

func (j *fakeJetStream) OrderedConsumer(_ context.Context, _ string, _ jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	return j.consumer, nil
}

func TestFatalClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{jetstream.ErrConsumerDeleted, false},
		{jetstream.ErrConsumerNotFound, false},
		{fmt.Errorf(
			"wrapped: %w",
			jetstream.ErrConsumerDeleted,
		), false},
		{jetstream.ErrStreamNotFound, true},
		{fmt.Errorf(
			"wrapped: %w",
			jetstream.ErrStreamNotFound,
		), true},
		{errors.New("other"), false},
	}

	for _, tc := range cases {
		if got := fatal(tc.err); got != tc.want {
			t.Fatalf(
				"fatal(%v): want %v got %v",
				tc.err,
				tc.want,
				got,
			)
		}
	}
}

func TestOnAttachedSeesInfoBeforeFirstHandlerCall(t *testing.T) {
	var events []string
	js := &fakeJetStream{
		consumer: &fakeConsumer{
			events:  &events,
			info:    &jetstream.ConsumerInfo{NumPending: 7},
			deliver: true,
		},
	}

	var attached uint64
	_, err := New(
		context.Background(),
		Opts{
			JetStream: js,
			Stream:    "S",
			OnAttached: func(_ context.Context, info *jetstream.ConsumerInfo) {
				events = append(
					events,
					"attached",
				)
				attached = info.NumPending
			},
			HandlerFunc: func(_ context.Context, _ jetstream.Msg) error {
				events = append(
					events,
					"handler",
				)
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"New: %v",
			err,
		)
	}

	want := []string{"info", "attached", "consume", "handler"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf(
			"order: want %v got %v",
			want,
			events,
		)
	}
	if attached != 7 {
		t.Fatalf(
			"OnAttached must see info, NumPending=%d",
			attached,
		)
	}
}

func TestInfoErrorFailsNew(t *testing.T) {
	var events []string
	boom := errors.New("info boom")
	js := &fakeJetStream{
		consumer: &fakeConsumer{
			events:  &events,
			infoErr: boom,
		},
	}

	called := false
	_, err := New(
		context.Background(),
		Opts{
			JetStream: js,
			OnAttached: func(_ context.Context, _ *jetstream.ConsumerInfo) {
				called = true
			},
		},
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"want info error, got %v",
			err,
		)
	}
	if called || fmt.Sprint(events) != "[info]" {
		t.Fatalf(
			"no attach and no consume after info error: called=%v events=%v",
			called,
			events,
		)
	}
}

func TestNoOnAttachedSkipsInfo(t *testing.T) {
	var events []string
	js := &fakeJetStream{
		consumer: &fakeConsumer{events: &events},
	}

	if _, err := New(
		context.Background(),
		Opts{JetStream: js},
	); err != nil {
		t.Fatalf(
			"New: %v",
			err,
		)
	}
	if fmt.Sprint(events) != "[consume]" {
		t.Fatalf(
			"Info must not be called without OnAttached: %v",
			events,
		)
	}
}
