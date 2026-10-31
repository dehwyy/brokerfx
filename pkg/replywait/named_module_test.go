package replywait

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
)

type fakeConsumeContext struct {
	jetstream.ConsumeContext
}

func (fakeConsumeContext) Stop() {}

type fakeConsumer struct {
	jetstream.Consumer

	register func(handler jetstream.MessageHandler)
}

func (c *fakeConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	c.register(handler)
	return fakeConsumeContext{}, nil
}

type fakeJetStream struct {
	jetstream.JetStream

	mu       sync.Mutex
	handlers map[string]jetstream.MessageHandler
}

func newFakeJetStream() *fakeJetStream {
	return &fakeJetStream{handlers: map[string]jetstream.MessageHandler{}}
}

func (j *fakeJetStream) OrderedConsumer(
	_ context.Context,
	_ string,
	cfg jetstream.OrderedConsumerConfig,
) (jetstream.Consumer, error) {
	subject := cfg.FilterSubjects[0]

	return &fakeConsumer{
		register: func(handler jetstream.MessageHandler) {
			j.mu.Lock()
			defer j.mu.Unlock()
			j.handlers[subject] = handler
		},
	}, nil
}

func (j *fakeJetStream) deliver(subject string, msg jetstream.Msg) bool {
	j.mu.Lock()
	handler, ok := j.handlers[subject]
	j.mu.Unlock()
	if !ok {
		return false
	}

	handler(msg)

	return true
}

func namedTestConfig(subject string) Config {
	return Config{
		Stream:            "TEST_REPLY",
		ReplySubject:      subject,
		CorrelationFunc:   func(msg jetstream.Msg) (string, error) { return msg.Subject(), nil },
		DefaultTimeout:    time.Second,
		InactiveThreshold: time.Minute,
		DrainTimeout:      time.Second,
	}
}

func twoInstanceOptions(js jetstream.JetStream) []fx.Option {
	return []fx.Option{
		fx.Provide(func() jetstream.JetStream { return js }),
		fx.Provide(func() Config { return namedTestConfig("svc.result.default.pod") }),
		fx.Provide(
			fx.Annotate(
				func() jetstream.JetStream { return js },
				fx.ResultTags(`name:"balance"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() Config { return namedTestConfig("balance.result.tenant.pod") },
				fx.ResultTags(`name:"balance"`),
			),
		),
		Module,
		NamedModule("balance"),
	}
}

type twoWaiters struct {
	fx.In

	Default *ReplyWaiter
	Balance *ReplyWaiter `name:"balance"`
}

func TestNamedModule_TwoInstances(t *testing.T) {
	opts := append(twoInstanceOptions(newFakeJetStream()), fx.Invoke(func(w twoWaiters) {}))

	if err := fx.ValidateApp(opts...); err != nil {
		t.Fatalf("unexpected graph error: %v", err)
	}

	var got twoWaiters
	app := fx.New(append(twoInstanceOptions(newFakeJetStream()), fx.Populate(&got))...)
	if err := app.Err(); err != nil {
		t.Fatalf("unexpected app error: %v", err)
	}

	if got.Default == got.Balance {
		t.Fatal("expected distinct waiters")
	}
	if got.Default.ReplySubject() != "svc.result.default.pod" {
		t.Fatalf("unexpected default subject %q", got.Default.ReplySubject())
	}
	if got.Balance.ReplySubject() != "balance.result.tenant.pod" {
		t.Fatalf("unexpected named subject %q", got.Balance.ReplySubject())
	}
}

func TestNamedModule_ResolvesOnlyOwnSubject(t *testing.T) {
	js := newFakeJetStream()

	var got twoWaiters
	app := fx.New(append(twoInstanceOptions(js), fx.Populate(&got))...)
	if err := app.Err(); err != nil {
		t.Fatalf("unexpected app error: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if err := app.Stop(stopCtx); err != nil {
			t.Errorf("unexpected stop error: %v", err)
		}
	}()

	balanceWaiter, err := got.Balance.Register("corr-balance")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}
	defaultWaiter, err := got.Default.Register("corr-default")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	if !js.deliver("balance.result.tenant.pod", &fakeMsg{subject: "corr-balance"}) {
		t.Fatal("named instance consumer was not started")
	}
	if !js.deliver("svc.result.default.pod", &fakeMsg{subject: "corr-default"}) {
		t.Fatal("default instance consumer was not started")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()

	msg, err := balanceWaiter.Wait(waitCtx)
	if err != nil || msg.Subject() != "corr-balance" {
		t.Fatalf("named waiter: msg=%v err=%v", msg, err)
	}
	msg, err = defaultWaiter.Wait(waitCtx)
	if err != nil || msg.Subject() != "corr-default" {
		t.Fatalf("default waiter: msg=%v err=%v", msg, err)
	}

	crossWaiter, err := got.Default.Register("corr-cross")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}
	js.deliver("balance.result.tenant.pod", &fakeMsg{subject: "corr-cross"})

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shortCancel()
	if _, err := crossWaiter.Wait(shortCtx); err == nil {
		t.Fatal("default waiter resolved by a message of the named instance")
	}
}
