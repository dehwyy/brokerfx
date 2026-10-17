package pgtimer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func payinTable() Table {
	return Table{
		Name:      "payin_timers",
		Key:       "order_id",
		TimerName: "name",
		Attempt:   "attempt_id",
		DueAt:     "due_at",
		CreatedAt: "created_at",
		UpdatedAt: "updated_at",
	}
}

func TestBuildQueriesQuotesColumns(t *testing.T) {
	built, err := buildQueries(payinTable())
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		`FROM "payin_timers"`,
		`ORDER BY "due_at", "created_at"`,
		`FOR UPDATE SKIP LOCKED`,
		`"updated_at" = now()`,
		`t."order_id"::text AS timer_key`,
	} {
		if !strings.Contains(
			built.claim,
			want,
		) {
			t.Fatalf("claim query misses %q:\n%s", want, built.claim)
		}
	}
	if built.remove != `DELETE FROM "payin_timers" WHERE "order_id" = ? AND "name" = ? AND "attempt_id" = ? AND "due_at" = ?` {
		t.Fatalf("remove query %s", built.remove)
	}
}

func TestBuildQueriesRejectsUnsafeNames(t *testing.T) {
	cases := []Table{
		{
			Name:      "payin_timers; DROP TABLE orders",
			Key:       "order_id",
			TimerName: "name",
			Attempt:   "attempt_id",
			DueAt:     "due_at",
		},
		{
			Name:      "payin_timers",
			Key:       `order_id"`,
			TimerName: "name",
			Attempt:   "attempt_id",
			DueAt:     "due_at",
		},
		{
			Name:      "a.b.c",
			Key:       "order_id",
			TimerName: "name",
			Attempt:   "attempt_id",
			DueAt:     "due_at",
		},
		{
			Name: "payin_timers",
		},
	}

	for _, table := range cases {
		_, err := buildQueries(table)
		if !errors.Is(
			err,
			ErrInvalidConfig,
		) {
			t.Fatalf("table %+v: want ErrInvalidConfig got %v", table, err)
		}
	}
}

func TestBuildQueriesAcceptsSchema(t *testing.T) {
	table := payinTable()
	table.Name = "processing.payin_timers"

	built, err := buildQueries(table)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		built.claim,
		`"processing"."payin_timers"`,
	) {
		t.Fatalf("claim query %s", built.claim)
	}
}

func TestResolve(t *testing.T) {
	s, err := resolve(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if s.lease != defaultLease || s.handlerTimeout != defaultHandlerTimeout || s.poll != defaultPoll || s.batch != defaultBatch || s.concurrency != defaultConcurrency {
		t.Fatalf("defaults %+v", s)
	}

	t.Setenv(
		envLease,
		"20s",
	)
	t.Setenv(
		envHandlerTimeout,
		"5s",
	)
	t.Setenv(
		envConcurrency,
		"4",
	)
	s, err = resolve(Config{
		Batch: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.lease != 20*time.Second || s.handlerTimeout != 5*time.Second || s.concurrency != 4 || s.batch != 10 {
		t.Fatalf("env %+v", s)
	}

	_, err = resolve(Config{
		Lease:          10 * time.Second,
		HandlerTimeout: 10 * time.Second,
	})
	if !errors.Is(
		err,
		ErrInvalidConfig,
	) {
		t.Fatalf("handler timeout equal to lease: want ErrInvalidConfig got %v", err)
	}

	t.Setenv(
		envPoll,
		"soon",
	)
	_, err = resolve(Config{})
	if !errors.Is(
		err,
		ErrInvalidConfig,
	) {
		t.Fatalf("bad env: want ErrInvalidConfig got %v", err)
	}
}

func TestHandleAndStartGuards(t *testing.T) {
	p, err := New(
		&gorm.DB{},
		Config{
			Table: payinTable(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Start(); !errors.Is(
		err,
		ErrNoHandlers,
	) {
		t.Fatalf("want ErrNoHandlers got %v", err)
	}

	handler := func(context.Context, Timer) error {
		return nil
	}
	if err := p.Handle(
		"TIMEOUT",
		handler,
	); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(
		"TIMEOUT",
		handler,
	); !errors.Is(
		err,
		ErrDuplicateHandler,
	) {
		t.Fatalf("want ErrDuplicateHandler got %v", err)
	}

	p.state = stateRunning
	if err := p.Handle(
		"OTHER",
		handler,
	); !errors.Is(
		err,
		ErrStarted,
	) {
		t.Fatalf("want ErrStarted got %v", err)
	}
}

func TestCallRecoversPanic(t *testing.T) {
	err := call(
		context.Background(),
		func(context.Context, Timer) error {
			panic("boom")
		},
		Timer{},
	)
	if !errors.Is(
		err,
		ErrHandlerPanic,
	) {
		t.Fatalf("want ErrHandlerPanic got %v", err)
	}
}
