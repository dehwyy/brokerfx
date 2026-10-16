package outbox

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func resetIDVersion(t *testing.T) {
	t.Helper()
	idV7Once = sync.Once{}
	t.Cleanup(func() {
		idV7Once = sync.Once{}
	})
}

func eventIDVersion(t *testing.T) uuid.Version {
	t.Helper()
	id, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Version()
}

func TestNewEventIDVersion(t *testing.T) {
	cases := []struct {
		env  string
		want uuid.Version
	}{
		{
			env:  "",
			want: 7,
		},
		{
			env:  "true",
			want: 7,
		},
		{
			env:  "false",
			want: 4,
		},
		{
			env:  "0",
			want: 4,
		},
		{
			env:  "maybe",
			want: 7,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.env,
			func(t *testing.T) {
				t.Setenv(
					envIDV7,
					tc.env,
				)
				resetIDVersion(t)
				if got := eventIDVersion(t); got != tc.want {
					t.Fatalf(
						"%s=%q: version %d, want %d",
						envIDV7,
						tc.env,
						got,
						tc.want,
					)
				}
			},
		)
	}
}

func TestNewEventIDReadsSwitchOnce(t *testing.T) {
	t.Setenv(
		envIDV7,
		"false",
	)
	resetIDVersion(t)
	if got := eventIDVersion(t); got != 4 {
		t.Fatalf(
			"version %d, want 4",
			got,
		)
	}

	t.Setenv(
		envIDV7,
		"true",
	)
	if got := eventIDVersion(t); got != 4 {
		t.Fatalf(
			"version %d after env change, want 4",
			got,
		)
	}
}

func TestBuildMessageRowsUseTimeOrderedIDs(t *testing.T) {
	t.Setenv(
		envIDV7,
		"",
	)
	resetIDVersion(t)

	rows, err := buildMessageRows(
		[]Message{
			{
				Subject: "a",
			},
			{
				Subject: "b",
			},
		},
		nil,
		true,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		parsed, err := uuid.Parse(row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Version() != 7 {
			t.Fatalf(
				"row id %s version %d, want 7",
				row.ID,
				parsed.Version(),
			)
		}
	}
	if rows[0].ID >= rows[1].ID {
		t.Fatalf(
			"ids are not increasing: %s then %s",
			rows[0].ID,
			rows[1].ID,
		)
	}
}
