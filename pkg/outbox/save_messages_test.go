package outbox

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestBuildMessageRowsKeepsInputOrder(t *testing.T) {
	now := time.Date(
		2026,
		10,
		12,
		2,
		0,
		0,
		123456789,
		time.UTC,
	)
	messages := make(
		[]Message,
		300,
	)
	for i := range messages {
		messages[i] = Message{
			Subject: "orders.updated",
			Payload: []byte{
				byte(i),
			},
		}
	}

	rows, err := buildMessageRows(
		messages,
		nil,
		true,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(messages) {
		t.Fatalf("rows %d", len(rows))
	}

	ids := make(
		map[string]struct{},
		len(rows),
	)
	for i, row := range rows {
		if row.CreatedAt.Nanosecond()%1000 != 0 {
			t.Fatalf("row %d created_at not truncated to microseconds: %s", i, row.CreatedAt)
		}
		if i > 0 && !rows[i-1].CreatedAt.Before(row.CreatedAt) {
			t.Fatalf("row %d created_at %s not after %s", i, row.CreatedAt, rows[i-1].CreatedAt)
		}
		if !row.UpdatedAt.Equal(row.CreatedAt) {
			t.Fatalf("row %d updated_at %s", i, row.UpdatedAt)
		}
		if row.State != StatePending || row.Topic != "orders.updated" || row.Payload[0] != byte(i) {
			t.Fatalf("row %d %+v", i, row)
		}
		if row.Headers != nil {
			t.Fatalf("row %d headers %s", i, row.Headers)
		}
		ids[row.ID] = struct{}{}
	}
	if len(ids) != len(rows) {
		t.Fatalf("ids not unique: %d of %d", len(ids), len(rows))
	}
}

func TestBuildMessageRowsHeaders(t *testing.T) {
	kvDelete := KVDelete(
		"bucket",
		"key",
	)
	rows, err := buildMessageRows(
		[]Message{
			{
				Subject: "facade.event",
				Payload: []byte("a"),
			},
			kvDelete,
		},
		map[string]string{
			"Paylonium-Envelope": "v1",
			kvOperationHeader:    "PUT",
		},
		true,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	var shared map[string]string
	if err := json.Unmarshal(rows[0].Headers, &shared); err != nil {
		t.Fatal(err)
	}
	if len(shared) != 2 || shared["Paylonium-Envelope"] != "v1" {
		t.Fatalf("shared headers %v", shared)
	}

	var merged map[string]string
	if err := json.Unmarshal(rows[1].Headers, &merged); err != nil {
		t.Fatal(err)
	}
	if merged[kvOperationHeader] != kvOperationDelete || merged["Paylonium-Envelope"] != "v1" {
		t.Fatalf("message headers must win over options: %v", merged)
	}
	if len(rows[1].Payload) != 0 || rows[1].Payload == nil {
		t.Fatalf("kv delete payload %v", rows[1].Payload)
	}
}

func TestBuildMessageRowsNilPayloadBecomesEmpty(t *testing.T) {
	rows, err := buildMessageRows(
		[]Message{
			{
				Subject: "empty",
			},
		},
		nil,
		false,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Payload == nil {
		t.Fatal("nil payload must be stored as empty bytes")
	}
}

func TestBuildMessageRowsErrors(t *testing.T) {
	invalid := KVPut(
		"bad bucket",
		"key",
		nil,
	)

	cases := []struct {
		name     string
		messages []Message
		headers  map[string]string
		v2       bool
		want     error
	}{
		{
			name: "message error",
			messages: []Message{
				{
					Subject: "ok",
				},
				invalid,
			},
			v2:   true,
			want: ErrInvalidKVBucket,
		},
		{
			name: "headers on legacy schema",
			messages: []Message{
				{
					Subject: "ok",
				},
			},
			headers: map[string]string{
				"X-Trace": "1",
			},
			v2:   false,
			want: ErrSchemaOutdated,
		},
		{
			name: "message headers on legacy schema",
			messages: []Message{
				KVDelete(
					"bucket",
					"key",
				),
			},
			v2:   false,
			want: ErrSchemaOutdated,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				_, err := buildMessageRows(
					tc.messages,
					tc.headers,
					tc.v2,
					time.Now(),
				)
				if !errors.Is(err, tc.want) {
					t.Fatalf("err %v, want %v", err, tc.want)
				}
			},
		)
	}
}

func TestLegacyEventsFromRows(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	events := legacyEvents([]outboxEventRow{
		{
			ID:        "a",
			Topic:     "t",
			Payload:   []byte("p"),
			State:     StatePending,
			CreatedAt: now,
			UpdatedAt: now,
		},
	})

	if len(events) != 1 {
		t.Fatalf("events %d", len(events))
	}
	event := events[0]
	if event.ID != "a" || event.Topic != "t" || string(event.Payload) != "p" || event.State != StatePending || !event.CreatedAt.Equal(now) || !event.UpdatedAt.Equal(now) {
		t.Fatalf("event %+v", event)
	}
}
