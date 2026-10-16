package outbox

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestBuildMessageRowsElementHeaders(t *testing.T) {
	kvDelete := KVDelete(
		"bucket",
		"key",
	)
	kvDelete.Headers = map[string]string{
		kvOperationHeader: "PUT",
		"X-Element":       "kv",
	}

	rows, err := buildMessageRows(
		[]Message{
			{
				Subject: "a",
				Headers: map[string]string{
					"Paylonium-Signature": "sig-a",
					"X-Shared":            "element",
				},
			},
			{
				Subject: "b",
			},
			kvDelete,
		},
		map[string]string{
			"X-Shared": "shared",
		},
		true,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	decoded := make(
		[]map[string]string,
		len(rows),
	)
	for i, row := range rows {
		if err := json.Unmarshal(row.Headers, &decoded[i]); err != nil {
			t.Fatal(err)
		}
	}

	if decoded[0]["Paylonium-Signature"] != "sig-a" || decoded[0]["X-Shared"] != "element" {
		t.Fatalf("element headers must win over shared: %v", decoded[0])
	}
	if len(decoded[1]) != 1 || decoded[1]["X-Shared"] != "shared" {
		t.Fatalf("element without headers keeps shared: %v", decoded[1])
	}
	if decoded[2][kvOperationHeader] != kvOperationDelete || decoded[2]["X-Element"] != "kv" {
		t.Fatalf("kv operation must survive element headers: %v", decoded[2])
	}
}

func TestBuildMessageRowsElementHeaderErrors(t *testing.T) {
	cases := []struct {
		name    string
		message Message
		v2      bool
		want    error
	}{
		{
			name: "reserved msg id",
			message: Message{
				Subject: "a",
				Headers: map[string]string{
					"nats-msg-id": "x",
				},
			},
			v2:   true,
			want: ErrReservedHeader,
		},
		{
			name: "legacy schema",
			message: Message{
				Subject: "a",
				Headers: map[string]string{
					"X-Trace": "1",
				},
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
					[]Message{
						tc.message,
					},
					nil,
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
