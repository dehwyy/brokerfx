package batch

import (
	"strings"
	"testing"

	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
)

func TestClaimValuesQueryKeepsOrder(t *testing.T) {
	query, args, err := claimValuesQuery([]consumer.ProcessedMessage{
		{
			MessageID: "a",
			Subject:   "s.a",
		},
		{
			MessageID: "b",
			Subject:   "s.b",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(query, "(?, ?, NOW())") != 2 {
		t.Fatalf("query %s", query)
	}
	if len(args) != 4 || args[0] != "a" || args[2] != "b" {
		t.Fatalf("args %v", args)
	}
}

func TestBatchTextArrayLiteral(t *testing.T) {
	got := textArrayLiteral([]string{
		"stream:1",
		`q"\`,
	})
	if got != `{"stream:1","q\"\\"}` {
		t.Fatalf("literal %s", got)
	}
}
