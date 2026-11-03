package itemctx

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func TestWithBaggageKeepsOnlyAllowedMembersAndNoSpan(t *testing.T) {
	headers := nats.Header{
		"traceparent": []string{"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		"baggage":     []string{"pl.tx_id=tx-1,secret=x"},
	}

	ctx := WithBaggage(
		context.Background(),
		headers,
	)

	members := baggage.FromContext(ctx)
	if members.Member("pl.tx_id").Value() != "tx-1" {
		t.Fatalf(
			"pl.tx_id missing: %v",
			members,
		)
	}
	if members.Member("secret").Key() != "" {
		t.Fatal("non-pl member must be filtered")
	}
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("span context must not be applied")
	}
}

func TestWithBaggageNoHeadersKeepsContext(t *testing.T) {
	base := context.Background()
	if got := WithBaggage(
		base,
		nats.Header{},
	); got != base {
		t.Fatal("context must be unchanged")
	}
}
