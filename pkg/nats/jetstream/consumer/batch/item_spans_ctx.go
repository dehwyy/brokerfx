package batch

import "context"

type itemSpansKey struct{}

func withItemSpans(ctx context.Context, spans itemSpans) context.Context {
	return context.WithValue(
		ctx,
		itemSpansKey{},
		spans,
	)
}

func itemSpansFrom(ctx context.Context) itemSpans {
	spans, _ := ctx.Value(itemSpansKey{}).(itemSpans)

	return spans
}
