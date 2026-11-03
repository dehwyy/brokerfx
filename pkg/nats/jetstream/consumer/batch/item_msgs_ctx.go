package batch

import "context"

type itemMsgsKey struct{}

func withItemMsgs(ctx context.Context, msgs []*itemMsg) context.Context {
	return context.WithValue(
		ctx,
		itemMsgsKey{},
		msgs,
	)
}

func itemMsgsFrom(ctx context.Context) []*itemMsg {
	msgs, _ := ctx.Value(itemMsgsKey{}).([]*itemMsg)

	return msgs
}
