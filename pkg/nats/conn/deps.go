package conn

import "context"

type SecretsProvider interface {
	MustGet(ctx context.Context, key string) any
	Get(ctx context.Context, key string) (any, error)
}
