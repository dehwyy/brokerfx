package batch

import "github.com/nats-io/nats.go/jetstream"

type fetcher interface {
	Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error)
}
