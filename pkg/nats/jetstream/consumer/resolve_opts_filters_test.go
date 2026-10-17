package consumer

import (
	"errors"
	"testing"

	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
)

func TestResolveOptsRejectsBadFilters(t *testing.T) {
	cases := map[string]*consumeroptsbuilder.ConsumerOptsBuilder{
		"duplicate": consumeroptsbuilder.NewDefault().WithFilterSubjects("a", "a"),
		"empty":     consumeroptsbuilder.NewDefault().WithFilterSubjects("a", ""),
	}
	for name, builder := range cases {
		_, err := resolveOpts(Opts{ConsumerOptsBuilder: builder})
		if !errors.Is(err, ErrInvalidOpts) || !errors.Is(err, ErrInvalidFilters) {
			t.Fatalf("%s: %v", name, err)
		}
	}

	if _, err := resolveOpts(Opts{ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithFilterSubjects("a", "a.>")}); err != nil {
		t.Fatal(err)
	}
}
