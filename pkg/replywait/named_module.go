package replywait

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/fx"
)

func NamedModule(name string) fx.Option {
	tag := fmt.Sprintf(`name:%q`, name)

	return fx.Module(
		"replywait-"+name,
		fx.Provide(
			fx.Annotate(
				newModuleComponentsFromParts,
				fx.ParamTags(tag, tag, `optional:"true"`, `optional:"true"`),
				fx.ResultTags(tag, tag),
			),
		),
		fx.Invoke(
			fx.Annotate(
				registerModuleLifecycle,
				fx.ParamTags(``, tag, tag),
			),
		),
	)
}

func newModuleComponentsFromParts(
	js jetstream.JetStream,
	cfg Config,
	waker Waker,
	observer Observer,
) (*ReplyWaiter, *listener, error) {
	return newModuleComponents(ModuleDeps{
		JS:       js,
		Config:   cfg,
		Waker:    waker,
		Observer: observer,
	})
}
