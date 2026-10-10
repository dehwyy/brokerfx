package cleanup

import (
	"go.uber.org/fx"
	"gorm.io/gorm"
)

type moduleDeps struct {
	fx.In

	DB   *gorm.DB
	Opts Opts `optional:"true"`
}

var Module = fx.Module(
	"brokerfx-processed-cleanup",
	fx.Provide(newFromDeps),
	fx.Invoke(registerLifecycle),
)

func newFromDeps(deps moduleDeps) (*Cleaner, error) {
	opts := deps.Opts
	opts.DB = deps.DB

	return New(opts)
}

func registerLifecycle(
	lc fx.Lifecycle,
	cleaner *Cleaner,
) {
	lc.Append(
		fx.Hook{
			OnStart: cleaner.Start,
			OnStop:  cleaner.Stop,
		},
	)
}
