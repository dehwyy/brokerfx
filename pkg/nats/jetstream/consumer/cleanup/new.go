package cleanup

import "time"

func New(opts Opts) (*Cleaner, error) {
	s, err := resolve(opts)
	if err != nil {
		return nil, err
	}

	return &Cleaner{
		db:         opts.DB,
		retention:  s.retention,
		interval:   s.interval,
		batchLimit: s.batchLimit,
		onError:    opts.OnError,
		now:        time.Now,
		pause:      batchPause,
	}, nil
}
