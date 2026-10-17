package batch

type noopObserver struct{}

func (noopObserver) OnFetch(FetchEvent) {}

func (noopObserver) OnApply(ApplyEvent) {}

func (noopObserver) OnBisect(BisectEvent) {}

func (noopObserver) OnSettle(SettleEvent) {}

func observerOrNoop(observer Observer) Observer {
	if observer == nil {
		return noopObserver{}
	}

	return observer
}

func (c *Consumer) watch() Observer {
	return observerOrNoop(c.observer)
}
