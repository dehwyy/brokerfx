package batch

func (c *Consumer) watchFrame(event FrameEvent) {
	if observer, ok := c.observer.(FrameObserver); ok {
		observer.OnFrame(event)
	}
}
