package frame

func (e *elementMsg) InProgress() error {
	return e.frame.progress(e)
}
