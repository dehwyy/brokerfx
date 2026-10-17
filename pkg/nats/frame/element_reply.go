package frame

func (e *elementMsg) Reply() string {
	return e.frame.msg.Reply()
}
