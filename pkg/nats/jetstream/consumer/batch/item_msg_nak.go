package batch

func (m *itemMsg) Nak() error {
	err := m.Msg.Nak()
	m.record(
		settledRetry,
		err,
	)

	return err
}
