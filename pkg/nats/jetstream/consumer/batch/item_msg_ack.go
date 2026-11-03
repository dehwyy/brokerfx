package batch

func (m *itemMsg) Ack() error {
	err := m.Msg.Ack()
	m.record(
		settledAck,
		err,
	)

	return err
}
