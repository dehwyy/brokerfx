package batch

import "context"

func (m *itemMsg) DoubleAck(ctx context.Context) error {
	err := m.Msg.DoubleAck(ctx)
	m.record(
		settledAck,
		err,
	)

	return err
}
