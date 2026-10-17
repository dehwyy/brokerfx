package outbox

func (p *JetStreamProducer) MaxPayload() int64 {
	return p.js.Conn().MaxPayload()
}
