package frame

type Element struct {
	Subject string              `msgpack:"subject"`
	Headers map[string][]string `msgpack:"headers"`
	Payload []byte              `msgpack:"payload"`
}
