package producer

type Event interface {
	Subject() string
	Data() any
}
