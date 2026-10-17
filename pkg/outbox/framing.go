package outbox

import "sync"

const (
	envFrameSubjects       = "BROKERFX_FRAME_SUBJECTS"
	envFrameMaxElements    = "BROKERFX_FRAME_MAX_ELEMENTS"
	envFrameMaxBytes       = "BROKERFX_FRAME_MAX_BYTES"
	defaultFrameMaxElement = 16
	defaultFrameMaxBytes   = 262144
	limitFrameMaxElements  = 1024
	minFrameMaxBytes       = 1024
	frameHeaderReserve     = 1024
)

type framing struct {
	patterns     []string
	maxElements  int
	maxBytes     int
	clampOnce    sync.Once
	legacyOnce   sync.Once
	latestOnce   sync.Once
	headerOnce   sync.Once
	oversizeOnce sync.Once
}

func (f *framing) active() bool {
	return f != nil && len(f.patterns) > 0
}

type FrameObserver interface {
	OnFramed(event FramedEvent)
}

type FramedEvent struct {
	Messages int
	Frames   int
	Elements int
}
