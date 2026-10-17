package outbox

import (
	"github.com/dehwyy/brokerfx/pkg/nats/frame"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type wireFrame struct {
	members []int
	encoded [][]byte
	size    int
}

func (r *OutboxRelay) frameEvents(events []ProducerEvent) ([]ProducerEvent, [][]int, bool) {
	if !r.frames.active() || len(events) < 2 {
		return events, nil, false
	}

	maxBytes := r.frameMaxBytes()
	groups := make(
		[]*wireFrame,
		0,
		len(events),
	)
	open := make(map[string]*wireFrame)
	framed := false
	for i, event := range events {
		encoded, ok := r.frameElement(
			event,
			maxBytes,
		)
		if !ok {
			delete(
				open,
				event.Subject,
			)
			groups = append(
				groups,
				&wireFrame{
					members: []int{i},
				},
			)
			continue
		}

		group := open[event.Subject]
		if group != nil && (len(group.members) >= r.frames.maxElements || group.size+len(encoded) > maxBytes) {
			group = nil
		}
		if group == nil {
			group = &wireFrame{
				size: frame.MaxArrayHeader,
			}
			open[event.Subject] = group
			groups = append(
				groups,
				group,
			)
		}
		group.members = append(
			group.members,
			i,
		)
		group.encoded = append(
			group.encoded,
			encoded,
		)
		group.size += len(encoded)
		framed = framed || len(group.members) > 1
	}
	if !framed {
		return events, nil, false
	}

	return r.wireEvents(
		events,
		groups,
	)
}

func (r *OutboxRelay) wireEvents(
	events []ProducerEvent,
	groups []*wireFrame,
) ([]ProducerEvent, [][]int, bool) {
	wire := make(
		[]ProducerEvent,
		0,
		len(groups),
	)
	members := make(
		[][]int,
		0,
		len(groups),
	)
	frames := 0
	elements := 0
	for _, group := range groups {
		members = append(
			members,
			group.members,
		)
		if len(group.members) == 1 {
			wire = append(
				wire,
				events[group.members[0]],
			)
			continue
		}

		ids := make(
			[]string,
			0,
			len(group.members),
		)
		for _, index := range group.members {
			ids = append(
				ids,
				events[index].Headers[jetstream.MsgIDHeader],
			)
		}
		wire = append(
			wire,
			ProducerEvent{
				Subject: events[group.members[0]].Subject,
				Headers: frame.WireHeaders(ids),
				Payload: frame.Join(group.encoded),
			},
		)
		frames++
		elements += len(group.members)
	}

	if observer, ok := r.observer.(FrameObserver); ok {
		observer.OnFramed(FramedEvent{
			Messages: len(wire),
			Frames:   frames,
			Elements: elements,
		})
	}
	log.Debug().
		Int(
			"events",
			len(events),
		).
		Int(
			"messages",
			len(wire),
		).
		Int(
			"frames",
			frames,
		).
		Msg("outbox: events framed")

	return wire, members, true
}

func (r *OutboxRelay) frameElement(
	event ProducerEvent,
	maxBytes int,
) ([]byte, bool) {
	if !r.frameable(event) {
		return nil, false
	}

	headers := make(
		map[string][]string,
		len(event.Headers),
	)
	for key, value := range event.Headers {
		headers[key] = []string{value}
	}
	encoded, err := frame.Marshal(frame.Element{
		Subject: event.Subject,
		Headers: headers,
		Payload: event.Payload,
	})
	if err != nil {
		log.Warn().
			Err(err).
			Str(
				"subject",
				event.Subject,
			).
			Msg("outbox: frame element encoding failed, publishing it per message")
		return nil, false
	}
	if len(encoded)+frame.MaxArrayHeader > maxBytes {
		r.frames.oversizeOnce.Do(func() {
			log.Info().
				Str(
					"subject",
					event.Subject,
				).
				Int(
					"size",
					len(encoded),
				).
				Int(
					"frame_max_bytes",
					maxBytes,
				).
				Msg("outbox: event above the frame size goes per message")
		})
		return nil, false
	}

	return encoded, true
}
