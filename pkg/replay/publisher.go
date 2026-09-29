package replay

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

const maxErrorLen = 256

type publishAPI interface {
	PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type streamAPI interface {
	StreamNameBySubject(ctx context.Context, subject string) (string, error)
	Stream(ctx context.Context, name string) (jetstream.Stream, error)
}

type Result struct {
	Published  int64
	Duplicates int64
	Status     string
}

type Publisher struct {
	pub        publishAPI
	streams    streamAPI
	maxPayload func() int64
	cfg        Config
}

func NewPublisher(js jetstream.JetStream, cfg Config) (*Publisher, error) {
	if js == nil {
		return nil, errors.New("replay: jetstream is nil")
	}
	p, err := newPublisher(js, func() int64 { return js.Conn().MaxPayload() }, cfg)
	if err != nil {
		return nil, err
	}
	p.streams = js
	return p, nil
}

func newPublisher(pub publishAPI, maxPayload func() int64, cfg Config) (*Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Publisher{pub: pub, maxPayload: maxPayload, cfg: cfg}, nil
}

func (p *Publisher) Run(ctx context.Context, req Request, src Source) (Result, error) {
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	if src == nil {
		return Result{}, fmt.Errorf("%w: source is nil", ErrInvalidRequest)
	}
	if err := p.checkStream(ctx, req.Subject); err != nil {
		return Result{}, err
	}
	log.Info().Str("replay_id", req.ReplayID).Str("subject", req.Subject).Msg("replay: started")

	res, err := p.snapshots(ctx, req, src)
	if err == nil {
		err = p.finish(ctx, req, res, StatusComplete, "")
	}
	if err != nil {
		res.Status = StatusFailed
		if merr := p.finish(ctx, req, res, StatusFailed, err.Error()); merr != nil {
			err = errors.Join(err, merr)
		}
		log.Warn().Err(err).Str("replay_id", req.ReplayID).Int64("published", res.Published).Msg("replay: failed")
		return res, err
	}
	res.Status = StatusComplete
	log.Info().Str("replay_id", req.ReplayID).Int64("published", res.Published).Int64("duplicates", res.Duplicates).Msg("replay: complete")
	return res, nil
}

func (p *Publisher) checkStream(ctx context.Context, subject string) error {
	if p.streams == nil {
		return nil
	}
	name, err := p.streams.StreamNameBySubject(ctx, subject)
	if err != nil {
		return fmt.Errorf("%w: no stream for subject %q: %w", ErrStreamUnsuitable, subject, err)
	}
	stream, err := p.streams.Stream(ctx, name)
	if err != nil {
		return fmt.Errorf("%w: stream %q: %w", ErrStreamUnsuitable, name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("%w: stream %q: %w", ErrStreamUnsuitable, name, err)
	}
	if info.Config.MaxMsgsPerSubject > 0 {
		return fmt.Errorf("%w: stream %q has MaxMsgsPerSubject %d", ErrStreamUnsuitable, name, info.Config.MaxMsgsPerSubject)
	}
	return nil
}

var errMarkerFailed = errors.New("replay: end marker not published")

func (p *Publisher) snapshots(ctx context.Context, req Request, src Source) (Result, error) {
	var res Result
	var seq int64
	start := time.Now()
	interval := time.Second / time.Duration(p.cfg.RatePerSecond)
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		items, err := src.Next(ctx)
		if err != nil {
			return res, fmt.Errorf("replay: source: %w", err)
		}
		if len(items) == 0 {
			return res, nil
		}
		for _, it := range items {
			if err := it.Validate(); err != nil {
				return res, err
			}
			if err := waitUntil(ctx, start.Add(time.Duration(seq)*interval)); err != nil {
				return res, err
			}
			seq++
			msg := buildSnapshot(req, it, seq)
			if err := p.checkSize(msg, it.Key); err != nil {
				return res, err
			}
			ack, err := p.publish(ctx, msg)
			if err != nil {
				return res, fmt.Errorf("replay: publish key %q: %w", it.Key, err)
			}
			if ack != nil && ack.Duplicate {
				res.Duplicates++
			} else {
				res.Published++
			}
		}
	}
}

func (p *Publisher) finish(ctx context.Context, req Request, res Result, status, cause string) error {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.FinalizeTimeout)
	defer cancel()
	msg := &nats.Msg{Subject: req.Subject, Data: req.EndPayload, Header: nats.Header{}}
	msg.Header.Set(HeaderReplayID, req.ReplayID)
	msg.Header.Set(HeaderEnd, endTrue)
	msg.Header.Set(HeaderCount, strconv.FormatInt(res.Published+res.Duplicates, 10))
	msg.Header.Set(HeaderStatus, status)
	msg.Header.Set(nats.MsgIdHdr, req.ReplayID+":end:"+status)
	if status == StatusFailed {
		msg.Header.Set(HeaderError, sanitizeError(cause))
	}
	if _, err := p.publish(fctx, msg); err != nil {
		return fmt.Errorf("%w: %s: %w", errMarkerFailed, status, err)
	}
	return nil
}

func (p *Publisher) publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
	var last error
	for attempt := 1; attempt <= p.cfg.PublishAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ack, err := p.pub.PublishMsg(ctx, msg)
		if err == nil {
			return ack, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if attempt == p.cfg.PublishAttempts {
			break
		}
		log.Warn().Err(err).Int("attempt", attempt).Str("subject", msg.Subject).Msg("replay: publish retry")
		if err := sleepCtx(ctx, p.cfg.RetryDelay); err != nil {
			return nil, err
		}
	}
	return nil, last
}

func (p *Publisher) checkSize(msg *nats.Msg, key string) error {
	limit := p.maxPayload()
	if limit <= 0 {
		return nil
	}
	size := int64(len(msg.Subject) + len(msg.Data))
	if len(msg.Header) > 0 {
		size += int64(len("NATS/1.0\r\n") + len("\r\n"))
		for k, vs := range msg.Header {
			for _, v := range vs {
				size += int64(len(k) + len(": ") + len(v) + len("\r\n"))
			}
		}
	}
	if size > limit {
		return fmt.Errorf("%w: key %q is %d bytes, limit %d", ErrPayloadTooLarge, key, size, limit)
	}
	return nil
}

func buildSnapshot(req Request, it Item, seq int64) *nats.Msg {
	msg := &nats.Msg{Subject: req.Subject, Data: it.Payload, Header: nats.Header{}}
	for k, v := range it.Headers {
		msg.Header.Set(k, v)
	}
	msg.Header.Set(HeaderReplayID, req.ReplayID)
	msg.Header.Set(HeaderSeq, strconv.FormatInt(seq, 10))
	msg.Header.Set(nats.MsgIdHdr, req.ReplayID+":"+it.Key+":"+it.Version)
	return msg
}

func sanitizeError(s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	if len(s) > maxErrorLen {
		s = strings.ToValidUTF8(s[:maxErrorLen], "")
	}
	return s
}

func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	return sleepCtx(ctx, d)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
