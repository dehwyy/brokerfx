package conn

import (
	"errors"
	"strings"
	"time"

	connbuilder "github.com/dehwyy/brokerfx/pkg/nats/conn/builder"
	nc "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

type Opts struct {
	// Must be provided
	Servers []string
	SeedKey string
	// Optional
	CertFile   string
	KeyFile    string
	CAFile     string
	ConnName   string
	EnabledTLS bool
	// RetryOnFailedConnect is kept for compatibility: the retry is always enabled.
	RetryOnFailedConnect bool
	// MaxReconnects: 0 and negative mean unlimited (-1); a positive value caps the attempts.
	MaxReconnects int
	// ReconnectWait defaults to DefaultReconnectWait.
	ReconnectWait time.Duration
	// Shutdowner, when set, is invoked once the connection is closed for good
	// (reconnects exhausted or an unrecoverable error), so the pod restarts instead
	// of hanging on a dead connection. Not triggered by a deliberate Close or Drain.
	Shutdowner fx.Shutdowner
	// OnClosed is an extra hook called when the connection is closed for good.
	OnClosed func(*nc.Conn)
}

const (
	DefaultReconnectWait   = 2 * time.Second
	DefaultReconnectJitter = 500 * time.Millisecond
	UnlimitedReconnects    = -1
)

func resolveMaxReconnects(v int) int {
	if v > 0 {
		return v
	}

	return UnlimitedReconnects
}

func resolveReconnectWait(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}

	return DefaultReconnectWait
}

func closedHandler(opts Opts) nc.ConnHandler {
	return func(c *nc.Conn) {
		deliberate := c != nil && c.LastError() == nil
		if deliberate {
			log.Info().Msg("nats connection closed")
		} else {
			log.Error().Err(lastError(c)).Msg("nats connection closed")
		}

		if opts.OnClosed != nil {
			opts.OnClosed(c)
		}

		if deliberate || opts.Shutdowner == nil {
			return
		}

		go func() {
			if err := opts.Shutdowner.Shutdown(fx.ExitCode(1)); err != nil {
				log.Error().Err(err).Msg("fx shutdown after nats connection closed failed")
			}
		}()
	}
}

func lastError(c *nc.Conn) error {
	if c == nil {
		return nil
	}

	return c.LastError()
}

func New(opts Opts) func() (*nc.Conn, error) {
	return func() (*nc.Conn, error) {
		if len(opts.Servers) == 0 {
			return nil, errors.New("'servers' must be provided")
		}
		if opts.SeedKey == "" {
			return nil, errors.New("'seedKey' must be provided")
		}

		// Build connection options
		connOptsBuilder := connbuilder.NewConnBuilder().
			WithNkey(opts.SeedKey)

		if opts.EnabledTLS {
			connOptsBuilder.WithTLS(opts.CertFile, opts.KeyFile, opts.CAFile)
		}
		connOptsBuilder.
			WithRetryOnFailedConnect(true).
			WithMaxReconnects(resolveMaxReconnects(opts.MaxReconnects)).
			WithReconnectWait(resolveReconnectWait(opts.ReconnectWait)).
			WithReconnectJitter(DefaultReconnectJitter).
			WithDisconnectErrHandler(func(_ *nc.Conn, err error) {
				log.Warn().Err(err).Msg("nats disconnected")
			}).
			WithReconnectHandler(func(c *nc.Conn) {
				log.Info().Str("url", c.ConnectedUrl()).Msg("nats reconnected")
			}).
			WithClosedHandler(closedHandler(opts))
		if opts.ConnName != "" {
			connOptsBuilder.WithConnName(opts.ConnName)
		}

		conn, err := nc.Connect(
			strings.Join(opts.Servers, ","),
			connOptsBuilder.Build()...,
		)
		if err != nil {
			panic(err)
		}

		if tt, rttErr := conn.RTT(); rttErr != nil {
			log.Warn().Err(rttErr).Msg("initial RTT check failed, connection still establishing")
		} else {
			log.Info().Dur("RTT", tt).Msg("RoundTripTime received")
		}

		return conn, nil
	}
}
