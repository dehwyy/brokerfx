package conn

import (
	"errors"
	"strings"
	"time"

	connbuilder "github.com/dehwyy/brokerfx/pkg/nats/conn/builder"
	nc "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type Opts struct {
	// Must be provided
	Servers []string
	SeedKey string
	// Optional
	CertFile             string
	KeyFile              string
	CAFile               string
	ConnName             string
	EnabledTLS           bool
	RetryOnFailedConnect bool
	MaxReconnects        int
	ReconnectWait        time.Duration
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
		if opts.RetryOnFailedConnect {
			connOptsBuilder.WithRetryOnFailedConnect(true)
		}
		if opts.MaxReconnects > 0 {
			connOptsBuilder.WithMaxReconnects(opts.MaxReconnects)
		}
		if opts.ReconnectWait > 0 {
			connOptsBuilder.WithReconnectWait(opts.ReconnectWait)
		}
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
