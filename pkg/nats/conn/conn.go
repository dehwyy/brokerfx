package conn

import (
	"context"
	"strings"
	"time"

	connbuilder "github.com/dehwyy/brokerfx/pkg/nats/conn/builder"
	nc "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type Opts struct {
	// Must be provided
	KeySecretServers string // Should have value of []string
	KeySecretSeedKey string // Should have value of string
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

func New(opts Opts) func(SecretsProvider) (*nc.Conn, error) {
	return func(secretsProvider SecretsProvider) (*nc.Conn, error) {

		// Get servers
		serversAny := secretsProvider.MustGet(context.Background(), opts.KeySecretServers).([]any)
		servers := make([]string, len(serversAny))
		for i, server := range serversAny {
			servers[i] = server.(string)
		}

		// Build connection options
		connOptsBuilder := connbuilder.NewConnBuilder()

		if opts.EnabledTLS {
			connOptsBuilder.WithTLS(opts.CertFile, opts.KeyFile, opts.CAFile)
		}
		if seedKey, err := secretsProvider.Get(context.Background(), opts.KeySecretSeedKey); err == nil {
			connOptsBuilder.WithNkey(seedKey.(string))
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
			strings.Join(servers, ","),
			connOptsBuilder.Build()...,
		)
		if err != nil {
			panic(err)
		}

		tt, err := conn.RTT()
		if err != nil {
			panic(err)
		}

		log.Info().Dur("RTT", tt).Any("params", opts).Msg("RoundTripTime received")
		return conn, nil
	}
}
