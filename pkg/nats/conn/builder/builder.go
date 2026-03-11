package connbuilder

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type ConnBuilder struct {
	opts []nats.Option
}

func NewConnBuilder() *ConnBuilder {
	return &ConnBuilder{
		opts: make([]nats.Option, 0),
	}
}

func (b *ConnBuilder) Build() []nats.Option {
	return append(
		b.opts,
		nats.PingInterval(20*time.Second),
		nats.MaxPingsOutstanding(3),
	)
}

func (b *ConnBuilder) WithTLS(
	certFile string,
	keyFile string,
	caFile string,
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.ClientCert(certFile, keyFile),
		nats.RootCAs(caFile),
	)
	return b
}

func (b *ConnBuilder) WithNkey(
	seedKey string,
) *ConnBuilder {
	kp, err := nkeys.FromSeed([]byte(seedKey))
	if err != nil {
		panic(err)
	}
	pubKey, err := kp.PublicKey()
	if err != nil {
		panic(err)
	}
	b.opts = append(
		b.opts,
		nats.Nkey(pubKey, func(nonce []byte) ([]byte, error) {
			sig, err := kp.Sign(nonce)
			if err != nil {
				return nil, err
			}
			return sig, nil
		}),
	)
	return b
}

func (b *ConnBuilder) WithRetryOnFailedConnect(
	retryOnFailedConnect bool,
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.RetryOnFailedConnect(retryOnFailedConnect),
	)
	return b
}

func (b *ConnBuilder) WithMaxReconnects(
	maxReconnects int,
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.MaxReconnects(maxReconnects),
	)
	return b
}

func (b *ConnBuilder) WithReconnectWait(
	reconnectWait time.Duration,
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.ReconnectWait(reconnectWait),
	)
	return b
}

func (b *ConnBuilder) WithReconnectHandler(
	reconnectHandler func(*nats.Conn),
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.ReconnectHandler(reconnectHandler),
	)
	return b
}

func (b *ConnBuilder) WithConnName(
	connName string,
) *ConnBuilder {
	b.opts = append(
		b.opts,
		nats.Name(connName),
	)
	return b
}
