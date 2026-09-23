package signature_test

import (
	"github.com/dehwyy/brokerfx/pkg/nats/signature"
	"github.com/dehwyy/brokerfx/pkg/outbox"
)

var _ outbox.Signer = (*signature.Ed25519Signer)(nil)
