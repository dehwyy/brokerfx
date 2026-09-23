package core

import (
	"crypto/ed25519"
	"time"

	"github.com/dehwyy/brokerfx/pkg/nats/signature"
)

const (
	HeaderAPIKeyID  = signature.HeaderAPIKeyID
	HeaderTimestamp = signature.HeaderTimestamp
	HeaderSignature = signature.HeaderSignature
)

var (
	ErrInvalidSignature = signature.ErrInvalidSignature
	ErrSignatureExpired = signature.ErrSignatureExpired
	ErrMissingHeader    = signature.ErrMissingHeader
)

func Sign(
	headers map[string]string,
	apiKeyID string,
	privateKey ed25519.PrivateKey,
	subject []byte,
	body []byte,
) error {
	return signature.Sign(headers, apiKeyID, privateKey, subject, body)
}

func Verify(
	headers map[string]string,
	publicKey ed25519.PublicKey,
	subject []byte,
	body []byte,
	now time.Time,
	window time.Duration,
) (string, error) {
	return signature.Verify(headers, publicKey, subject, body, now, window)
}
