package signature

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"strconv"
	"time"
)

const (
	HeaderAPIKeyID  = "X-API-Key-ID"
	HeaderTimestamp = "X-Timestamp"
	HeaderSignature = "X-Signature"
)

var (
	ErrInvalidSignature = errors.New("invalid signature")
	ErrSignatureExpired = errors.New("signature expired")
	ErrMissingHeader    = errors.New("missing signature header")
)

func Sign(
	headers map[string]string,
	apiKeyID string,
	privateKey ed25519.PrivateKey,
	subject []byte,
	body []byte,
) error {
	if headers == nil {
		return errors.New("headers map is nil")
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

	payload := buildSignaturePayload(subject, []byte(timestamp), body)
	sig := ed25519.Sign(privateKey, payload)

	headers[HeaderAPIKeyID] = apiKeyID
	headers[HeaderTimestamp] = timestamp
	headers[HeaderSignature] = base64.StdEncoding.EncodeToString(sig)

	return nil
}

func Verify(
	headers map[string]string,
	publicKey ed25519.PublicKey,
	subject []byte,
	body []byte,
	now time.Time,
	window time.Duration,
) (string, error) {
	apiKeyID, ok := headers[HeaderAPIKeyID]
	if !ok || apiKeyID == "" {
		return "", ErrMissingHeader
	}

	timestampStr, ok := headers[HeaderTimestamp]
	if !ok || timestampStr == "" {
		return "", ErrMissingHeader
	}

	signatureStr, ok := headers[HeaderSignature]
	if !ok || signatureStr == "" {
		return "", ErrMissingHeader
	}

	timestampMs, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return "", ErrInvalidSignature
	}

	messageTime := time.UnixMilli(timestampMs)
	delta := now.Sub(messageTime)
	if delta < 0 {
		delta = -delta
	}
	if delta > window {
		return "", ErrSignatureExpired
	}

	sig, err := base64.StdEncoding.DecodeString(signatureStr)
	if err != nil {
		return "", ErrInvalidSignature
	}

	payload := buildSignaturePayload(subject, []byte(timestampStr), body)
	if !ed25519.Verify(publicKey, payload, sig) {
		return "", ErrInvalidSignature
	}

	return apiKeyID, nil
}

func buildSignaturePayload(subject, timestamp, body []byte) []byte {
	out := make([]byte, 0, len(subject)+1+len(timestamp)+1+len(body))
	out = append(out, subject...)
	out = append(out, '\n')
	out = append(out, timestamp...)
	out = append(out, '\n')
	out = append(out, body...)
	return out
}

type Ed25519Signer struct {
	apiKeyID   string
	privateKey ed25519.PrivateKey
}

func NewEd25519Signer(apiKeyID string, privateKey ed25519.PrivateKey) *Ed25519Signer {
	return &Ed25519Signer{
		apiKeyID:   apiKeyID,
		privateKey: privateKey,
	}
}

func (s *Ed25519Signer) Sign(subject string, headers map[string]string, payload []byte) error {
	return Sign(headers, s.apiKeyID, s.privateKey, []byte(subject), payload)
}
