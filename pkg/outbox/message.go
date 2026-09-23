package outbox

import (
	"errors"
	"net/textproto"
	"regexp"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrReservedHeader  = errors.New("outbox: header is reserved")
	ErrInvalidKVBucket = errors.New("outbox: invalid kv bucket")
	ErrInvalidKVKey    = errors.New("outbox: invalid kv key")
)

const (
	kvOperationHeader = "KV-Operation"
	kvOperationDelete = "DEL"
)

var (
	kvBucketPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	kvKeyPattern    = regexp.MustCompile(`^[-/_=.A-Za-z0-9]+$`)
)

type Message struct {
	Subject string
	Payload []byte

	headers map[string]string
	err     error
}

func KVPut(bucket, key string, value []byte) Message {
	subject, err := kvSubject(bucket, key)
	if err != nil {
		return Message{err: err}
	}

	payload := value
	if payload == nil {
		payload = []byte{}
	}

	return Message{
		Subject: subject,
		Payload: payload,
	}
}

func KVDelete(bucket, key string) Message {
	subject, err := kvSubject(bucket, key)
	if err != nil {
		return Message{err: err}
	}

	return Message{
		Subject: subject,
		Payload: []byte{},
		headers: map[string]string{
			kvOperationHeader: kvOperationDelete,
		},
	}
}

func kvSubject(bucket, key string) (string, error) {
	if !kvBucketPattern.MatchString(bucket) {
		return "", ErrInvalidKVBucket
	}

	if !kvKeyValid(key) {
		return "", ErrInvalidKVKey
	}

	return kvSubjectPrefix + bucket + "." + key, nil
}

func kvKeyValid(key string) bool {
	if key == "" || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}

	return kvKeyPattern.MatchString(key)
}

type messageOptions struct {
	headers map[string]string
}

type MessageOption func(*messageOptions)

func WithHeaders(headers map[string]string) MessageOption {
	return func(o *messageOptions) {
		if o.headers == nil {
			o.headers = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			o.headers[k] = v
		}
	}
}

func newMessageOptions(opts []MessageOption) (messageOptions, error) {
	var options messageOptions
	for _, opt := range opts {
		opt(&options)
	}

	reserved := textproto.CanonicalMIMEHeaderKey(jetstream.MsgIDHeader)
	for k := range options.headers {
		if textproto.CanonicalMIMEHeaderKey(k) == reserved {
			return messageOptions{}, ErrReservedHeader
		}
	}

	return options, nil
}
