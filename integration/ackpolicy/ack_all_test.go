//go:build integration

package ackpolicy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dehwyy/brokerfx/integration/testenv"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer"
	consumeroptsbuilder "github.com/dehwyy/brokerfx/pkg/nats/jetstream/consumer/consumer-opts-builder"
	"github.com/dehwyy/brokerfx/pkg/nats/jetstream/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const wqRequiresExplicitAck = 10098

func createStream(t *testing.T, js jetstream.JetStream, name string, retention jetstream.RetentionPolicy) {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{name + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: retention,
	})
	require.NoError(t, err)
}

func TestAckAllDurableOnWorkQueueIsRejectedWith10098(t *testing.T) {
	js, _ := testenv.EmbeddedNATS(t)
	createStream(t, js, "WQ", jetstream.WorkQueuePolicy)
	createStream(t, js, "LIM", jetstream.LimitsPolicy)

	cfg := consumeroptsbuilder.NewDefault().WithName("lane-p0", true).WithFilterSubject("WQ.cmd.p0").WithAckPolicy(jetstream.AckAllPolicy).Build()
	_, err := js.CreateOrUpdateConsumer(context.Background(), "WQ", cfg)
	var apiErr *jetstream.APIError
	require.True(t, errors.As(err, &apiErr), "err %v", err)
	require.Equal(t, jetstream.ErrorCode(wqRequiresExplicitAck), apiErr.ErrorCode)
	t.Logf("workqueue AckAll: code %d err_code %d %q", apiErr.Code, apiErr.ErrorCode, apiErr.Description)

	cfg.FilterSubject = "LIM.cmd.p0"
	_, err = js.CreateOrUpdateConsumer(context.Background(), "LIM", cfg)
	require.NoError(t, err, "a limits stream accepts AckAll")

	st, err := stream.Bind(js, "WQ")
	require.NoError(t, err)
	_, err = consumer.New(consumer.Opts{
		JetStream:           js,
		Stream:              st,
		ConsumerOptsBuilder: consumeroptsbuilder.NewDefault().WithName("lane-p1", true).WithFilterSubject("WQ.cmd.p1").WithAckPolicy(jetstream.AckAllPolicy),
		HandlerFunc:         func(context.Context, jetstream.Msg) error { return nil },
		StrictUpdate:        true,
	})
	require.True(t, errors.As(err, &apiErr), "consumer.New err %v", err)
	require.Equal(t, jetstream.ErrorCode(wqRequiresExplicitAck), apiErr.ErrorCode)
}
