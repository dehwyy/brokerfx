package streamoptsbuilder

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCompressionFor(t *testing.T) {
	cases := []struct {
		role Role
		want jetstream.StoreCompression
	}{
		{
			role: RoleEvent,
			want: jetstream.S2Compression,
		},
		{
			role: RoleCommand,
			want: jetstream.NoCompression,
		},
		{
			role: RoleResult,
			want: jetstream.S2Compression,
		},
		{
			role: RoleReply,
			want: jetstream.NoCompression,
		},
		{
			role: RoleDLQ,
			want: jetstream.S2Compression,
		},
	}

	for _, tc := range cases {
		got, err := CompressionFor(tc.role)
		if err != nil {
			t.Fatalf(
				"%s: %v",
				tc.role,
				err,
			)
		}
		if got != tc.want {
			t.Fatalf(
				"CompressionFor(%s) = %s, want %s",
				tc.role,
				got,
				tc.want,
			)
		}
	}

	if _, err := CompressionFor(Role(42)); !errors.Is(
		err,
		ErrUnknownRole,
	) {
		t.Fatalf(
			"err = %v, want ErrUnknownRole",
			err,
		)
	}
}

func TestRoleCompressionAndExplicitOrder(t *testing.T) {
	cases := []struct {
		name    string
		builder *StreamOptsBuilder
		want    jetstream.StoreCompression
	}{
		{
			name:    "default event stream is s2",
			builder: NewDefault(),
			want:    jetstream.S2Compression,
		},
		{
			name:    "command role is none",
			builder: NewDefault().WithRole(RoleCommand),
			want:    jetstream.NoCompression,
		},
		{
			name:    "reply role is none",
			builder: NewDefault().WithRole(RoleReply),
			want:    jetstream.NoCompression,
		},
		{
			name:    "result role is s2",
			builder: NewDefault().WithRole(RoleResult),
			want:    jetstream.S2Compression,
		},
		{
			name:    "dlq role is s2",
			builder: NewDefault().WithRole(RoleDLQ),
			want:    jetstream.S2Compression,
		},
		{
			name:    "explicit before role wins",
			builder: NewDefault().WithCompression(jetstream.S2Compression).WithRole(RoleCommand),
			want:    jetstream.S2Compression,
		},
		{
			name:    "explicit after role wins",
			builder: NewDefault().WithRole(RoleCommand).WithCompression(jetstream.S2Compression),
			want:    jetstream.S2Compression,
		},
		{
			name:    "explicit none on event wins",
			builder: NewDefault().WithCompression(jetstream.NoCompression).WithRole(RoleEvent),
			want:    jetstream.NoCompression,
		},
		{
			name:    "last role wins without explicit",
			builder: NewDefault().WithRole(RoleCommand).WithRole(RoleResult),
			want:    jetstream.S2Compression,
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg := tc.builder.
					WithName("S").
					WithSubjects([]string{"s.>"}).
					Build()
				if cfg.Compression != tc.want {
					t.Fatalf(
						"compression = %s, want %s",
						cfg.Compression,
						tc.want,
					)
				}
			},
		)
	}
}
