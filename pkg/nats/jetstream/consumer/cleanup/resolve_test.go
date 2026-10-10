package cleanup

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestResolve(t *testing.T) {
	db := &gorm.DB{}
	cases := []struct {
		name          string
		env           map[string]string
		opts          Opts
		wantErr       error
		wantRetention time.Duration
		wantInterval  time.Duration
		wantLimit     int
	}{
		{name: "defaults", opts: Opts{DB: db}, wantRetention: 48 * time.Hour, wantInterval: time.Hour, wantLimit: 5000},
		{name: "env retention", env: map[string]string{envRetention: "72h"}, opts: Opts{DB: db}, wantRetention: 72 * time.Hour, wantInterval: time.Hour, wantLimit: 5000},
		{name: "env interval", env: map[string]string{envInterval: "10m"}, opts: Opts{DB: db}, wantRetention: 48 * time.Hour, wantInterval: 10 * time.Minute, wantLimit: 5000},
		{name: "opts win", env: map[string]string{envRetention: "72h"}, opts: Opts{DB: db, Retention: 24 * time.Hour, BatchLimit: 10}, wantRetention: 24 * time.Hour, wantInterval: time.Hour, wantLimit: 10},
		{name: "small duplicates ok", opts: Opts{DB: db, Duplicates: 2 * time.Minute}, wantRetention: 48 * time.Hour, wantInterval: time.Hour, wantLimit: 5000},
		{name: "broken env", env: map[string]string{envRetention: "x"}, opts: Opts{DB: db}, wantErr: ErrInvalidEnv},
		{name: "retention too short", opts: Opts{DB: db, Retention: 30 * time.Minute}, wantErr: ErrInvalidOpts},
		{name: "retention within duplicates", opts: Opts{DB: db, Duplicates: 30 * time.Hour}, wantErr: ErrInvalidOpts},
		{name: "negative interval", opts: Opts{DB: db, Interval: -time.Second}, wantErr: ErrInvalidOpts},
		{name: "negative limit", opts: Opts{DB: db, BatchLimit: -1}, wantErr: ErrInvalidOpts},
		{name: "nil db", opts: Opts{}, wantErr: ErrInvalidOpts},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{envRetention, envInterval} {
				t.Setenv(name, tc.env[name])
			}

			s, err := resolve(tc.opts)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected %v", err)
			}
			if s.retention != tc.wantRetention || s.interval != tc.wantInterval || s.batchLimit != tc.wantLimit {
				t.Fatalf("got %+v", s)
			}
		})
	}
}
