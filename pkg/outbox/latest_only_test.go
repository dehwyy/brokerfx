package outbox

import (
	"reflect"
	"strconv"
	"testing"
)

func versioned(topic string, version int) relayEvent {
	return relayEvent{
		Topic: topic,
		Headers: map[string]string{
			LatestVersionHeader: strconv.Itoa(version),
		},
	}
}

func plain(topic string) relayEvent {
	return relayEvent{
		Topic: topic,
	}
}

func TestLatestIndexes(t *testing.T) {
	prefixes := latestOnlyPrefixes(
		[]string{
			"snap.",
		},
		nil,
	)

	cases := []struct {
		name   string
		events []relayEvent
		want   map[string]int
	}{
		{
			name: "kv last wins as before",
			events: []relayEvent{
				plain("$KV.b.k"),
				plain("$KV.b.k"),
				plain("other"),
			},
			want: map[string]int{
				"$KV.b.k": 1,
			},
		},
		{
			name: "max version wins",
			events: []relayEvent{
				versioned("snap.a", 3),
				versioned("snap.a", 5),
				versioned("snap.a", 4),
			},
			want: map[string]int{
				"snap.a": 1,
			},
		},
		{
			name: "equal versions take the later",
			events: []relayEvent{
				versioned("snap.a", 5),
				versioned("snap.a", 5),
			},
			want: map[string]int{
				"snap.a": 1,
			},
		},
		{
			name: "mixed versions fall back to the last",
			events: []relayEvent{
				versioned("snap.a", 9),
				plain("snap.a"),
				versioned("snap.a", 2),
			},
			want: map[string]int{
				"snap.a": 2,
			},
		},
		{
			name: "subjects are independent and unlisted ones untouched",
			events: []relayEvent{
				versioned("snap.a", 2),
				versioned("snap.b", 1),
				versioned("snap.a", 1),
				plain("events.x"),
				plain("events.x"),
			},
			want: map[string]int{
				"snap.a": 0,
				"snap.b": 1,
			},
		},
		{
			name: "lowercase header and bad value",
			events: []relayEvent{
				{
					Topic: "snap.a",
					Headers: map[string]string{
						"brokerfx-latest-version": "7",
					},
				},
				{
					Topic: "snap.a",
					Headers: map[string]string{
						LatestVersionHeader: "x",
					},
				},
			},
			want: map[string]int{
				"snap.a": 1,
			},
		},
	}

	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got := latestIndexes(
					tc.events,
					prefixes,
				)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("want %v got %v", tc.want, got)
				}
			},
		)
	}
}

func TestLatestOnlyPrefixesKeepKVAndMergeEnv(t *testing.T) {
	got := latestOnlyPrefixes(
		splitList(" snap.a., $KV.,"),
		splitList("snap.b.,snap.a."),
	)
	want := []string{
		"$KV.",
		"snap.a.",
		"snap.b.",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v got %v", want, got)
	}
}

type supersededRecorder struct {
	statsOnlyObserver
	count int
}

func (s *supersededRecorder) OnSuperseded(n int) {
	s.count += n
}

func TestRelayLatestIndexesReportsSuperseded(t *testing.T) {
	t.Setenv(
		envLatestOnlyPrefixes,
		"snap.",
	)
	observer := &supersededRecorder{}
	r := NewRelay(RelayDeps{
		Store:    &OutboxStore{},
		Observer: observer,
		Config: Config{
			Mode: ModeDeleteAfterSend,
		},
	})

	r.latestIndexes([]relayEvent{
		versioned("snap.a", 1),
		versioned("snap.a", 2),
		plain("$KV.b.k"),
		plain("$KV.b.k"),
		plain("other"),
	})
	if observer.count != 2 {
		t.Fatalf("superseded %d", observer.count)
	}
}
