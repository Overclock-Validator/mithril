package dashboard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func safetyFixture(now time.Time) Snapshot {
	s := testSnapshot(now)
	s.Consensus = &Consensus{SampledAt: now}
	return s
}

func TestPublicSafetyFaultWithoutLifecycle(t *testing.T) {
	now := time.Now().UTC()
	s := safetyFixture(now)
	fault := true
	s.Consensus.SafetyFault = &fault
	b, err := encodePublicSnapshotProfile(&s, "fresh", now, nil, nil, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Consensus struct {
			SafetyFault *bool `json:"safety_fault"`
		} `json:"consensus"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Consensus.SafetyFault == nil || !*got.Consensus.SafetyFault {
		t.Fatal("an observed safety fault disappeared with missing lifecycle")
	}
}

func TestSafetyFaultProfilesPreserveUnknown(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name string
		wire string
		want string
	}{
		{"missing", "", ""},
		{"null", "null", ""},
		{"false", "false", "false"},
		{"true", "true", "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := safetyFixture(now)
			raw, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var snapshot, consensus map[string]json.RawMessage
			if json.Unmarshal(raw, &snapshot) != nil || json.Unmarshal(snapshot["consensus"], &consensus) != nil {
				t.Fatal("invalid snapshot fixture")
			}
			if tc.wire == "" {
				delete(consensus, "safety_fault")
			} else {
				consensus["safety_fault"] = json.RawMessage(tc.wire)
			}
			snapshot["consensus"], err = json.Marshal(consensus)
			if err != nil {
				t.Fatal(err)
			}
			raw, err = json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			// Decode into a new value: missing JSON fields must not retain old data.
			s = Snapshot{}
			if err := json.Unmarshal(raw, &s); err != nil {
				t.Fatal(err)
			}
			if (s.Consensus.SafetyFault == nil) != (tc.want == "") {
				t.Fatal("private fault presence was lost")
			}
			c := &collector{public: true}
			frames, err := c.encode(&s, "fresh", now)
			if err != nil {
				t.Fatal(err)
			}
			if len(frames.safetyPublicJSON) > MaxSnapshotBytes || len(frames.safetyMessages) != 1 {
				t.Fatal("v3 response missing or oversized")
			}
			var frame struct{ Value json.RawMessage }
			if json.Unmarshal(frames.safetyMessages[0], &frame) != nil || string(frame.Value) != string(frames.safetyPublicJSON) || len(frames.safetyMessages[0]) > MaxSnapshotBytes+128 {
				t.Fatal("v3 HTTP and WebSocket payloads differ or exceed their bounds")
			}
			for profile := 0; profile <= 3; profile++ {
				var message struct {
					Consensus map[string]json.RawMessage `json:"consensus"`
				}
				var v struct{ Value json.RawMessage }
				msgs := frames.messagesForProfile(profile)
				if len(msgs) != 1 || json.Unmarshal(msgs[0], &v) != nil || json.Unmarshal(v.Value, &message) != nil {
					t.Fatalf("invalid profile %d", profile)
				}
				got, present := message.Consensus["safety_fault"]
				if present != (profile == 3 && tc.want != "") || present && string(got) != tc.want {
					t.Fatalf("profile %d: safety_fault %s, present=%v", profile, got, present)
				}
			}
		})
	}
}

func TestSafetyFaultSurvivesStaleSource(t *testing.T) {
	now := time.Now().UTC()
	{
		s := safetyFixture(now)
		fault := true
		s.Consensus.SafetyFault = &fault
		c := &collector{public: true}
		frames, err := c.encode(&s, "stale", now)
		if err != nil {
			t.Fatal(err)
		}
		var got PublicSnapshot
		if json.Unmarshal(frames.safetyPublicJSON, &got) != nil || got.SourceState != "stale" || got.Consensus == nil || got.Consensus.State != "stale" || got.Consensus.SafetyFault == nil || !*got.Consensus.SafetyFault {
			t.Fatalf("fault lost after lifecycle rejection or staleness: %+v", got.Consensus)
		}
	}
	c := &collector{public: true}
	frames, err := c.encode(nil, "unavailable", now)
	if err != nil || !strings.Contains(string(frames.safetyPublicJSON), `"source_state":"unavailable"`) || strings.Contains(string(frames.safetyPublicJSON), `"safety_fault"`) {
		t.Fatal("unavailable source fabricated a safety status")
	}
}

func TestSafetyFaultCollectorSourceLossAndReplacement(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	now := time.Now().UTC()
	s := testSnapshot(now)
	fault := true
	s.Consensus = &Consensus{SampledAt: now, SafetyFault: &fault}
	setFixture(t, body, s)
	c.poll(context.Background(), now)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().safetyPublicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := read(); p.SourceState != "fresh" || p.Consensus == nil || p.Consensus.SafetyFault == nil || !*p.Consensus.SafetyFault {
		t.Fatal("current fault was lost")
	}
	body.Store(nil)
	c.poll(context.Background(), now.Add(2*time.Second))
	if p := read(); p.SourceState != "stale" || p.Consensus == nil || p.Consensus.State != "stale" || p.Consensus.SafetyFault == nil || !*p.Consensus.SafetyFault {
		t.Fatal("source loss did not mark the last fault stale")
	}
	s.RunID, s.SampledAt = "new-run", now.Add(3*time.Second)
	s.Consensus = &Consensus{SampledAt: s.SampledAt}
	setFixture(t, body, s)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.SourceState != "fresh" || p.Consensus == nil || p.Consensus.SafetyFault != nil {
		t.Fatal("unknown replacement inherited a previous fault")
	}
}

func TestSafetyProfileOverflowFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	s := safetyFixture(now)
	fault := true
	s.Consensus.SafetyFault, s.Version, s.RunID = &fault, "", strings.Repeat("r", 128)
	s.SnapshotManifests = &SnapshotManifests{Attempt: 1, Full: &SnapshotManifestParse{Slot: 42, ParsedAt: now}, ReadyMarker: &SnapshotReadyMarker{Slot: 42, ObservedAt: now}}
	c := &collector{public: true}
	initial, err := c.encode(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	// The v2 manifest profile fits exactly; the additional v3 field cannot.
	s.Version = strings.Repeat("v", MaxSnapshotBytes-len(initial.readyPublicJSON))
	private, err := json.Marshal(s)
	if err != nil || len(private) > MaxPrivateSnapshotBytes || s.validate() != nil {
		t.Fatalf("invalid private fixture: %d bytes (%v)", len(private), err)
	}
	frames, err := c.encode(&s, "fresh", now)
	if err != nil || len(frames.readyPublicJSON) != MaxSnapshotBytes || len(frames.publicJSON) > MaxSnapshotBytes {
		t.Fatalf("v2 response did not fit exactly: %d bytes (%v)", len(frames.readyPublicJSON), err)
	}
	if !strings.Contains(string(frames.publicJSON), `"source_state":"fresh"`) || !strings.Contains(string(frames.readyPublicJSON), `"ready_marker"`) || !strings.Contains(string(frames.safetyPublicJSON), `"source_state":"unavailable"`) || strings.Contains(string(frames.safetyPublicJSON), `"safety_fault"`) {
		t.Fatal("v3 overflow silently presented an incomplete fresh profile")
	}
	var frame struct{ Value json.RawMessage }
	if len(frames.safetyMessages) != 1 || json.Unmarshal(frames.safetyMessages[0], &frame) != nil || string(frame.Value) != string(frames.safetyPublicJSON) {
		t.Fatal("v3 overflow differed between HTTP and WebSocket")
	}
}
