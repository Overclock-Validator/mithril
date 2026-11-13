package dashboard

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"github.com/gagliardetto/solana-go"
)

func nodeDisplayFixture(t *testing.T, now time.Time) (Snapshot, *clusterView) {
	t.Helper()
	directory := clusterTestSnapshot(now, 3)
	if err := cluster.Validate(directory); err != nil {
		t.Fatal(err)
	}
	s := publicTestSnapshot(now)
	s.Alpenglow, s.GenesisHash, s.IdentityKey = true, &directory.GenesisHash, &directory.Validators[0].Identity
	s.Replay.Completed.Epoch = 239
	return s, projectCluster(&directory, "display-fixture", "fresh", "", now)
}

func TestNodeDisplaySelection(t *testing.T) {
	now := time.Now().UTC()
	for _, role := range []string{"validator", "verifying"} {
		t.Run(role, func(t *testing.T) {
			s, view := nodeDisplayFixture(t, now)
			s.Mode = role
			if role == "verifying" {
				s.Consensus = nil // No vote account is required for an identity label.
			}
			v := selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "fresh" || v.Name == nil || *v.Name != "Node 000" || *v.DirectorySource != "solstat" ||
				v.IconURL != nil || v.IconState != "not_supplied" || !v.SourceAt.Equal(now) || !v.CapturedAt.Equal(now) {
				t.Fatalf("wrong attributed selection: %+v", v)
			}
			// Repeated linked samples never change the provider's original dates.
			s.SampledAt = now.Add(time.Second)
			later := selectNodeDisplay(&s, "fresh", view, s.SampledAt)
			if !later.SourceAt.Equal(*v.SourceAt) || !later.CapturedAt.Equal(*v.CapturedAt) {
				t.Fatal("linked heartbeat redated external metadata")
			}
			view.rows[1].Identity, view.rows[1].Name = *s.IdentityKey, ""
			view.rows[2].Identity, view.rows[2].Name, view.rows[2].DirectorySource = *s.IdentityKey, "Node 000", "titan"
			v = selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "fresh" || *v.DirectorySource != "solstat" {
				t.Fatal("empty or agreeing multi-vote claim displaced preferred provenance")
			}
			view.rows[2].Name = "Other name"
			v = selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "conflict" || v.Reason != "name_conflict" || v.Name != nil || v.DirectorySource != nil || v.SourceAt != nil {
				t.Fatal("conflicting eligible names were arbitrarily selected")
			}
			view.metadata.Providers[1].State = "stale"
			v = selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "fresh" || *v.Name != "Other name" || *v.DirectorySource != "titan" {
				t.Fatal("expired preferred provider prevented eligible fallback")
			}
		})
	}
}

func TestNodeDisplayBindingAndUnknown(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name, reason string
		mutate       func(*Snapshot, *clusterView)
	}{
		{"identity", "name_unavailable", func(s *Snapshot, _ *clusterView) { key := cluster.GenesisHash; s.IdentityKey = &key }},
		{"invalid identity", "invalid_context", func(s *Snapshot, _ *clusterView) { key := "not a public key"; s.IdentityKey = &key }},
		{"unknown identity", "unknown_binding", func(s *Snapshot, _ *clusterView) { s.IdentityKey = nil }},
		{"genesis", "unsupported_profile", func(s *Snapshot, _ *clusterView) { s.GenesisHash = s.IdentityKey }},
		{"classic", "unsupported_profile", func(s *Snapshot, _ *clusterView) { s.Alpenglow = false }},
		{"old status", "status_stale", func(s *Snapshot, _ *clusterView) { s.SampledAt = now.Add(-StaleAfter - 1) }},
		{"unknown epoch", "unknown_epoch", func(s *Snapshot, _ *clusterView) { s.Replay.Completed = nil }},
		{"old run epoch", "unknown_epoch", func(s *Snapshot, _ *clusterView) { s.Replay.Completed.At = s.StartedAt.Add(-1) }},
		{"epoch", "directory_unavailable", func(s *Snapshot, _ *clusterView) { s.Replay.Completed.Epoch++ }},
		{"view profile", "directory_unavailable", func(_ *Snapshot, v *clusterView) { v.metadata.Profile = "other" }},
		{"view genesis", "directory_unavailable", func(_ *Snapshot, v *clusterView) { v.metadata.GenesisHash = nil }},
		{"expired directory", "directory_unavailable", func(_ *Snapshot, v *clusterView) { at := now.Add(-cluster.MaxAge - 1); v.metadata.CapturedAt = &at }},
		{"future directory", "directory_unavailable", func(_ *Snapshot, v *clusterView) { at := now.Add(5*time.Second + 1); v.metadata.CapturedAt = &at }},
		{"stale view", "directory_unavailable", func(_ *Snapshot, v *clusterView) { v.metadata.State = "stale" }},
		{"provider epoch", "name_unavailable", func(_ *Snapshot, v *clusterView) {
			epoch := cluster.Count("240")
			v.metadata.Providers[1].Epoch = &epoch
		}},
		{"provider genesis", "name_unavailable", func(_ *Snapshot, v *clusterView) { v.metadata.Providers[1].GenesisHash = "" }},
		{"expired provider", "name_unavailable", func(_ *Snapshot, v *clusterView) {
			at := now.Add(-cluster.MaxAge - 1)
			v.metadata.Providers[1].SourceAt = &at
		}},
		{"missing provider date", "name_unavailable", func(_ *Snapshot, v *clusterView) { v.metadata.Providers[1].SourceAt = nil }},
		{"empty name", "name_unavailable", func(_ *Snapshot, v *clusterView) { v.rows[0].Name = "" }},
		{"control name", "name_unavailable", func(_ *Snapshot, v *clusterView) { v.rows[0].Name = "\x00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, view := nodeDisplayFixture(t, now)
			tc.mutate(&s, view)
			v := selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "unavailable" || v.Reason != tc.reason || v.Name != nil || v.IconURL != nil {
				t.Fatalf("got state=%s reason=%s name=%v, want unavailable/%s/null", v.State, v.Reason, v.Name, tc.reason)
			}
		})
	}
	s, view := nodeDisplayFixture(t, now)
	for _, state := range []string{"stale", "unavailable"} {
		if v := selectNodeDisplay(&s, state, view, now); v.Name != nil || v.Reason != "status_stale" {
			t.Fatal("stale linked node kept an external label")
		}
	}
	if v := selectNodeDisplay(nil, "unavailable", view, now); v.Name != nil || v.RunID != "" {
		t.Fatal("missing node inherited directory identity")
	}
	// A different run gets its own binding, not the previous response's header.
	s.RunID = "replacement"
	if v := selectNodeDisplay(&s, "fresh", view, now); v.RunID != "replacement" || v.State != "fresh" {
		t.Fatal("replacement run binding was lost")
	}
}

func TestNodeDisplayMainnetProfile(t *testing.T) {
	now := time.Now().UTC()
	s, view := nodeDisplayFixture(t, now)
	if v := selectNodeDisplay(nil, "unavailable", nil, now); v.Profile != "unknown" {
		t.Fatalf("an unavailable node must not claim a network: %+v", v)
	}
	genesis := cluster.MainnetGenesisHash
	s.GenesisHash = &genesis
	view.metadata.Profile, view.metadata.Network, view.metadata.GenesisHash = "mainnet", "mainnet-beta", &genesis
	for i := range view.rows {
		view.rows[i].DirectorySource, view.rows[i].Name = "rpc", ""
	}
	v := selectNodeDisplay(&s, "fresh", view, now)
	if v.Profile != "mainnet" || v.Reason != "name_unavailable" || v.Name != nil {
		t.Fatalf("mainnet display must identify the network without inventing a name: %+v", v)
	}
	if v := selectNodeDisplay(&s, "stale", view, now); v.Profile != "mainnet" || v.Reason != "status_stale" || v.Name != nil {
		t.Fatalf("stale mainnet display has incorrect profile or state: %+v", v)
	}
	otherGenesis := cluster.GenesisHash
	view.metadata.GenesisHash = &otherGenesis
	if v := selectNodeDisplay(&s, "fresh", view, now); v.Profile != "mainnet" || v.Reason != "directory_unavailable" || v.Name != nil {
		t.Fatalf("mismatched directory accepted: %+v", v)
	}
}

func TestNodeDisplayMaximumShape(t *testing.T) {
	now := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("fixture", 23*3600+59*60))
	s, view := nodeDisplayFixture(t, now)
	wideIdentity := (solana.PublicKey{255}).String()
	s.IdentityKey, view.rows[0].Identity = &wideIdentity, wideIdentity
	if len(wideIdentity) != 44 {
		t.Fatal("maximum public-key fixture is not44 bytes")
	}
	s.Replay.Completed.Epoch = math.MaxUint64
	epoch := cluster.Count(strconv.FormatUint(math.MaxUint64, 10))
	view.metadata.Epoch = &epoch
	for i := range view.metadata.Providers {
		view.metadata.Providers[i].Epoch = &epoch
	}
	for _, run := range []string{strings.Repeat("\x00", 128), strings.Repeat("\xff", 128), strings.Repeat("\u2028", 42), strings.Repeat("\\\"", 64)} {
		for _, name := range []string{strings.Repeat("<", 128), strings.Repeat("&", 128), strings.Repeat("\"\\", 64), strings.Repeat("é", 64), strings.Repeat("\u2028\u2029", 21)} {
			s.RunID, view.rows[0].Name = run, name
			if err := s.validate(); err != nil {
				t.Fatalf("maximum legal snapshot invalid: %v", err)
			}
			v := selectNodeDisplay(&s, "fresh", view, now)
			if v.State != "fresh" {
				t.Fatalf("maximum legal fixture lost name: %+v", v)
			}
			if name == strings.Repeat("<", 128) && (run == strings.Repeat("\x00", 128) || run == strings.Repeat("\xff", 128)) {
				published, err := encodeNodeDisplay(v)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("actual Go UTC escaped node-display fixture: %s", published.body)
			}
			// Include widest legal offset dates without changing the source facts.
			v.SourceAt, v.CapturedAt = &now, &now
			frame, err := encodeNodeDisplay(v)
			if err != nil || len(frame.body) > nodeDisplayBytes || len(frame.body) == 0 {
				t.Fatalf("maximum response bytes=%d err=%v", len(frame.body), err)
			}
			var parsed nodeDisplay
			var fields map[string]json.RawMessage
			if json.Unmarshal([]byte(frame.body), &parsed) != nil || json.Unmarshal([]byte(frame.body), &fields) != nil || len(fields) != 15 ||
				parsed.Name == nil || *parsed.Name != name || parsed.IconURL != nil || parsed.RunID != string([]rune(run)) {
				t.Fatal("default JSON encoding changed legal display fields")
			}
			for _, state := range []string{"unavailable", "conflict"} {
				v.State, v.Reason, v.Name, v.DirectorySource, v.SourceAt, v.CapturedAt = state, "directory_unavailable", nil, nil, nil, nil
				if _, err := encodeNodeDisplay(v); err != nil {
					t.Fatal("null state exceeded response bound", err)
				}
			}
			t.Logf("run_bytes=%d name_bytes=%d fresh_encoded_bytes=%d", len(run), len(name), len(frame.body))
		}
	}
	tooLong := strings.Repeat("x", nodeDisplayBytes)
	v := selectNodeDisplay(&s, "fresh", view, now)
	v.Name = &tooLong
	if _, err := encodeNodeDisplay(v); err == nil {
		t.Fatal("oversized prepared input did not fail closed")
	}
	invalid := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	v.Name, v.SourceAt = nil, &invalid
	if _, err := encodeNodeDisplay(v); err == nil {
		t.Fatal("unencodable date did not fail closed")
	}
}

func TestNodeDisplayHeldGenerations(t *testing.T) {
	raw := time.Now()
	s, view := nodeDisplayFixture(t, raw.UTC())
	view.rows[0].Name = strings.Repeat("<", 128)
	var owner nodeDisplayOwner
	held := make(chan nodeDisplayFrame, 32)
	release := make(chan struct{})
	var viewers sync.WaitGroup
	defer func() { close(release); viewers.Wait() }()
	for i := range 32 {
		at := raw.Add(time.Duration(i) * time.Nanosecond)
		s.RunID = fmt.Sprintf("%03d", i) + strings.Repeat("\x00", 125)
		s.SampledAt = at.UTC()
		publishNodeDisplayTest(t, &owner, &s, view, at.UTC(), at)
		ready := make(chan struct{})
		viewers.Go(func() {
			copy := owner.selectFrame(at)
			held <- copy
			close(ready)
			<-release
			if len(copy.body) == 0 || len(copy.body) > nodeDisplayBytes {
				t.Error("held generation is empty or exceeds cap")
			}
			body := owner.admitFrame(copy, raw.Add(31*time.Nanosecond))
			if i < 31 && body != nodeDisplayUnavailableBody || i == 31 && body != copy.body {
				t.Error("selected old/current generation crossed the wrong revision fence")
			}
		})
		<-ready
	}
	seen := make(map[string]bool)
	for i := range 32 {
		frame := <-held
		var v nodeDisplay
		if json.Unmarshal([]byte(frame.body), &v) != nil || seen[v.RunID] || v.Name == nil || *v.Name != view.rows[0].Name {
			t.Fatal("held immutable response was changed by later publications")
		}
		if frame.revision != uint64(i+1) || !frame.deadline.Equal(raw.Add(time.Duration(i)*time.Nanosecond+StaleAfter)) || frame.deadline == frame.deadline.Round(0) {
			t.Fatal("held frame lost its original monotonic deadline or revision")
		}
		seen[v.RunID] = true
	}
	// Each viewer holds only the encoded frame. The owner, preparation scratch
	// and bounded context backing strings are charged by the holder-shape test.
	if unsafe.Sizeof(nodeDisplayFrame{}) > 512 {
		t.Fatal("display frame header exceeds allowance")
	}
	t.Logf("held=32 header_bytes=%d frame_holder_charge=%d; complete owner model is TestNodeDisplayOwnerHolderShape, routed/native RSS proof remains separate", unsafe.Sizeof(nodeDisplayFrame{}), 34*(nodeDisplayBytes+512))
}
