package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"golang.org/x/time/rate"
)

func publishNodeDisplayTest(t *testing.T, o *nodeDisplayOwner, s *Snapshot, view *clusterView, wall, raw time.Time) nodeDisplayFrame {
	t.Helper()
	revision := o.observeStatus(s, "fresh", wall, raw)
	frame, err := o.prepare(s, view, wall, raw, revision)
	if err != nil || !o.publish(frame, raw) {
		t.Fatalf("display publication failed: %v", err)
	}
	return o.selectFrame(raw)
}

func nodeDisplayTestValue(t *testing.T, body string) nodeDisplay {
	t.Helper()
	var value nodeDisplay
	if err := json.Unmarshal([]byte(body), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestNodeDisplayOwnerMainnetDirectory(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	genesis := cluster.MainnetGenesisHash
	s.GenesisHash = &genesis
	view.metadata.Profile, view.metadata.Network, view.metadata.GenesisHash = "mainnet", "mainnet-beta", &genesis
	for i := range view.rows {
		view.rows[i].DirectorySource, view.rows[i].Name = "rpc", ""
	}
	var owner nodeDisplayOwner
	frame := publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	value := nodeDisplayTestValue(t, frame.body)
	if value.Profile != "mainnet" || value.Reason != "name_unavailable" || value.Name != nil {
		t.Fatalf("matching mainnet directory was not accepted: %+v", value)
	}
	other := cluster.GenesisHash
	view.metadata.GenesisHash = &other
	frame = publishNodeDisplayTest(t, &owner, &s, view, wall, raw.Add(time.Nanosecond))
	value = nodeDisplayTestValue(t, frame.body)
	if value.Reason != "directory_unavailable" || value.Name != nil {
		t.Fatalf("mismatched directory was accepted: %+v", value)
	}
}

func TestNodeDisplayOwnerOriginalProviderLease(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	s.SampledAt = wall.Add(-time.Second)
	source := wall.Add(-cluster.MaxAge + time.Second)
	view.metadata.Providers[1].SourceAt = &source
	var owner nodeDisplayOwner
	first := publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	if !first.deadline.Equal(raw.Add(time.Second)) || first.deadline == first.deadline.Round(0) || owner.providers[1].sourceAt.IsZero() {
		t.Fatal("original provider age/raw clock or unselected provider observation was lost")
	}
	// Wall time moves backwards while the raw clock advances. A newer wrapper
	// and changed name cannot renew this same provider/source-date observation.
	later, reversed := raw.Add(500*time.Millisecond), wall.Add(-500*time.Millisecond)
	s.SampledAt = reversed
	view.rows[0].Name = "Changed label"
	view.metadata.Generation = "changed-body"
	view.metadata.CapturedAt = &reversed
	view.metadata.Providers[1].CapturedAt = reversed
	next := publishNodeDisplayTest(t, &owner, &s, view, reversed, later)
	value := nodeDisplayTestValue(t, owner.admitFrame(next, later))
	if value.Name == nil || *value.Name != "Changed label" || !value.SourceAt.Equal(source) || !next.deadline.Equal(first.deadline) {
		t.Fatal("same source date renewed or changed original provenance")
	}
	if owner.admitFrame(next, raw.Add(time.Second)) != nodeDisplayUnavailableBody || owner.selectFrame(raw.Add(time.Second)).body != "" {
		t.Fatal("provider expiry required another collection poll")
	}
}

func TestNodeDisplayOwnerProviderFallback(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	view.rows[1].Identity, view.rows[1].Name, view.rows[1].DirectorySource = *s.IdentityKey, view.rows[0].Name, "titan"
	var owner nodeDisplayOwner
	publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	original := owner.providers
	for i, state := range []string{"stale", "fresh"} {
		at := wall.Add(time.Duration(i+1) * time.Second)
		s.SampledAt = at
		view.metadata.Providers[1].State = state
		frame := publishNodeDisplayTest(t, &owner, &s, view, at, raw.Add(time.Duration(i+1)*time.Second))
		value := nodeDisplayTestValue(t, frame.body)
		want := "titan"
		if state == "fresh" {
			want = "solstat"
		}
		if value.DirectorySource == nil || *value.DirectorySource != want || owner.providers != original {
			t.Fatal("provider fallback renewed a source or selected the wrong provenance")
		}
	}
	// Both wall dates look recent again, but both original raw leases expired.
	s.SampledAt = wall.Add(10 * time.Second)
	frame := publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(cluster.MaxAge+time.Second))
	if value := nodeDisplayTestValue(t, frame.body); value.Name != nil || value.Reason != "name_unavailable" {
		t.Fatal("fallback/clock reversal revived an expired provider")
	}
	newSource := s.SampledAt
	view.metadata.Providers[1].SourceAt = &newSource
	frame = publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(cluster.MaxAge+2*time.Second))
	if value := nodeDisplayTestValue(t, frame.body); value.Name == nil || *value.DirectorySource != "solstat" {
		t.Fatal("genuinely newer source observation did not recover")
	}
}

func TestNodeDisplayOwnerStatusLease(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	s.SampledAt = wall.Add(-time.Second)
	var owner nodeDisplayOwner
	first := publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	deadline, providers := first.deadline, owner.providers
	frame := publishNodeDisplayTest(t, &owner, &s, view, wall.Add(-time.Second), raw.Add(time.Second))
	if !frame.deadline.Equal(deadline) {
		t.Fatal("same accepted status sample renewed during wall rollback")
	}
	owner.observeStatus(&s, "stale", wall, raw.Add(2*time.Second))
	if owner.admitFrame(frame, raw.Add(2*time.Second)) != nodeDisplayUnavailableBody || owner.providers != providers {
		t.Fatal("status failure retained label or erased provider high-waters")
	}
	frame = publishNodeDisplayTest(t, &owner, &s, view, wall, raw.Add(3*time.Second))
	if !frame.deadline.Equal(deadline) || owner.admitFrame(frame, deadline) != nodeDisplayUnavailableBody {
		t.Fatal("status recovery renewed the old sample or ignored its expiry")
	}
	s.RunID = "replacement"
	revision := owner.observeStatus(&s, "fresh", wall, raw.Add(3*time.Second))
	if _, err := owner.prepare(&s, view, wall, raw.Add(3*time.Second), revision); err == nil {
		t.Fatal("new context borrowed a nonnewer status sample's lease")
	}
	s.SampledAt = wall.Add(time.Second)
	frame = publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(3500*time.Millisecond))
	if value := nodeDisplayTestValue(t, frame.body); value.RunID != "replacement" || value.Name == nil {
		t.Fatal("newer replacement status did not recover its own binding")
	}
}

func TestNodeDisplayOwnerRevisionAndOwnership(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	var owner nodeDisplayOwner
	first := publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	revision := owner.observeStatus(&s, "fresh", wall, raw)
	pending, err := owner.prepare(&s, view, wall, raw, revision)
	if err != nil {
		t.Fatal(err)
	}
	originalRun := s.RunID
	for i, run := range []string{"other-run", originalRun} {
		s.RunID, s.SampledAt = run, wall.Add(time.Duration(i+1)*time.Second)
		publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(time.Duration(i+1)*time.Second))
	}
	if owner.publish(pending, raw.Add(2*time.Second)) || owner.admitFrame(first, raw.Add(2*time.Second)) != nodeDisplayUnavailableBody {
		t.Fatal("context A to B to A admitted old preparation or selected reply")
	}
	current := owner.selectFrame(raw.Add(2 * time.Second))
	kept := current
	current.body, current.deadline, current.revision = "changed", raw.Add(time.Hour), 0
	s.RunID, *s.IdentityKey = "caller-header-changed", "invalid"
	view.rows[0].Name = "caller-name-changed"
	*view.metadata.Providers[1].SourceAt = wall.Add(time.Hour)
	if owner.admitFrame(kept, raw.Add(2*time.Second)) != kept.body || owner.admitFrame(current, raw.Add(2*time.Second)) != nodeDisplayUnavailableBody {
		t.Fatal("caller headers mutated publication or forged its admission")
	}
}

func TestNodeDisplayOwnerInvalidAndExpired(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	for _, tc := range []struct {
		name   string
		mutate func(*Snapshot, *clusterView)
	}{
		{"future_status", func(s *Snapshot, _ *clusterView) { s.SampledAt = wall.Add(time.Second + 1) }},
		{"stale_status", func(s *Snapshot, _ *clusterView) { s.SampledAt = wall.Add(-StaleAfter - 1) }},
		{"unknown_epoch", func(s *Snapshot, _ *clusterView) { s.Replay.Completed = nil }},
		{"future_provider", func(_ *Snapshot, v *clusterView) {
			at := wall.Add(5*time.Second + 1)
			v.metadata.Providers[1].SourceAt = &at
		}},
		{"stale_directory", func(_ *Snapshot, v *clusterView) { v.metadata.State = "stale" }},
		{"wrong_provider_epoch", func(_ *Snapshot, v *clusterView) {
			epoch := cluster.Count("240")
			v.metadata.Providers[1].Epoch = &epoch
		}},
		{"wrong_profile", func(_ *Snapshot, v *clusterView) { v.metadata.Profile = "other" }},
		{"wrong_genesis", func(_ *Snapshot, v *clusterView) { v.metadata.GenesisHash = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, view := nodeDisplayFixture(t, wall)
			tc.mutate(&s, view)
			var owner nodeDisplayOwner
			revision := owner.observeStatus(&s, "fresh", wall, raw)
			frame, err := owner.prepare(&s, view, wall, raw, revision)
			if err == nil && owner.publish(frame, raw) {
				if value := nodeDisplayTestValue(t, owner.admitFrame(owner.selectFrame(raw), raw)); value.Name != nil {
					t.Fatal("invalid/stale source retained a display name")
				}
			}
		})
	}
	s, view := nodeDisplayFixture(t, wall)
	source := wall.Add(-cluster.MaxAge + time.Millisecond)
	view.metadata.Providers[1].SourceAt = &source
	var owner nodeDisplayOwner
	revision := owner.observeStatus(&s, "fresh", wall, raw)
	frame, err := owner.prepare(&s, view, wall, raw, revision)
	if err != nil || owner.publish(frame, raw.Add(2*time.Millisecond)) {
		t.Fatal("encoding/publication delay did not consume the original lease", err)
	}
}

func TestNodeDisplayOwnerClockAndExhaustion(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	var absent *nodeDisplayOwner
	if absent.observeStatus(&s, "fresh", wall, raw) != 0 || absent.admitFrame(absent.selectFrame(raw), raw) != nodeDisplayUnavailableBody {
		t.Fatal("nil owner was not inert")
	}
	encoded, err := encodeNodeDisplay(selectNodeDisplay(nil, "unavailable", nil, time.Time{}))
	if err != nil || encoded.body != nodeDisplayUnavailableBody {
		t.Fatal("fixed expiry response differs from the approved null shape")
	}
	var owner nodeDisplayOwner
	revision := owner.observeStatus(&s, "fresh", wall, raw.Round(0))
	if _, err := owner.prepare(&s, view, wall, raw, revision); err == nil {
		t.Fatal("UTC-only receipt clock fabricated a monotonic lease")
	}
	frame := publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	if owner.admitFrame(frame, raw.Round(0)) != nodeDisplayUnavailableBody || owner.publish(frame, raw.Add(-1)) {
		t.Fatal("missing/backwards raw clock was accepted")
	}
	owner.revision = math.MaxUint64 - 2
	frame = publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	if frame.revision != math.MaxUint64-1 {
		t.Fatal("last usable revision was lost")
	}
	for range 2 {
		if revision := owner.observeStatus(&s, "fresh", wall, raw); revision != math.MaxUint64 || owner.publish(frame, raw) || owner.admitFrame(frame, raw) != nodeDisplayUnavailableBody {
			t.Fatal("revision exhaustion wrapped or retained a usable frame")
		}
	}
}

func TestNodeDisplayOwnerEpochHighwater(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	s, view := nodeDisplayFixture(t, wall)
	var owner nodeDisplayOwner
	publishNodeDisplayTest(t, &owner, &s, view, wall, raw)
	original := owner.providers
	epoch := cluster.Count("240")
	s.Replay.Completed.Epoch++
	view.metadata.Epoch = &epoch
	for i := range view.metadata.Providers {
		view.metadata.Providers[i].Epoch = &epoch
	}
	s.SampledAt = wall.Add(time.Second)
	frame := publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(time.Second))
	if value := nodeDisplayTestValue(t, frame.body); value.Name != nil || owner.providers != original {
		t.Fatal("same source date under a different epoch minted a lease")
	}
	older := wall.Add(-time.Second)
	view.metadata.Providers[1].SourceAt = &older
	s.SampledAt = wall.Add(2 * time.Second)
	frame = publishNodeDisplayTest(t, &owner, &s, view, s.SampledAt, raw.Add(2*time.Second))
	if value := nodeDisplayTestValue(t, frame.body); value.Name != nil || owner.providers != original {
		t.Fatal("older source date lowered the provider high-water")
	}
}

func TestNodeDisplayOwnerHolderShape(t *testing.T) {
	frame := reflect.TypeFor[nodeDisplayFrame]()
	if frame.NumField() != 4 || frame.Field(0).Type != reflect.TypeFor[string]() || frame.Field(1).Type != reflect.TypeFor[time.Time]() || frame.Field(2).Type != reflect.TypeFor[uint64]() || frame.Field(3).Type != reflect.TypeFor[*statusFrameLease]() {
		t.Fatal("display frame retained more than body/deadline/revision and a status lease token")
	}
	// Current/prepared owner headers and cloned bounded context strings are
	// charged in addition to all 32 pinned responses. Existing directory storage
	// is shared input, never an additional retained display-owned copy.
	ownerBytes := unsafe.Sizeof(nodeDisplayOwner{}) + 2*(128+44+44)
	// The routed owner adds one limiter and a bounded closure capturing the
	// owner, admission function and limiter. Per-response headers stay in512B.
	ownerBytes += unsafe.Sizeof(rate.Limiter{}) + 64
	scratch := 2*nodeDisplayBytes + unsafe.Sizeof(nodeDisplay{}) + unsafe.Sizeof(clusterView{}) + unsafe.Sizeof([5]cluster.ProviderStatus{}) + unsafe.Sizeof([2]time.Time{})
	// Charge a conservative channel and allocation allowance for each token.
	leaseBytes := unsafe.Sizeof(statusFrameLease{}) + unsafe.Sizeof(statusFrameLeaseValue{}) + 256
	modeled := 34*(nodeDisplayBytes+512+leaseBytes) + ownerBytes + scratch + uintptr(len(nodeDisplayUnavailableBody))
	if unsafe.Sizeof(nodeDisplayFrame{}) > 512 || modeled > 192<<10 {
		t.Fatalf("frame=%d owner=%d scratch=%d modeled=%d limit=%d", unsafe.Sizeof(nodeDisplayFrame{}), ownerBytes, scratch, modeled, 192<<10)
	}
	if strings.Contains(nodeDisplayUnavailableBody, "http") {
		t.Fatal("fixed fallback contains an external resource")
	}
	t.Logf("frame=%d owner=%d scratch=%d held32_plus_current_prepared=%d modeled=%d limit=%d; no RSS or routed proof", unsafe.Sizeof(nodeDisplayFrame{}), ownerBytes, scratch, 34*(nodeDisplayBytes+512+leaseBytes), modeled, 192<<10)
}
