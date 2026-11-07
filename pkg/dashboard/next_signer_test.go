package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func maximumNextEpochSigner(at time.Time) *NextEpochSigner {
	eligible, rank, stake := true, uint16(math.MaxUint16), uint64(math.MaxUint64)
	total := stake
	return &NextEpochSigner{SampledAt: at, RootSlot: math.MaxUint64, RootEpoch: math.MaxUint64 - 1, Epoch: math.MaxUint64,
		InstalledSet: true, KeyEligible: &eligible, Rank: &rank, StakeLamports: &stake, TotalStakeLamports: &total}
}

func nextEpochSignerFixture(at time.Time) Snapshot {
	s := testSnapshot(at)
	identity, vote := (solana.PublicKey{1}).String(), (solana.PublicKey{2}).String()
	s.Mode, s.Alpenglow, s.IdentityKey = "validator", true, &identity
	s.Consensus = &Consensus{SampledAt: at, VotingEnabled: true,
		Voting: &Voting{SampledAt: at, Identity: identity, VoteAccount: vote, NextEpochSigner: maximumNextEpochSigner(at)}}
	return s
}

func TestPublicNextEpochSignerRawValuesAndAllowlist(t *testing.T) {
	at := time.Now().UTC()
	s := nextEpochSignerFixture(at)
	// Installed source totals are positive raw fields, not a verified sum.
	*s.Consensus.Voting.NextEpochSigner.TotalStakeLamports = 1
	p, raw := advancedPublicValue(t, &s, "fresh", at)
	n := p.Voting.NextEpochSigner
	if n == nil || n.State != "fresh" || n.Provenance != "installed_engine_set" || !n.SampledAt.Equal(at) ||
		n.RootSlot != math.MaxUint64 || n.RootEpoch != math.MaxUint64-1 || n.Epoch != math.MaxUint64 ||
		!n.InstalledSet || n.KeyEligible == nil || !*n.KeyEligible || *n.Rank != math.MaxUint16 || *n.StakeLamports != math.MaxUint64 || *n.TotalStakeLamports != 1 {
		t.Fatalf("wrong raw installed observation: %+v", n)
	}
	if p.Voting.RootSlot != nil || p.Voting.Epoch != nil || p.Voting.KeyEligible != nil {
		t.Fatal("next-only observation fabricated current-epoch knowledge")
	}
	if !strings.Contains(string(raw), `"stake_lamports":"18446744073709551615"`) || !strings.Contains(string(raw), `"epoch":"18446744073709551615"`) {
		t.Fatal("uint64 precision lost")
	}
	var object map[string]json.RawMessage
	b, err := json.Marshal(n)
	if err != nil || json.Unmarshal(b, &object) != nil {
		t.Fatal("cannot inspect next-signer JSON")
	}
	var keys []string
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"epoch", "installed_set", "key_eligible", "provenance", "rank", "root_epoch", "root_slot", "sampled_at", "stake_lamports", "state", "total_stake_lamports"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("next-signer allowlist: %v", keys)
	}
	n = publicNextEpochSigner(&s, "fresh", at)
	*n.KeyEligible, *n.Rank, *n.StakeLamports, *n.TotalStakeLamports = false, 7, 8, 9
	f := s.Consensus.Voting.NextEpochSigner
	if !*f.KeyEligible || *f.Rank != math.MaxUint16 || *f.StakeLamports != math.MaxUint64 || *f.TotalStakeLamports != 1 {
		t.Fatal("public pointers alias private observation")
	}
}

func TestPublicNextEpochSignerUnknownZeroStaleAndCompatibility(t *testing.T) {
	at := time.Now().UTC()
	s := nextEpochSignerFixture(at)
	f := s.Consensus.Voting.NextEpochSigner
	f.RootSlot, f.RootEpoch, f.Epoch, *f.Rank = 0, 0, 1, 0
	n := publicNextEpochSigner(&s, "fresh", at)
	if n == nil || n.RootSlot != 0 || n.RootEpoch != 0 || *n.Rank != 0 {
		t.Fatal("known zero reference/rank lost")
	}
	*f.KeyEligible, f.Rank, f.StakeLamports = false, nil, nil
	n = publicNextEpochSigner(&s, "fresh", at)
	if n == nil || n.KeyEligible == nil || *n.KeyEligible || n.Rank != nil || n.StakeLamports != nil || n.TotalStakeLamports == nil {
		t.Fatal("installed ineligible became unknown or attributed stake")
	}
	f.InstalledSet, f.KeyEligible, f.TotalStakeLamports = false, nil, nil
	for _, state := range []string{"fresh", "stale"} {
		n = publicNextEpochSigner(&s, state, at.Add(31*time.Second))
		if n == nil || n.State != "stale" || !n.SampledAt.Equal(at) || n.InstalledSet || n.KeyEligible != nil || n.TotalStakeLamports != nil || n.Epoch != 1 {
			t.Fatal("unknown installed set or historical source time lost")
		}
	}
	s.Consensus.Voting.NextEpochSigner = nil
	p, raw := advancedPublicValue(t, &s, "fresh", at)
	if p.Voting == nil || strings.Contains(string(raw), "next_epoch_signer") {
		t.Fatal("older producer shape changed")
	}
	s.Mode = "verifying"
	p, _ = advancedPublicValue(t, &s, "fresh", at)
	if p.Voting != nil {
		t.Fatal("full node acquired false signer state")
	}
}

func TestPublicNextEpochSignerRejectsUnboundOrMalformedChild(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"absent":            func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner = nil },
		"not enabled":       func(s *Snapshot) { s.Consensus.VotingEnabled = false },
		"identity mismatch": func(s *Snapshot) { s.Consensus.Voting.Identity = (solana.PublicKey{3}).String() },
		"zero identity": func(s *Snapshot) {
			*s.IdentityKey = (solana.PublicKey{}).String()
			s.Consensus.Voting.Identity = *s.IdentityKey
		},
		"bad vote":         func(s *Snapshot) { s.Consensus.Voting.VoteAccount = "not a public key" },
		"zero vote":        func(s *Snapshot) { s.Consensus.Voting.VoteAccount = (solana.PublicKey{}).String() },
		"no run":           func(s *Snapshot) { s.RunID = "" },
		"pre-run":          func(s *Snapshot) { s.StartedAt = at.Add(time.Nanosecond) },
		"undated":          func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.SampledAt = time.Time{} },
		"different sample": func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.SampledAt = at.Add(-time.Nanosecond) },
		"after consensus":  func(s *Snapshot) { s.Consensus.SampledAt = at.Add(-time.Nanosecond) },
		"collector future": func(s *Snapshot) {
			s.Consensus.Voting.NextEpochSigner.SampledAt = at.Add(2 * time.Second)
			s.Consensus.Voting.SampledAt = at.Add(2 * time.Second)
			s.Consensus.SampledAt = at.Add(2 * time.Second)
		},
		"absolute source cap": func(s *Snapshot) { s.SampledAt = at.Add(-2 * time.Second) },
		"wrapped epoch": func(s *Snapshot) {
			s.Consensus.Voting.NextEpochSigner.RootEpoch = math.MaxUint64
			s.Consensus.Voting.NextEpochSigner.Epoch = 0
		},
		"wrong epoch":           func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.Epoch-- },
		"parent root":           func(s *Snapshot) { s.Consensus.Voting.RootSlot = new(uint64) },
		"parent epoch":          func(s *Snapshot) { s.Consensus.Voting.Epoch = new(uint64) },
		"unknown has fields":    func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.InstalledSet = false },
		"nil eligibility":       func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.KeyEligible = nil },
		"nil total":             func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.TotalStakeLamports = nil },
		"zero total":            func(s *Snapshot) { *s.Consensus.Voting.NextEpochSigner.TotalStakeLamports = 0 },
		"nil rank":              func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.Rank = nil },
		"nil stake":             func(s *Snapshot) { s.Consensus.Voting.NextEpochSigner.StakeLamports = nil },
		"zero stake":            func(s *Snapshot) { *s.Consensus.Voting.NextEpochSigner.StakeLamports = 0 },
		"ineligible attributed": func(s *Snapshot) { *s.Consensus.Voting.NextEpochSigner.KeyEligible = false },
	} {
		t.Run(name, func(t *testing.T) {
			s := nextEpochSignerFixture(at)
			mutate(&s)
			p, _ := advancedPublicValue(t, &s, "fresh", at)
			if p.Voting != nil && p.Voting.NextEpochSigner != nil {
				t.Fatalf("invalid child survived: %+v", p.Voting.NextEpochSigner)
			}
		})
	}
}

func TestNextEpochSignerCollectorSourceLossAndNewRun(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := nextEpochSignerFixture(at)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().advancedPublicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	if p := read(); p.Voting == nil || p.Voting.NextEpochSigner == nil || p.Voting.NextEpochSigner.State != "fresh" {
		t.Fatal("collector lost next-only observation")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if p := read(); p.Voting == nil || p.Voting.NextEpochSigner == nil || p.Voting.NextEpochSigner.State != "stale" || !p.Voting.NextEpochSigner.SampledAt.Equal(at) {
		t.Fatal("source loss redated or kept fresh signer state")
	}
	s.RunID, s.StartedAt, s.SampledAt = "new-run", at.Add(time.Second), at.Add(time.Second)
	s.Consensus.SampledAt, s.Consensus.Voting.SampledAt = s.SampledAt, s.SampledAt
	s.Consensus.Voting.NextEpochSigner = nil
	setFixture(t, body, s)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.Node.RunID != "new-run" || p.Voting == nil || p.Voting.NextEpochSigner != nil {
		t.Fatal("new run inherited older signer observation")
	}
}

func TestNextEpochSignerWireBoundsAndMalformedChildIsolation(t *testing.T) {
	for _, raw := range []string{`{"rank":-1}`, `{"rank":65536}`, `{"rank":1.5}`, `{"stake_lamports":1}`, `{"epoch":"18446744073709551616"}`} {
		var n NextEpochSigner
		if json.Unmarshal([]byte(raw), &n) == nil {
			t.Fatalf("invalid numeric wire bound accepted: %s", raw)
		}
	}
	at := time.Now().UTC()
	s := nextEpochSignerFixture(at)
	before, _ := advancedPublicValue(t, &s, "fresh", at)
	before.Voting.NextEpochSigner = nil
	want, err := json.Marshal(before.Voting)
	if err != nil {
		t.Fatal(err)
	}
	s.Consensus.Voting.NextEpochSigner.Epoch = 0
	after, _ := advancedPublicValue(t, &s, "fresh", at)
	got, err := json.Marshal(after.Voting)
	if err != nil || string(got) != string(want) {
		t.Fatalf("malformed child changed original voting fields: %s, want %s (%v)", got, want, err)
	}
}
