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

func TestAccountFactsCollectorSourceLossAndReset(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := accountFactsFixture(at)
	read := func(wantState string) *PublicAccountFacts {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		if p.SourceState != wantState {
			t.Fatalf("source state = %q, want %q", p.SourceState, wantState)
		}
		if p.Replay == nil {
			return nil
		}
		return p.Replay.AccountFacts
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	if read("fresh") == nil || read("fresh").State != "fresh" {
		t.Fatal("collector lost bank facts")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if read("stale") == nil || read("stale").State != "stale" || !read("stale").SampledAt.Equal(at) {
		t.Fatal("source loss kept facts fresh or redated them")
	}
	for _, reset := range []string{"old producer", "rewind", "attempt", "run", "identity"} {
		s = accountFactsFixture(at)
		s.SampledAt = at.Add(2 * time.Second)
		switch reset {
		case "old producer":
			s.Replay.AccountFacts = nil
		case "rewind":
			s.Replay.Completed = nil
		case "attempt":
			s.Replay.Attempt++
			s.Replay.StartedAt = at.Add(time.Second)
		case "run":
			s.RunID = "new-run"
			s.StartedAt = at.Add(time.Second)
		case "identity":
			key := (solana.PublicKey{8}).String()
			s.IdentityKey = &key
		}
		setFixture(t, body, s)
		c.poll(context.Background(), s.SampledAt)
		if read("fresh") != nil {
			t.Fatalf("%s retained old facts", reset)
		}
		at = at.Add(3 * time.Second)
	}
}

func accountFactsFixture(at time.Time) Snapshot {
	s := publicTestSnapshot(at)
	identity, vote := (solana.PublicKey{1}).String(), (solana.PublicKey{2}).String()
	zero, balance := uint64(0), uint64(math.MaxUint64)
	inflation, block := uint16(0), uint16(math.MaxUint16)
	s.IdentityKey = &identity
	s.Consensus.Voting.Identity, s.Consensus.Voting.VoteAccount = identity, vote
	s.Replay.Attempt, s.Replay.StartedAt = 1, at.Add(-time.Minute)
	s.Replay.Completed = &CompletedBlock{At: at, Slot: 50, Bankhash: (solana.Hash{3}).String(), Epoch: 2}
	s.Replay.AccountFacts = &AccountFacts{SampledAt: at, Slot: 50, Bankhash: s.Replay.Completed.Bankhash, Epoch: 2,
		Identity: identity, IdentityBalanceLamports: &zero, VoteAccount: &vote, VoteBalanceLamports: &balance,
		StoredCommissions: &StoredCommissions{InflationRewardsBPS: &inflation, BlockRevenueBPS: &block}}
	return s
}

func TestPublicAccountFactsPrecisionContextAndAllowlist(t *testing.T) {
	at := time.Now().UTC()
	s := accountFactsFixture(at)
	p, raw := publicValue(t, &s, "fresh", at)
	f := p.Replay.AccountFacts
	if f == nil || f.State != "fresh" || f.Provenance != "successful_local_bank" || f.IdentityBalanceLamports == nil || *f.IdentityBalanceLamports != 0 ||
		f.VoteBalanceLamports == nil || *f.VoteBalanceLamports != math.MaxUint64 || f.Slot != 50 || f.Epoch != 2 || f.Bankhash != s.Replay.Completed.Bankhash {
		t.Fatalf("wrong account observation: %+v", f)
	}
	if !strings.Contains(string(raw), `"identity_balance_lamports":"0"`) || !strings.Contains(string(raw), `"vote_balance_lamports":"18446744073709551615"`) {
		t.Fatal("balance precision or known zero lost")
	}
	var envelope struct {
		Replay struct {
			AccountFacts map[string]json.RawMessage `json:"account_facts"`
		} `json:"replay"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for key := range envelope.Replay.AccountFacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"bankhash", "epoch", "identity", "identity_balance_lamports", "provenance", "sampled_at", "slot", "state", "stored_commissions", "ticket_capacity_no_income", "vote_account", "vote_balance_lamports"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("account allowlist changed: %v", keys)
	}

	// A cached earlier bank is not relabelled with the latest completed bank.
	s.Replay.Completed.Slot++
	s.Replay.Completed.Bankhash = (solana.Hash{4}).String()
	s.Replay.Completed.Epoch++
	f = publicAccountFacts(&s, "fresh", at)
	if f == nil || f.Slot != 50 || f.Epoch != 2 || f.Bankhash != (solana.Hash{3}).String() {
		t.Fatal("cached bank context changed")
	}
	*s.Replay.AccountFacts.IdentityBalanceLamports = 5
	*s.Replay.AccountFacts.VoteBalanceLamports = 6
	*s.Replay.AccountFacts.VoteAccount = (solana.PublicKey{7}).String()
	if *f.IdentityBalanceLamports != 0 || *f.VoteBalanceLamports != math.MaxUint64 || *f.VoteAccount != (solana.PublicKey{2}).String() {
		t.Fatal("public pointers alias producer")
	}

	s = accountFactsFixture(at)
	s.Replay.AccountFacts.Slot, s.Replay.Completed.Slot = math.MaxUint64, math.MaxUint64
	s.Replay.AccountFacts.Epoch, s.Replay.Completed.Epoch = math.MaxUint64, math.MaxUint64
	_, raw = publicValue(t, &s, "fresh", at)
	if !strings.Contains(string(raw), `"slot":"18446744073709551615"`) || !strings.Contains(string(raw), `"epoch":"18446744073709551615"`) {
		t.Fatal("bank context precision lost")
	}
}

func TestPublicAccountFactsAvailabilityAndRole(t *testing.T) {
	at := time.Now().UTC()
	s := accountFactsFixture(at)
	s.Mode, s.Alpenglow, s.Consensus = "verifying", false, nil
	s.Replay.AccountFacts.VoteAccount, s.Replay.AccountFacts.VoteBalanceLamports = nil, nil
	s.Replay.AccountFacts.StoredCommissions = nil
	p, raw := publicValue(t, &s, "fresh", at)
	if p.Replay.AccountFacts == nil || p.Replay.AccountFacts.IdentityBalanceLamports == nil || p.Replay.AccountFacts.VoteAccount != nil || !strings.Contains(string(raw), `"vote_account":null`) {
		t.Fatal("configured full-node identity facts missing or vote key fabricated")
	}
	s = accountFactsFixture(at)
	s.Consensus.Voting = nil // Funding does not require runtime voter admission.
	s.Replay.AccountFacts.IdentityBalanceLamports, s.Replay.AccountFacts.VoteBalanceLamports = nil, nil
	p, raw = publicValue(t, &s, "fresh", at)
	if p.Replay.AccountFacts == nil || p.Replay.AccountFacts.VoteAccount == nil || p.Replay.AccountFacts.IdentityBalanceLamports != nil || p.Replay.AccountFacts.VoteBalanceLamports != nil || !strings.Contains(string(raw), `"vote_balance_lamports":null`) {
		t.Fatal("unavailable balance was invented or configured subject lost")
	}
	for _, conflict := range []string{"identity", "vote"} {
		s = accountFactsFixture(at)
		if conflict == "identity" {
			s.Consensus.Voting.Identity = (solana.PublicKey{8}).String()
		} else {
			s.Consensus.Voting.VoteAccount = (solana.PublicKey{8}).String()
		}
		p, _ = publicValue(t, &s, "fresh", at)
		if p.Replay.AccountFacts == nil || p.Replay.AccountFacts.IdentityBalanceLamports == nil || p.Replay.AccountFacts.VoteAccount != nil || p.Replay.AccountFacts.VoteBalanceLamports != nil || p.Replay.AccountFacts.StoredCommissions != nil {
			t.Fatalf("%s conflict relabelled vote funds or hid identity funds", conflict)
		}
	}
	s = accountFactsFixture(at)
	for _, state := range []string{"fresh", "stale"} {
		p, _ = publicValue(t, &s, state, at.Add(31*time.Second))
		if p.Replay.AccountFacts == nil || p.Replay.AccountFacts.State != "stale" || !p.Replay.AccountFacts.SampledAt.Equal(at) || *p.Replay.AccountFacts.IdentityBalanceLamports != 0 {
			t.Fatal("historical facts redated or lost on staleness")
		}
	}
}

func TestPublicAccountFactsRejectsUnboundEvidence(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"old producer": func(s *Snapshot) { s.Replay.AccountFacts = nil },
		"no identity":  func(s *Snapshot) { s.IdentityKey = nil },
		"zero identity": func(s *Snapshot) {
			key := (solana.PublicKey{}).String()
			s.IdentityKey = &key
			s.Replay.AccountFacts.Identity = key
		},
		"invalid identity": func(s *Snapshot) { key := "bad"; s.IdentityKey = &key; s.Replay.AccountFacts.Identity = key },
		"identity swap":    func(s *Snapshot) { key := (solana.PublicKey{9}).String(); s.IdentityKey = &key },
		"run":              func(s *Snapshot) { s.RunID = "" },
		"attempt":          func(s *Snapshot) { s.Replay.Attempt = 0 },
		"old run":          func(s *Snapshot) { s.StartedAt = at.Add(-time.Second) },
		"old attempt":      func(s *Snapshot) { s.Replay.AccountFacts.SampledAt = s.Replay.StartedAt.Add(-time.Nanosecond) },
		"undated":          func(s *Snapshot) { s.Replay.AccountFacts.SampledAt = time.Time{} },
		"after replay":     func(s *Snapshot) { s.Replay.AccountFacts.SampledAt = at.Add(time.Nanosecond) },
		"after node":       func(s *Snapshot) { s.SampledAt = at.Add(-time.Second - time.Nanosecond) },
		"after collector": func(s *Snapshot) {
			s.SampledAt = at.Add(time.Second + time.Nanosecond)
			s.Replay.SampledAt = s.SampledAt
			s.Replay.AccountFacts.SampledAt = s.SampledAt
		},
		"rewind":                  func(s *Snapshot) { s.Replay.Completed = nil },
		"old completed":           func(s *Snapshot) { s.Replay.Completed.At = s.Replay.StartedAt.Add(-time.Nanosecond) },
		"future completed":        func(s *Snapshot) { s.Replay.Completed.At = at.Add(time.Nanosecond) },
		"future bank":             func(s *Snapshot) { s.Replay.AccountFacts.Slot++ },
		"older bank future epoch": func(s *Snapshot) { s.Replay.AccountFacts.Slot--; s.Replay.AccountFacts.Epoch++ },
		"equal bank hash":         func(s *Snapshot) { s.Replay.AccountFacts.Bankhash = (solana.Hash{8}).String() },
		"equal bank epoch":        func(s *Snapshot) { s.Replay.AccountFacts.Epoch++ },
		"invalid hash":            func(s *Snapshot) { s.Replay.AccountFacts.Slot--; s.Replay.AccountFacts.Bankhash = "bad" },
		"balance without vote":    func(s *Snapshot) { s.Replay.AccountFacts.VoteAccount = nil },
		"invalid vote":            func(s *Snapshot) { key := "bad"; s.Replay.AccountFacts.VoteAccount = &key },
		"zero vote":               func(s *Snapshot) { key := (solana.PublicKey{}).String(); s.Replay.AccountFacts.VoteAccount = &key },
	} {
		t.Run(name, func(t *testing.T) {
			s := accountFactsFixture(at)
			mutate(&s)
			p, _ := publicValue(t, &s, "fresh", at)
			if p.Replay != nil && p.Replay.AccountFacts != nil {
				t.Fatalf("unbound account facts published: %+v", p.Replay.AccountFacts)
			}
		})
	}
	// Exactly the existing one-second producer-clock allowance remains valid.
	s := accountFactsFixture(at)
	s.SampledAt = at.Add(-time.Second)
	if publicAccountFacts(&s, "fresh", at) == nil {
		t.Fatal("exact node clock allowance rejected")
	}
	s = accountFactsFixture(at.Add(time.Second))
	if publicAccountFacts(&s, "fresh", at) == nil {
		t.Fatal("exact collector clock allowance rejected")
	}
}

func TestPublicStoredCommissionsPreserveRawNullableFields(t *testing.T) {
	at := time.Now().UTC()
	zero, raw, max := uint16(0), uint16(25500), uint16(math.MaxUint16)
	for _, tc := range []struct {
		name             string
		inflation, block *uint16
		wire             string
	}{
		{"zero", &zero, &zero, `"stored_commissions":{"inflation_rewards_bps":0,"block_revenue_bps":0}`},
		{"raw maximum", &max, &max, `"stored_commissions":{"inflation_rewards_bps":65535,"block_revenue_bps":65535}`},
		{"legacy", &raw, nil, `"stored_commissions":{"inflation_rewards_bps":25500,"block_revenue_bps":null}`},
		{"unknown", nil, nil, `"stored_commissions":{"inflation_rewards_bps":null,"block_revenue_bps":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := accountFactsFixture(at)
			s.Replay.AccountFacts.StoredCommissions = &StoredCommissions{InflationRewardsBPS: tc.inflation, BlockRevenueBPS: tc.block}
			p, body := publicValue(t, &s, "fresh", at)
			if p.Replay.AccountFacts == nil || p.Replay.AccountFacts.StoredCommissions == nil ||
				!reflect.DeepEqual(p.Replay.AccountFacts.StoredCommissions.InflationRewardsBPS, tc.inflation) ||
				!reflect.DeepEqual(p.Replay.AccountFacts.StoredCommissions.BlockRevenueBPS, tc.block) || !strings.Contains(string(body), tc.wire) {
				t.Fatalf("raw nullable commission changed: %s", body)
			}
			if len(body) > MaxSnapshotBytes {
				t.Fatal("account facts exceed public budget")
			}
		})
	}
	s := accountFactsFixture(at)
	p := publicAccountFacts(&s, "fresh", at)
	*s.Replay.AccountFacts.StoredCommissions.InflationRewardsBPS = 1234
	*s.Replay.AccountFacts.StoredCommissions.BlockRevenueBPS = 5678
	if *p.StoredCommissions.InflationRewardsBPS != 0 || *p.StoredCommissions.BlockRevenueBPS != math.MaxUint16 {
		t.Fatal("public commission pointers alias source")
	}
	body, err := json.Marshal(p.StoredCommissions)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 || string(fields["inflation_rewards_bps"]) != "0" || string(fields["block_revenue_bps"]) != "65535" {
		t.Fatalf("commission allowlist changed: %s", body)
	}

	// A producer predating commission observation retains exactly its old shape.
	s.Replay.AccountFacts.StoredCommissions = nil
	public, body := publicValue(t, &s, "fresh", at)
	if public.Replay.AccountFacts == nil || public.Replay.AccountFacts.StoredCommissions != nil || strings.Contains(string(body), `"stored_commissions"`) {
		t.Fatal("old producer acquired commission fields")
	}
	private, err := json.Marshal(s.Replay.AccountFacts)
	if err != nil || strings.Contains(string(private), `"stored_commissions"`) {
		t.Fatal("private old-producer shape changed")
	}
}

func TestStoredCommissionsRequireVoteAndUint16WireValues(t *testing.T) {
	at := time.Now().UTC()
	s := accountFactsFixture(at)
	s.Replay.AccountFacts.VoteAccount, s.Replay.AccountFacts.VoteBalanceLamports = nil, nil
	if publicAccountFacts(&s, "fresh", at) != nil {
		t.Fatal("commission fields were published without their vote subject")
	}
	for _, field := range []string{"inflation_rewards_bps", "block_revenue_bps"} {
		for _, value := range []string{"-1", "65536", "1.5", `"1"`, "true", "1e100"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				body := []byte(`{"stored_commissions":{"` + field + `":` + value + `}}`)
				var private AccountFacts
				var public PublicAccountFacts
				if json.Unmarshal(body, &private) == nil || json.Unmarshal(body, &public) == nil {
					t.Fatalf("non-uint16 wire value accepted: %s", body)
				}
			})
		}
	}
}
