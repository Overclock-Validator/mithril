package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func TestPublicAccountNestedTypesAndJSONShape(t *testing.T) {
	tests := []struct {
		private any
		public  any
		want    string
	}{
		{StoredCommissions{}, PublicStoredCommissions{}, `{"inflation_rewards_bps":null,"block_revenue_bps":null}`},
		{StoredVoteHeader{}, PublicStoredVoteHeader{}, `{"node_identity":null,"pending_delegator_rewards_lamports":null,"bls_registration_present":null}`},
		{FundingRules{}, PublicFundingRules{}, `{"reference_slot":"0","vat_filtering_active":false,"vat_charge_gate_active":false,"rent_minimum_lamports":"0","burn_lamports":"0","floor_lamports":"0","meets_floor":null,"shortfall_lamports":null,"surplus_lamports":null}`},
		{EpochStakeSet{}, PublicEpochStakeSet{}, `{"epoch":"0","generation":"0","material_known":false,"member":null,"selected_stake_lamports":null,"total_stake_lamports":null,"metadata_node_identity":null,"node_matches_identity":null,"selection_balance_lamports":null}`},
		{EpochPayerReference{}, PublicEpochPayerReference{}, `{"reference_slot":"0","epoch":"0","member":null}`},
		{EpochStakeSets{}, PublicEpochStakeSets{}, `{"provenance":"","current":{"epoch":"0","generation":"0","material_known":false,"member":null,"selected_stake_lamports":null,"total_stake_lamports":null,"metadata_node_identity":null,"node_matches_identity":null,"selection_balance_lamports":null},"next":null,"payer_reference":null}`},
	}
	for _, tc := range tests {
		if reflect.TypeOf(tc.private) == reflect.TypeOf(tc.public) {
			t.Fatalf("private and public types must differ: %T", tc.private)
		}
		body, err := json.Marshal(tc.public)
		if err != nil || string(body) != tc.want {
			t.Errorf("%T JSON = %s, %v; want %s", tc.public, body, err, tc.want)
		}
	}
	for name, want := range map[string]reflect.Type{
		"StoredCommissions": reflect.TypeOf((*PublicStoredCommissions)(nil)),
		"StoredVoteHeader":  reflect.TypeOf((*PublicStoredVoteHeader)(nil)),
		"FundingRules":      reflect.TypeOf((*PublicFundingRules)(nil)),
		"EpochStakeSets":    reflect.TypeOf((*PublicEpochStakeSets)(nil)),
	} {
		field, ok := reflect.TypeOf(PublicAccountFacts{}).FieldByName(name)
		if !ok || field.Type != want {
			t.Errorf("PublicAccountFacts.%s type = %v, want %v", name, field.Type, want)
		}
	}
	for name, want := range map[string]reflect.Type{
		"Current": reflect.TypeOf(PublicEpochStakeSet{}),
		"Next":    reflect.TypeOf((*PublicEpochStakeSet)(nil)),
		"Payer":   reflect.TypeOf((*PublicEpochPayerReference)(nil)),
	} {
		field, ok := reflect.TypeOf(PublicEpochStakeSets{}).FieldByName(name)
		if !ok || field.Type != want {
			t.Errorf("PublicEpochStakeSets.%s type = %v, want %v", name, field.Type, want)
		}
	}
}

func TestPublicAccountNestedValuesPreserveWireShape(t *testing.T) {
	s := admissionFactsFixture(time.Now().UTC())
	f := s.Replay.AccountFacts
	p := publicAccountFacts(&s, "fresh", f.SampledAt)
	if p == nil || p.StoredCommissions == nil || p.StoredVoteHeader == nil || p.FundingRules == nil || p.EpochStakeSets == nil {
		t.Fatal("valid account facts were not published")
	}
	for _, pair := range [][2]any{
		{f.StoredCommissions, p.StoredCommissions}, {f.StoredVoteHeader, p.StoredVoteHeader},
		{f.FundingRules, p.FundingRules}, {f.EpochStakeSets, p.EpochStakeSets},
	} {
		want, err := json.Marshal(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(pair[1])
		if err != nil || string(got) != string(want) {
			t.Errorf("%T JSON = %s, %v; want %s", pair[1], got, err, want)
		}
	}
	unknown := EpochStakeSet{Epoch: 2, Generation: 9}
	projected, ok := publicEpochStakeSet(unknown, f.Identity)
	if !ok {
		t.Fatal("unknown stake material should remain representable")
	}
	want, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(projected)
	if err != nil || string(got) != string(want) {
		t.Errorf("unknown stake JSON = %s, %v; want %s", got, err, want)
	}
}

func TestAdmissionFactsCollectorLossAndReset(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := admissionFactsFixture(at)
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	for _, lost := range []bool{false, true} {
		if lost {
			body.Store(nil)
			c.poll(context.Background(), at.Add(time.Second))
		}
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		f := p.Replay.AccountFacts
		if f == nil || f.StoredVoteHeader == nil || f.FundingRules == nil || f.EpochStakeSets == nil || !f.SampledAt.Equal(at) ||
			f.TicketCapacityNoIncome == nil || f.TicketCapacityNoIncome.Status != "known" || f.TicketCapacityNoIncome.Tickets == nil || *f.TicketCapacityNoIncome.Tickets != 0 ||
			(lost && f.State != "stale") || (!lost && f.State != "fresh") {
			t.Fatal("actual collector lost or redated optional facts")
		}
	}
	s.SampledAt, s.Replay.SampledAt = at.Add(2*time.Second), at.Add(2*time.Second)
	s.Replay.Attempt++
	s.Replay.StartedAt = at.Add(time.Second)
	setFixture(t, body, s)
	c.poll(context.Background(), s.SampledAt)
	var p PublicSnapshot
	if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
		t.Fatal(err)
	}
	if p.Replay.AccountFacts != nil {
		t.Fatal("new replay attempt kept prior admission facts")
	}
}

func admissionFactsFixture(at time.Time) Snapshot {
	s := accountFactsFixture(at)
	f := s.Replay.AccountFacts
	max, zero, yes := uint64(math.MaxUint64), uint64(0), true
	node := f.Identity
	f.StoredVoteHeader = &StoredVoteHeader{NodeIdentity: &node, PendingDelegatorRewards: &max, BLSRegistrationPresent: &yes}
	f.FundingRules = &FundingRules{ReferenceSlot: f.Slot, VATFilteringActive: true, VATChargeGateActive: true,
		RentMinimumLamports: max, BurnLamports: max, FloorLamports: max, MeetsFloor: &yes, ShortfallLamports: &zero, SurplusLamports: &zero}
	row := EpochStakeSet{Epoch: f.Epoch, Generation: max, MaterialKnown: true, Member: &yes,
		SelectedStakeLamports: &max, TotalStakeLamports: &zero, MetadataNodeIdentity: &node,
		NodeMatchesIdentity: &yes, SelectionBalanceLamports: &max}
	next := row
	next.Epoch++
	f.EpochStakeSets = &EpochStakeSets{Provenance: EpochStakeSetsProvenance, Current: row, Next: &next,
		Payer: &EpochPayerReference{ReferenceSlot: 0, Epoch: next.Epoch, Member: &yes}}
	return s
}

func TestTicketCapacityExact(t *testing.T) {
	for _, tc := range []struct {
		balance, rent, pending, ticket, want uint64
	}{
		{0, 0, 0, 1, 0}, {3, 4, 0, 2, 0}, {8, 4, 5, 2, 0},
		{9, 4, 5, 2, 0}, {10, 4, 5, 2, 0}, {11, 4, 5, 2, 1},
		{17, 4, 5, 2, 4}, {math.MaxUint64, 0, 0, 1, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64, 1, 0},
		{math.MaxUint64, 0, math.MaxUint64 - 1, 1, 1},
	} {
		s := admissionFactsFixture(time.Now().UTC())
		f := s.Replay.AccountFacts
		f.VoteBalanceLamports = &tc.balance
		f.StoredVoteHeader.PendingDelegatorRewards = &tc.pending
		floor := uint64(math.MaxUint64)
		if tc.ticket <= math.MaxUint64-tc.rent {
			floor = tc.rent + tc.ticket
		}
		f.FundingRules = &FundingRules{ReferenceSlot: f.Slot, VATFilteringActive: true, VATChargeGateActive: true,
			RentMinimumLamports: tc.rent, BurnLamports: tc.ticket, FloorLamports: floor}
		p := publicAccountFacts(&s, "fresh", f.SampledAt)
		if p == nil || p.TicketCapacityNoIncome == nil || p.TicketCapacityNoIncome.Status != "known" ||
			p.TicketCapacityNoIncome.Tickets == nil || *p.TicketCapacityNoIncome.Tickets != tc.want {
			t.Fatalf("balance=%d rent=%d pending=%d ticket=%d: got %+v, want %d", tc.balance, tc.rent, tc.pending, tc.ticket, p, tc.want)
		}
		wire, err := json.Marshal(p.TicketCapacityNoIncome)
		if err != nil || string(wire) != `{"tickets":"`+strconv.FormatUint(tc.want, 10)+`","status":"known"}` {
			t.Fatalf("ticket capacity wire lost precision or changed its allowlist: %s, %v", wire, err)
		}
		// Selection membership is not a prerequisite or evidence of a paid seat.
		f.EpochStakeSets = nil
		if again := publicAccountFacts(&s, "fresh", f.SampledAt); !reflect.DeepEqual(again.TicketCapacityNoIncome, p.TicketCapacityNoIncome) {
			t.Fatal("ticket capacity depends on epoch-cache membership")
		}
		*f.VoteBalanceLamports = 0
		if *p.TicketCapacityNoIncome.Tickets != tc.want {
			t.Fatal("derived capacity aliases source balance")
		}
	}
}

func TestTicketCapacityUnknownAndRole(t *testing.T) {
	for _, tc := range []struct {
		status string
		change func(*AccountFacts)
	}{
		{"rules_unknown", func(f *AccountFacts) { f.FundingRules = nil }},
		{"rules_unknown", func(f *AccountFacts) { f.FundingRules.ReferenceSlot++ }},
		{"charge_inactive", func(f *AccountFacts) { f.FundingRules.VATChargeGateActive = false }},
		{"zero_ticket", func(f *AccountFacts) { f.FundingRules.BurnLamports = 0 }},
		{"balance_unknown", func(f *AccountFacts) {
			f.VoteBalanceLamports = nil
			f.FundingRules.MeetsFloor = nil
			f.FundingRules.ShortfallLamports = nil
			f.FundingRules.SurplusLamports = nil
		}},
		{"liability_unknown", func(f *AccountFacts) { f.StoredVoteHeader = nil }},
		{"liability_unknown", func(f *AccountFacts) { invalid := "invalid"; f.StoredVoteHeader.NodeIdentity = &invalid }},
		{"liability_unknown", func(f *AccountFacts) { f.StoredVoteHeader.PendingDelegatorRewards = nil }},
	} {
		s := admissionFactsFixture(time.Now().UTC())
		tc.change(s.Replay.AccountFacts)
		p := publicAccountFacts(&s, "fresh", s.SampledAt)
		if p == nil || p.TicketCapacityNoIncome == nil || p.TicketCapacityNoIncome.Status != tc.status || p.TicketCapacityNoIncome.Tickets != nil {
			t.Fatalf("%s: got %+v", tc.status, p)
		}
		wire, err := json.Marshal(p.TicketCapacityNoIncome)
		if err != nil || string(wire) != `{"tickets":null,"status":"`+tc.status+`"}` {
			t.Fatalf("unavailable ticket capacity changed null/status semantics: %s, %v", wire, err)
		}
	}
	s := admissionFactsFixture(time.Now().UTC())
	s.Mode, s.Consensus = "verifying", nil
	if p := publicAccountFacts(&s, "fresh", s.SampledAt); p == nil || p.TicketCapacityNoIncome != nil {
		t.Fatal("non-voting role advertised a ticket scenario")
	}
	s.Mode = "validator"
	f := s.Replay.AccountFacts
	f.VoteAccount, f.VoteBalanceLamports, f.StoredCommissions, f.StoredVoteHeader, f.FundingRules, f.EpochStakeSets = nil, nil, nil, nil, nil, nil
	if p := publicAccountFacts(&s, "fresh", s.SampledAt); p == nil || p.TicketCapacityNoIncome != nil {
		t.Fatal("missing vote subject advertised ticket capacity")
	}
}

func TestPublicAdmissionFactsPrecisionAndDetachedAllowlist(t *testing.T) {
	at := time.Now().UTC()
	s := admissionFactsFixture(at)
	p, _ := publicValue(t, &s, "fresh", at)
	f := p.Replay.AccountFacts
	if f == nil || f.StoredVoteHeader == nil || f.FundingRules == nil || f.EpochStakeSets == nil {
		t.Fatalf("admission projection lost values: %+v", f)
	}
	for _, pair := range [][2]any{{s.Replay.AccountFacts.StoredVoteHeader, f.StoredVoteHeader},
		{s.Replay.AccountFacts.FundingRules, f.FundingRules}, {s.Replay.AccountFacts.EpochStakeSets, f.EpochStakeSets}} {
		want, err := json.Marshal(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(pair[1])
		if err != nil || string(got) != string(want) {
			t.Fatalf("admission projection changed: got %s, %v; want %s", got, err, want)
		}
	}
	// An incomplete cached total must not erase a legitimate raw stake entry.
	if *f.EpochStakeSets.Current.SelectedStakeLamports <= *f.EpochStakeSets.Current.TotalStakeLamports {
		t.Fatal("fixture stopped covering raw incomplete totals")
	}
	before, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"pending_delegator_rewards_lamports", "rent_minimum_lamports", "burn_lamports", "floor_lamports", "generation", "selected_stake_lamports", "selection_balance_lamports"} {
		if !strings.Contains(string(before), `"`+field+`":"18446744073709551615"`) {
			t.Fatalf("precision lost for %s: %s", field, before)
		}
	}
	for _, tc := range []struct {
		value any
		keys  []string
	}{
		{f.StoredVoteHeader, []string{"node_identity", "pending_delegator_rewards_lamports", "bls_registration_present"}},
		{f.FundingRules, []string{"reference_slot", "vat_filtering_active", "vat_charge_gate_active", "rent_minimum_lamports", "burn_lamports", "floor_lamports", "meets_floor", "shortfall_lamports", "surplus_lamports"}},
		{f.EpochStakeSets, []string{"provenance", "current", "next", "payer_reference"}},
		{f.EpochStakeSets.Current, []string{"epoch", "generation", "material_known", "member", "selected_stake_lamports", "total_stake_lamports", "metadata_node_identity", "node_matches_identity", "selection_balance_lamports"}},
		{f.EpochStakeSets.Payer, []string{"reference_slot", "epoch", "member"}},
	} {
		b, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil || len(fields) != len(tc.keys) {
			t.Fatalf("allowlist changed: %s, %v", b, err)
		}
		for _, key := range tc.keys {
			if _, ok := fields[key]; !ok {
				t.Fatalf("missing allowlisted field %s", key)
			}
		}
	}
	// Change every pointed source value. Public DTOs must own their copies.
	var mutate func(reflect.Value)
	mutate = func(v reflect.Value) {
		if v.Kind() == reflect.Pointer && !v.IsNil() {
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := range v.NumField() {
				mutate(v.Field(i))
			}
		case reflect.Bool:
			v.SetBool(!v.Bool())
		case reflect.Uint64:
			v.SetUint(v.Uint() + 1)
		case reflect.String:
			v.SetString("changed")
		}
	}
	for _, v := range []any{s.Replay.AccountFacts.StoredVoteHeader, s.Replay.AccountFacts.FundingRules, s.Replay.AccountFacts.EpochStakeSets} {
		mutate(reflect.ValueOf(v))
	}
	after, err := json.Marshal(f)
	if err != nil || string(before) != string(after) {
		t.Fatal("public admission pointers alias the private producer")
	}
}

func TestPublicAdmissionUnknownsAndRole(t *testing.T) {
	at := time.Now().UTC()
	s := admissionFactsFixture(at)
	s.Mode, s.Consensus = "verifying", nil
	f := s.Replay.AccountFacts
	zero, no, zeroKey := uint64(0), false, (solana.PublicKey{}).String()
	f.StoredVoteHeader = &StoredVoteHeader{NodeIdentity: &zeroKey, PendingDelegatorRewards: &zero, BLSRegistrationPresent: &no}
	f.FundingRules = nil
	f.EpochStakeSets.Current = EpochStakeSet{Epoch: f.Epoch, Generation: 9}
	f.EpochStakeSets.Next = &EpochStakeSet{Epoch: f.Epoch + 1, Generation: 10, MaterialKnown: true,
		Member: &no, SelectedStakeLamports: &zero, TotalStakeLamports: &zero}
	f.EpochStakeSets.Payer.Member = &no
	p := publicAccountFacts(&s, "fresh", at)
	if p == nil || p.StoredVoteHeader == nil || *p.StoredVoteHeader.PendingDelegatorRewards != 0 || *p.StoredVoteHeader.BLSRegistrationPresent ||
		p.FundingRules != nil || p.EpochStakeSets == nil || p.EpochStakeSets.Current.Member != nil ||
		p.EpochStakeSets.Next.Member == nil || *p.EpochStakeSets.Next.Member || p.EpochStakeSets.Next.MetadataNodeIdentity != nil {
		t.Fatal("unknown, known zero, nonmember or full-node semantics lost")
	}
	f.StoredVoteHeader = &StoredVoteHeader{}
	f.EpochStakeSets.Payer = &EpochPayerReference{Epoch: f.Epoch + 2, ReferenceSlot: 0}
	p = publicAccountFacts(&s, "fresh", at)
	if p.StoredVoteHeader == nil || p.StoredVoteHeader.NodeIdentity != nil || p.EpochStakeSets == nil || p.EpochStakeSets.Payer.Member != nil {
		t.Fatal("unknown header or outside payer was fabricated")
	}
	f.Epoch, s.Replay.Completed.Epoch = math.MaxUint64, math.MaxUint64
	f.EpochStakeSets.Current.Epoch, f.EpochStakeSets.Next, f.EpochStakeSets.Payer = math.MaxUint64, nil, nil
	if p = publicAccountFacts(&s, "fresh", at); p == nil || p.EpochStakeSets == nil || p.EpochStakeSets.Next != nil {
		t.Fatal("maximum epoch current facts were lost or wrapped")
	}
	for _, subject := range []string{"identity", "vote"} {
		s = admissionFactsFixture(at)
		if subject == "identity" {
			s.Consensus.Voting.Identity = (solana.PublicKey{9}).String()
		} else {
			s.Consensus.Voting.VoteAccount = (solana.PublicKey{9}).String()
		}
		p = publicAccountFacts(&s, "fresh", at)
		if p == nil || p.IdentityBalanceLamports == nil || p.VoteAccount != nil || p.StoredVoteHeader != nil || p.FundingRules != nil || p.EpochStakeSets != nil || p.TicketCapacityNoIncome != nil {
			t.Fatalf("%s mismatch retained vote extensions", subject)
		}
	}
	for _, extension := range []string{"header", "funding", "epochs"} {
		s = accountFactsFixture(at)
		f = s.Replay.AccountFacts
		f.VoteAccount, f.VoteBalanceLamports, f.StoredCommissions = nil, nil, nil
		switch extension {
		case "header":
			f.StoredVoteHeader = &StoredVoteHeader{}
		case "funding":
			f.FundingRules = &FundingRules{}
		case "epochs":
			f.EpochStakeSets = &EpochStakeSets{}
		}
		if publicAccountFacts(&s, "fresh", at) != nil {
			t.Fatalf("%s was accepted without a vote subject", extension)
		}
	}
}

func TestPublicAdmissionRejectsContradictoryChildren(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*AccountFacts){
		"header invalid key":          func(f *AccountFacts) { v := "bad"; f.StoredVoteHeader.NodeIdentity = &v },
		"funding reference":           func(f *AccountFacts) { f.FundingRules.ReferenceSlot++ },
		"funding floor":               func(f *AccountFacts) { f.FundingRules.FloorLamports-- },
		"funding gate":                func(f *AccountFacts) { f.FundingRules.VATFilteringActive = false },
		"funding missing comparison":  func(f *AccountFacts) { f.FundingRules.MeetsFloor = nil },
		"funding comparison":          func(f *AccountFacts) { v := false; f.FundingRules.MeetsFloor = &v },
		"funding shortfall":           func(f *AccountFacts) { v := uint64(1); f.FundingRules.ShortfallLamports = &v },
		"funding surplus":             func(f *AccountFacts) { v := uint64(1); f.FundingRules.SurplusLamports = &v },
		"funding balance unknown":     func(f *AccountFacts) { f.VoteBalanceLamports = nil },
		"epochs provenance":           func(f *AccountFacts) { f.EpochStakeSets.Provenance = "bank" },
		"epochs current":              func(f *AccountFacts) { f.EpochStakeSets.Current.Epoch++ },
		"epochs next missing":         func(f *AccountFacts) { f.EpochStakeSets.Next = nil },
		"epochs next wrong":           func(f *AccountFacts) { f.EpochStakeSets.Next.Epoch++ },
		"epochs generation":           func(f *AccountFacts) { f.EpochStakeSets.Current.Generation = 0 },
		"epochs unknown populated":    func(f *AccountFacts) { f.EpochStakeSets.Current.MaterialKnown = false },
		"epochs member missing":       func(f *AccountFacts) { f.EpochStakeSets.Current.Member = nil },
		"epochs member contradiction": func(f *AccountFacts) { v := false; f.EpochStakeSets.Current.Member = &v },
		"epochs metadata partial":     func(f *AccountFacts) { f.EpochStakeSets.Current.NodeMatchesIdentity = nil },
		"epochs metadata invalid":     func(f *AccountFacts) { v := "bad"; f.EpochStakeSets.Current.MetadataNodeIdentity = &v },
		"epochs metadata mismatch":    func(f *AccountFacts) { v := false; f.EpochStakeSets.Current.NodeMatchesIdentity = &v },
		"epochs payer future slot":    func(f *AccountFacts) { f.EpochStakeSets.Payer.ReferenceSlot = f.Slot + 1 },
		"epochs payer missing member": func(f *AccountFacts) { f.EpochStakeSets.Payer.Member = nil },
		"epochs payer wrong member":   func(f *AccountFacts) { v := false; f.EpochStakeSets.Payer.Member = &v },
		"epochs payer outside known":  func(f *AccountFacts) { f.EpochStakeSets.Payer.Epoch += 10 },
	} {
		t.Run(name, func(t *testing.T) {
			s := admissionFactsFixture(at)
			mutate(s.Replay.AccountFacts)
			p := publicAccountFacts(&s, "fresh", at)
			if p == nil || p.IdentityBalanceLamports == nil || p.StoredCommissions == nil {
				t.Fatal("child failure erased unrelated valid account facts")
			}
			if (p.StoredVoteHeader == nil) != strings.HasPrefix(name, "header") ||
				(p.FundingRules == nil) != strings.HasPrefix(name, "funding") ||
				(p.EpochStakeSets == nil) != strings.HasPrefix(name, "epochs") {
				t.Fatalf("contradictory child accepted or valid sibling lost: %+v", p)
			}
		})
	}
}

func TestAdmissionFactsWireTypesAndOldProducer(t *testing.T) {
	for _, path := range []string{"stored_vote_header", "funding_rules", "epoch_stake_sets"} {
		for _, value := range []string{"-1", "1", "1.5", `"-1"`, `"18446744073709551616"`, "true"} {
			var body string
			switch path {
			case "stored_vote_header":
				body = `{"stored_vote_header":{"pending_delegator_rewards_lamports":` + value + `}}`
			case "funding_rules":
				body = `{"funding_rules":{"floor_lamports":` + value + `}}`
			case "epoch_stake_sets":
				body = `{"epoch_stake_sets":{"current":{"generation":` + value + `}}}`
			}
			var private AccountFacts
			var public PublicAccountFacts
			if json.Unmarshal([]byte(body), &private) == nil || json.Unmarshal([]byte(body), &public) == nil {
				t.Fatalf("non-uint64-string accepted: %s", body)
			}
		}
	}
	s := accountFactsFixture(time.Now().UTC())
	p, body := publicValue(t, &s, "fresh", s.SampledAt)
	if p.Replay.AccountFacts == nil || strings.Contains(string(body), "stored_vote_header") || strings.Contains(string(body), "funding_rules") || strings.Contains(string(body), "epoch_stake_sets") {
		t.Fatal("old producer acquired optional fields")
	}
}
