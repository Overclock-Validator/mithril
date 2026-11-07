package cluster

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProviderSOLScientificNotation(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		ok          bool
	}{
		{"0.000000001", "0.000000001", true},
		{"1e-9", "0.000000001", true},
		{"1.25E+3", "1250", true},
		{"0e-20", "0", true},
		{"1e-18", "0.000000000000000001", true},
		{"1e-19", "", false},
		{"1e999999999", "", false},
		{"1e64", "", false},
		{"-1e0", "", false},
		{"01e0", "", false},
		{"1e", "", false},
	} {
		got, ok := providerSOL(tc.input)
		if ok != tc.ok || ok && got != tc.want {
			t.Errorf("providerSOL(%q) = %q, %v; want %q, %v", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSolstatScientificStake(t *testing.T) {
	f := newFixture()
	f.solstat["validators"].([]any)[0].(map[string]any)["activatedStake"] = json.Number("1e-9")
	f.solstat["networkStats"].(map[string]any)["totalStake"] = json.Number("1.25e3")
	data, err := json.Marshal(f.solstat)
	if err != nil {
		t.Fatal(err)
	}
	var source solstatResponse
	if err := json.Unmarshal(data, &source); err != nil {
		t.Fatal(err)
	}
	epoch, slot, index, length := uint64(1), uint64(115), uint64(15), uint64(100)
	e := epochInfo{Epoch: &epoch, AbsoluteSlot: &slot, SlotIndex: &index, SlotsInEpoch: &length}
	votes := make(map[string]string, len(testVotes))
	for i, vote := range testVotes {
		votes[vote] = testIdentities[i]
	}
	rows, _, err := parseSolstat(source, e, votes, f.now)
	if err != nil || len(rows) != len(testVotes) || rows[0].StakeSOL == nil || *rows[0].StakeSOL != "0.000000001" {
		t.Fatalf("scientific validator stake: rows=%+v err=%v", rows, err)
	}
	wire, err := json.Marshal(rows[0])
	if err != nil || !strings.Contains(string(wire), `"stake_sol":"0.000000001"`) {
		t.Fatalf("serialized stake: %s, %v", wire, err)
	}
	if summary := parseSolstatSummary(source.NetworkStats, e); summary == nil || summary.TotalStakeSOL != "1250" {
		t.Fatalf("scientific summary stake: %+v", summary)
	}
}
