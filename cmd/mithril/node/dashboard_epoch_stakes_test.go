package node

import (
	"encoding/binary"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/gagliardetto/solana-go"
)

func vatEpochStakeFixture() epochstakes.Snapshot {
	return epochstakes.Snapshot{
		Epoch: 7, Generation: 9, Selection: epochstakes.SelectionVATFiltered,
		Stakes:       map[solana.PublicKey]uint64{{1}: 3, {2}: 5, {3}: 7},
		VoteAccounts: map[solana.PublicKey]*epochstakes.VoteAccount{{1}: {NodePubkey: solana.PublicKey{2}}, {2}: {NodePubkey: solana.PublicKey{1}}, {3}: {NodePubkey: solana.PublicKey{2}}},
		TotalStake:   15,
	}
}

func TestCaptureVATEpochStakesAvailabilityAndMalformedMaterial(t *testing.T) {
	for _, tc := range []struct {
		name   string
		known  bool
		change func(*epochstakes.Snapshot)
		reason string
	}{
		{"absent", false, func(*epochstakes.Snapshot) {}, "waiting_source"},
		{"unknown selection", true, func(s *epochstakes.Snapshot) { s.Selection = epochstakes.SelectionUnknown }, "selection_unverified"},
		{"future selection", true, func(s *epochstakes.Snapshot) { s.Selection = epochstakes.Selection(255) }, "selection_unverified"},
		{"unfiltered", true, func(s *epochstakes.Snapshot) { s.Selection = epochstakes.SelectionUnfiltered }, "not_applicable"},
		{"unknown generation", true, func(s *epochstakes.Snapshot) { s.Generation = 0 }, "source_invalid"},
		{"missing metadata", true, func(s *epochstakes.Snapshot) { delete(s.VoteAccounts, solana.PublicKey{1}) }, "source_invalid"},
		{"nil metadata", true, func(s *epochstakes.Snapshot) { s.VoteAccounts[solana.PublicKey{1}] = nil }, "source_invalid"},
		{"zero identity", true, func(s *epochstakes.Snapshot) { s.VoteAccounts[solana.PublicKey{1}].NodePubkey = solana.PublicKey{} }, "source_invalid"},
		{"zero vote", true, func(s *epochstakes.Snapshot) {
			s.Stakes[solana.PublicKey{}] = 1
			s.VoteAccounts[solana.PublicKey{}] = &epochstakes.VoteAccount{NodePubkey: solana.PublicKey{8}}
			s.TotalStake++
		}, "source_invalid"},
		{"zero weight", true, func(s *epochstakes.Snapshot) { s.Stakes[solana.PublicKey{1}] = 0; s.TotalStake -= 3 }, "source_invalid"},
		{"total below sum", true, func(s *epochstakes.Snapshot) { s.TotalStake-- }, "source_invalid"},
		{"total above sum", true, func(s *epochstakes.Snapshot) { s.TotalStake++ }, "source_invalid"},
		{"empty with nonzero total", true, func(s *epochstakes.Snapshot) { s.Stakes = nil }, "source_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := vatEpochStakeFixture()
			tc.change(&snapshot)
			got, err := captureVATEpochStakes(snapshot, tc.known)
			if got != nil || err == nil || err.Error() != tc.reason {
				t.Fatalf("got %+v, %v; want nil, %s", got, err, tc.reason)
			}
		})
	}
}

func TestCaptureVATEpochStakesInputCapAndCanonicalIdentityOrder(t *testing.T) {
	snapshot := epochstakes.Snapshot{Generation: 1, Selection: epochstakes.SelectionVATFiltered,
		Stakes: make(map[solana.PublicKey]uint64), VoteAccounts: make(map[solana.PublicKey]*epochstakes.VoteAccount)}
	for i := uint64(1); i <= alpenglow.MaximumVATValidators; i++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[:8], i)
		snapshot.Stakes[key] = i
		snapshot.VoteAccounts[key] = &epochstakes.VoteAccount{NodePubkey: key}
		snapshot.TotalStake += i
	}
	got, err := captureVATEpochStakes(snapshot, true)
	if err != nil || len(got.Identities) != alpenglow.MaximumVATValidators || len(got.Stakes) != alpenglow.MaximumVATValidators {
		t.Fatalf("exact input cap refused: %+v, %v", got, err)
	}
	var previous string
	for i, identity := range got.Identities {
		key, err := solana.PublicKeyFromBase58(identity)
		if err != nil || key.String() != identity || got.Stakes[i] != strconv.FormatUint(snapshot.Stakes[key], 10) || i > 0 && previous >= identity {
			t.Fatalf("entry %d lost a stake or canonical identity text ordering", i)
		}
		previous = identity
	}
	// The cap is on input vote weights, even when grouping would fit one row.
	for _, meta := range snapshot.VoteAccounts {
		meta.NodePubkey = solana.PublicKey{1}
	}
	extra := solana.PublicKey{255, 255}
	snapshot.Stakes[extra] = 1
	snapshot.VoteAccounts[extra] = &epochstakes.VoteAccount{NodePubkey: solana.PublicKey{1}}
	snapshot.TotalStake++
	if got, err := captureVATEpochStakes(snapshot, true); got != nil || err == nil || err.Error() != "response_limit" {
		t.Fatalf("2001 weights silently grouped or truncated: %+v, %v", got, err)
	}
}

func TestCaptureVATEpochStakesDetachedAcrossImmutablePublication(t *testing.T) {
	input := vatEpochStakeFixture()
	cache := epochstakes.NewEpochStakesCache()
	cache.PutEpochWithSelection(input.Epoch, input.Stakes, input.VoteAccounts, input.TotalStake, epochstakes.SelectionVATFiltered)
	first, known := cache.Snapshot(input.Epoch)
	got, err := captureVATEpochStakes(first, known)
	if err != nil {
		t.Fatal(err)
	}
	want := *got
	want.Identities, want.Stakes = slices.Clone(got.Identities), slices.Clone(got.Stakes)
	// Publisher-owned input changes cannot affect the first immutable snapshot
	// or the detached arrays, even after a replacement is published.
	input.Stakes[solana.PublicKey{1}] = 8
	input.VoteAccounts[solana.PublicKey{1}].NodePubkey = solana.PublicKey{3}
	input.TotalStake += 5
	cache.PutEpochWithSelection(input.Epoch, input.Stakes, input.VoteAccounts, input.TotalStake, epochstakes.SelectionVATFiltered)
	second, known := cache.Snapshot(input.Epoch)
	replaced, err := captureVATEpochStakes(second, known)
	if err != nil || replaced.Generation == got.Generation || replaced.TotalStake != 20 || reflect.DeepEqual(replaced.Identities, got.Identities) {
		t.Fatalf("replacement did not capture its own generation/material: %+v, %v", replaced, err)
	}
	if !reflect.DeepEqual(*got, want) || first.Stakes[solana.PublicKey{1}] != 3 || first.VoteAccounts[solana.PublicKey{1}].NodePubkey != (solana.PublicKey{2}) {
		t.Fatal("replacement mutated a retained snapshot or detached capture")
	}
	again, err := captureVATEpochStakes(first, true)
	if err != nil || !reflect.DeepEqual(*again, want) {
		t.Fatal("old immutable snapshot no longer captures the same arrays")
	}
	got.Identities[0], got.Stakes[0], got.Generation = "caller change", "0", 0
	again, err = captureVATEpochStakes(first, true)
	if err != nil || !reflect.DeepEqual(*again, want) {
		t.Fatal("caller result mutation reached the same source generation")
	}
	again, err = captureVATEpochStakes(second, true)
	if err != nil || !reflect.DeepEqual(again, replaced) {
		t.Fatal("caller result mutation reached source or another capture")
	}
	cache.ClearEpochStakes(input.Epoch)
	cleared, known := cache.Snapshot(input.Epoch)
	if got, err := captureVATEpochStakes(cleared, known); got != nil || err == nil || err.Error() != "waiting_source" {
		t.Fatalf("retired source retained availability: %+v, %v", got, err)
	}
	if !reflect.DeepEqual(replaced, again) {
		t.Fatal("source retirement changed a detached capture")
	}
}
