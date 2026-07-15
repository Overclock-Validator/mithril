package node

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

func recoveryFixture(t *testing.T, slot uint64, bankhash [32]byte) accountsdb.RecoveryResult {
	t.Helper()
	resume := state.ResumeContext{
		Slot:                  slot,
		Bankhash:              base58.Encode(bankhash[:]),
		BlockHeight:           slot + 10,
		Epoch:                 7,
		ComputedEpochStakes:   map[uint64]string{7: `{"epoch":7}`},
		EpochAuthorizedVoters: map[string][]string{"vote": {"voter"}},
	}
	encoded, err := json.Marshal(resume)
	if err != nil {
		t.Fatal(err)
	}
	return accountsdb.RecoveryResult{
		DurableThrough: slot,
		RootedBankhash: bankhash,
		ResumeCtx:      encoded,
	}
}

func TestReconcileFoldRecoveryAdoptsStoreAheadOfState(t *testing.T) {
	bankhash := [32]byte{9}
	recovered := recoveryFixture(t, 120, bankhash)
	s := &state.MithrilState{LastRootedSlot: 100, LastSlot: 110}

	changed, err := reconcileFoldRecovery(s, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || s.LastRootedSlot != 120 || s.LastSlot != 120 || s.LastRootedContext == nil {
		t.Fatalf("store frontier was not adopted: changed=%v state=%+v", changed, s)
	}
	if s.ComputedEpochStakes[7] == "" || len(s.ManifestEpochAuthorizedVoters) != 1 {
		t.Fatalf("epoch resume metadata was not adopted: stakes=%v voters=%v", s.ComputedEpochStakes, s.ManifestEpochAuthorizedVoters)
	}
}

func TestReconcileFoldRecoveryRejectsUnexplainedStoreRegression(t *testing.T) {
	recovered := recoveryFixture(t, 100, [32]byte{1})
	s := &state.MithrilState{LastRootedSlot: 120}

	if _, err := reconcileFoldRecovery(s, recovered); err == nil {
		t.Fatal("expected store behind state without a rewind marker to fail closed")
	}
}

func TestReconcileFoldRecoveryCompletesInterruptedRewind(t *testing.T) {
	bankhash := [32]byte{2}
	recovered := recoveryFixture(t, 100, bankhash)
	recovered.RewindInProgress = true
	s := &state.MithrilState{LastRootedSlot: 120, LastSlot: 125}

	changed, err := reconcileFoldRecovery(s, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || s.LastRootedSlot != 100 || s.LastSlot != 100 {
		t.Fatalf("interrupted rewind was not completed: changed=%v rooted=%d last=%d", changed, s.LastRootedSlot, s.LastSlot)
	}
}

func TestReconcileFoldRecoveryChecksManifestBankhash(t *testing.T) {
	recovered := recoveryFixture(t, 120, [32]byte{3})
	recovered.RootedBankhash = [32]byte{4}

	if _, err := reconcileFoldRecovery(&state.MithrilState{}, recovered); err == nil {
		t.Fatal("expected mismatched resume and manifest bankhashes to fail")
	}
}

func TestReconcileFoldRecoveryChecksExistingStateBankhash(t *testing.T) {
	recovered := recoveryFixture(t, 120, [32]byte{3})
	otherBankhash := [32]byte{4}
	s := &state.MithrilState{
		LastRootedSlot: 120,
		LastRootedContext: &state.ResumeContext{
			Slot:     120,
			Bankhash: base58.Encode(otherBankhash[:]),
		},
	}

	if _, err := reconcileFoldRecovery(s, recovered); err == nil {
		t.Fatal("expected an existing state/manifest bankhash mismatch to fail")
	}
}

func TestReconcileFoldRecoveryRefreshesSameSlotResumeMetadata(t *testing.T) {
	bankhash := [32]byte{5}
	recovered := recoveryFixture(t, 120, bankhash)
	s := &state.MithrilState{
		LastRootedSlot:     120,
		LastRootedBankhash: base58.Encode(bankhash[:]),
		LastRootedContext: &state.ResumeContext{
			Slot:     120,
			Bankhash: base58.Encode(bankhash[:]),
		},
		LastSlot: 130,
	}

	changed, err := reconcileFoldRecovery(s, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || s.LastSlot != 120 {
		t.Fatalf("same-slot recovery did not discard the vanished RAM suffix: changed=%v last=%d", changed, s.LastSlot)
	}
	if s.LastRootedContext.ComputedEpochStakes[7] == "" || s.ComputedEpochStakes[7] == "" {
		t.Fatal("same-slot recovery did not hydrate epoch stakes from the fold manifest")
	}
	if len(s.LastRootedContext.EpochAuthorizedVoters) != 1 || len(s.ManifestEpochAuthorizedVoters) != 1 {
		t.Fatal("same-slot recovery did not hydrate authorized voters from the fold manifest")
	}
}
