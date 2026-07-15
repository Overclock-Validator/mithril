package replay

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

func validRootedResumeContext(slot uint64) *state.ResumeContext {
	bankhash := solana.Hash{1}
	blockID := solana.Hash{2}
	chainedRoot := solana.Hash{3}
	blockhash := solana.Hash{4}
	evicted := solana.Hash{5}
	slotHash := solana.Hash{6}
	count := uint64(0)
	clock := &sealevel.SysvarClock{Slot: slot}
	return &state.ResumeContext{
		Slot:                 slot,
		Epoch:                7,
		Bankhash:             base58.Encode(bankhash[:]),
		AcctsLtHash:          base64.StdEncoding.EncodeToString(make([]byte, lthash.HashByteLen)),
		AlpenglowBlockID:     base58.Encode(blockID[:]),
		AlpenglowChainedRoot: base58.Encode(chainedRoot[:]),
		TransactionCount:     &count,
		RecentBlockhashes: []state.BlockhashEntry{{
			Blockhash: base58.Encode(blockhash[:]),
		}},
		EvictedBlockhash: base58.Encode(evicted[:]),
		Blockhash:        base58.Encode(blockhash[:]),
		SlotHashes: []state.SlotHashEntry{{
			Slot: slot,
			Hash: base58.Encode(slotHash[:]),
		}},
		Clock: base64.StdEncoding.EncodeToString(clock.MustMarshal()),
	}
}

func TestResumeStateFromRootedContextRestoresAlpenglowIdentity(t *testing.T) {
	blockID := solana.Hash{2}
	chainedRoot := solana.Hash{3}
	context := validRootedResumeContext(42)

	resume, err := ResumeStateFromRootedContext(context, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resume.HasAlpenglowIdentity || resume.AlpenglowBlockID != blockID || resume.AlpenglowChainedRoot != chainedRoot {
		t.Fatalf("Alpenglow identity was not restored: %+v", resume)
	}
	if resume.ParentEpoch != context.Epoch {
		t.Fatalf("parent epoch = %d, want %d", resume.ParentEpoch, context.Epoch)
	}
}

func TestResumeStateFromRootedContextRejectsPartialAlpenglowIdentity(t *testing.T) {
	context := validRootedResumeContext(42)
	context.AlpenglowChainedRoot = ""

	if _, err := ResumeStateFromRootedContext(context, nil); err == nil {
		t.Fatal("expected a partial Alpenglow identity to be rejected")
	}
}

func TestResumeStateFromRootedContextUsesFoldEpochStakes(t *testing.T) {
	context := validRootedResumeContext(42)
	foldStakes := `{"epoch":8,"total_stake":0,"stakes":{},"vote_accts":{}}`
	context.ComputedEpochStakes = map[uint64]string{8: foldStakes}

	resume, err := ResumeStateFromRootedContext(context, map[uint64]string{7: "stale-state-file"})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(resume.ComputedEpochStakes[8]); got != foldStakes {
		t.Fatalf("fold epoch stakes = %q, want %q", got, foldStakes)
	}
	if _, stale := resume.ComputedEpochStakes[7]; stale {
		t.Fatal("stale state-file epoch stakes overrode the durable fold context")
	}
}

func TestResumeStateFromRootedContextRejectsPartialBundle(t *testing.T) {
	context := validRootedResumeContext(42)
	context.EvictedBlockhash = ""

	if _, err := ResumeStateFromRootedContext(context, nil); err == nil {
		t.Fatal("expected a rooted context missing one blockhash member to fail closed")
	}
}

func TestResumeStateFromCheckpointRejectsPartialBundle(t *testing.T) {
	bankhash := solana.Hash{1}
	recent := solana.Hash{2}
	blockhash := solana.Hash{3}
	slotHash := solana.Hash{4}
	checkpoint := &state.MithrilState{
		LastSlot:              42,
		LastEpoch:             7,
		LastBankhash:          base58.Encode(bankhash[:]),
		LastAcctsLtHash:       base64.StdEncoding.EncodeToString(make([]byte, lthash.HashByteLen)),
		LastRecentBlockhashes: []state.BlockhashEntry{{Blockhash: base58.Encode(recent[:])}},
		LastBlockhash:         base58.Encode(blockhash[:]),
		LastSlotHashes:        []state.SlotHashEntry{{Slot: 42, Hash: base58.Encode(slotHash[:])}},
		// LastEvictedBlockhash deliberately missing.
	}

	if _, err := ResumeStateFromCheckpoint(checkpoint); err == nil {
		t.Fatal("expected a partial graceful checkpoint to fail closed")
	}
}
