package replay

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

func TestRestoreSysvarCacheUsesSelectedBranch(t *testing.T) {
	oldClock := sealevel.SysvarCache.Clock
	oldSlotHashes := sealevel.SysvarCache.SlotHashes
	oldRecent := sealevel.SysvarCache.RecentBlockHashes
	oldSlotHistory := sealevel.SysvarCache.SlotHistory
	oldStakeHistory := sealevel.SysvarCache.StakeHistory
	oldRestart := sealevel.SysvarCache.LastRestartSlot
	oldEpochRewards := sealevel.SysvarCache.EpochRewards
	oldRent := sealevel.SysvarCache.Rent
	oldFees := sealevel.SysvarCache.Fees
	defer func() {
		sealevel.SysvarCache.Clock = oldClock
		sealevel.SysvarCache.SlotHashes = oldSlotHashes
		sealevel.SysvarCache.RecentBlockHashes = oldRecent
		sealevel.SysvarCache.SlotHistory = oldSlotHistory
		sealevel.SysvarCache.StakeHistory = oldStakeHistory
		sealevel.SysvarCache.LastRestartSlot = oldRestart
		sealevel.SysvarCache.EpochRewards = oldEpochRewards
		sealevel.SysvarCache.Rent = oldRent
		sealevel.SysvarCache.Fees = oldFees
	}()

	const slot = uint64(77)
	clock := &sealevel.SysvarClock{Slot: slot}
	slotHashes := sealevel.SysvarSlotHashes{{Slot: slot, Hash: solana.Hash{1}}}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: solana.Hash{2}}}
	slotHistory := &sealevel.SysvarSlotHistory{NextSlot: slot + 1}
	stakeHistory := sealevel.SysvarStakeHistory{{
		Epoch: 7,
		Entry: sealevel.StakeHistoryEntry{Effective: 123},
	}}
	epochRewards := &sealevel.SysvarEpochRewards{NumPartitions: 16, Active: true}
	rent := &sealevel.SysvarRent{LamportsPerUint8Year: 99, ExemptionThreshold: 2, BurnPercent: 50}

	marshalWithEncoder := func(marshal func(*bin.Encoder)) []byte {
		var buf bytes.Buffer
		marshal(bin.NewBinEncoder(&buf))
		return buf.Bytes()
	}
	restartData := make([]byte, 8)
	binary.LittleEndian.PutUint64(restartData, 55)
	byKey := map[solana.PublicKey]*accounts.Account{
		sealevel.SysvarClockAddr:             {Key: sealevel.SysvarClockAddr, Data: clock.MustMarshal()},
		sealevel.SysvarSlotHashesAddr:        {Key: sealevel.SysvarSlotHashesAddr, Data: slotHashes.MustMarshal()},
		sealevel.SysvarRecentBlockHashesAddr: {Key: sealevel.SysvarRecentBlockHashesAddr, Data: recent.MustMarshal()},
		sealevel.SysvarSlotHistoryAddr:       {Key: sealevel.SysvarSlotHistoryAddr, Data: slotHistory.MustMarshal()},
		sealevel.SysvarStakeHistoryAddr: {
			Key: sealevel.SysvarStakeHistoryAddr,
			Data: marshalWithEncoder(func(enc *bin.Encoder) {
				stakeHistory.MustMarshalWithEncoder(enc)
			}),
		},
		sealevel.SysvarLastRestartSlotAddr: {Key: sealevel.SysvarLastRestartSlotAddr, Data: restartData},
		sealevel.SysvarEpochRewardsAddr: {
			Key: sealevel.SysvarEpochRewardsAddr,
			Data: marshalWithEncoder(func(enc *bin.Encoder) {
				epochRewards.MustMarshalWithEncoder(enc)
			}),
		},
		sealevel.SysvarRentAddr: {Key: sealevel.SysvarRentAddr, Data: rent.MustMarshal()},
	}
	db := &accountsdb.AccountsDb{RootedDurable: true}
	cleanup := db.InstallAccountResolver(func(requestedSlot uint64, key solana.PublicKey) (*accounts.Account, error) {
		if requestedSlot != slot {
			t.Fatalf("resolver slot = %d, want %d", requestedSlot, slot)
		}
		acct := byKey[key]
		if acct == nil {
			return nil, accountsdb.ErrNoAccount
		}
		return acct.Clone(), nil
	})
	defer cleanup()

	if err := restoreSysvarCacheFromSnapshot(&ReplayHeadSnapshot{Slot: slot}, db, slot); err != nil {
		t.Fatal(err)
	}
	if sealevel.SysvarCache.Clock.Sysvar.Slot != slot ||
		(*sealevel.SysvarCache.SlotHashes.Sysvar)[0].Slot != slot ||
		(*sealevel.SysvarCache.RecentBlockHashes.Sysvar)[0].Blockhash != (solana.Hash{2}) ||
		sealevel.SysvarCache.SlotHistory.Sysvar.NextSlot != slot+1 ||
		(*sealevel.SysvarCache.StakeHistory.Sysvar)[0].Entry.Effective != 123 ||
		sealevel.SysvarCache.LastRestartSlot.Sysvar.LastRestartSlot != 55 ||
		sealevel.SysvarCache.EpochRewards.Sysvar.NumPartitions != 16 ||
		sealevel.SysvarCache.Rent.Sysvar.LamportsPerUint8Year != 99 {
		t.Fatal("selected branch sysvar cache was not fully restored")
	}
	if sealevel.SysvarCache.Fees.Sysvar != nil || sealevel.SysvarCache.Fees.Acct != nil {
		t.Fatal("absent optional fees sysvar was not cleared")
	}
}

func TestParentIdentityMismatchDropsSpeculativeTail(t *testing.T) {
	sr := NewSpeculativeReplay()
	sr.Enable()
	db := &accountsdb.AccountsDb{}
	db.InitCaches()
	programKey := solana.PublicKey{9}
	db.AddProgramToCache(programKey, &accountsdb.ProgramCacheEntry{})
	anchorID := solana.Hash{1}
	anchorRoot := solana.Hash{2}
	executedID := solana.Hash{3}
	selectedID := solana.Hash{4}

	sr.mu.Lock()
	sr.committedSlot = 10
	sr.finalityCursor = 10
	sr.headSnapshot = &ReplayHeadSnapshot{
		Slot:                 10,
		ParentSlot:           9,
		BlockHeight:          10,
		FinalBankhash:        make([]byte, 32),
		AcctsLtHash:          &lthash.LtHash{},
		HasAlpenglowIdentity: true,
		AlpenglowBlockID:     anchorID,
		AlpenglowChainedRoot: anchorRoot,
		Aux: ReplayAuxState{
			PartitionedEpochRewardsEnabled: true,
			PartitionedRewardsInfo: &rewards.PartitionedRewardDistributionInfo{
				SpoolSlot:                    8,
				NumRewardPartitions:          16,
				NumRewardPartitionsRemaining: 7,
			},
		},
	}
	sr.store.SetFinalizedSlot(10)
	sr.mu.Unlock()

	slotCtx := &sealevel.SlotCtx{
		Slot:          11,
		ParentSlot:    10,
		FinalBankhash: make([]byte, 32),
		AcctsLtHash:   &lthash.LtHash{},
		AcctMapsMu:    &sync.Mutex{},
	}
	if err := sr.TrackPending(&DeferredBlockCommit{
		SlotCtx:                 slotCtx,
		BlockSlot:               11,
		BlockHeight:             11,
		Bankhash:                make([]byte, 32),
		HasAlpenglowBlockID:     true,
		AlpenglowBlockID:        executedID,
		HasAlpenglowChainedRoot: true,
		AlpenglowChainedRoot:    solana.Hash{5},
		ModifiedAccts: []*accounts.Account{{
			Key:      solana.PublicKey{6},
			Lamports: 1,
		}},
	}, &ReplayCtx{}); err != nil {
		t.Fatal(err)
	}

	stream := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{
		SourceType:                   blockstream.BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    11,
		EndSlot:                      20,
	})
	var lastSlotCtx *sealevel.SlotCtx
	var restoredAux ReplayAuxState
	handled := sr.HandleParentIdentityMismatch(
		12, 11, selectedID, executedID,
		SpeculativeRollbackParams{
			AcctsDb:        db,
			ReplayCtx:      &ReplayCtx{},
			LastSlotCtx:    &lastSlotCtx,
			BlockStream:    stream,
			RestoreSysvars: func(*ReplayHeadSnapshot) error { return nil },
			RestoreAux: func(snapshot *ReplayHeadSnapshot) error {
				restoredAux = snapshot.Aux.clone()
				return nil
			},
		},
	)
	if !handled {
		t.Fatal("expected same-slot parent identity mismatch to switch the RAM branch")
	}
	if sr.LayerCount() != 0 {
		t.Fatalf("speculative layers = %d, want 0", sr.LayerCount())
	}
	if _, cached := db.MaybeGetProgramFromCache(programKey); cached {
		t.Fatal("program compiled on discarded branch survived rollback")
	}
	if lastSlotCtx == nil || lastSlotCtx.Slot != 10 || global.Slot() != 10 {
		t.Fatalf("replay head was not restored to anchor: slot_ctx=%v global=%d", lastSlotCtx, global.Slot())
	}
	if !restoredAux.PartitionedEpochRewardsEnabled || restoredAux.PartitionedRewardsInfo == nil ||
		restoredAux.PartitionedRewardsInfo.NumRewardPartitionsRemaining != 7 {
		t.Fatalf("replay auxiliary state was not restored: %+v", restoredAux)
	}
}

func TestForkSwitchRefusedBelowFinalityCursor(t *testing.T) {
	executedID := solana.Hash{3}
	selectedID := solana.Hash{4}
	sr, stream, lastSlotCtx := speculativeTailForDecisionTest(t, executedID)
	sr.mu.Lock()
	sr.finalityCursor = 11
	sr.mu.Unlock()

	handled := sr.HandleParentIdentityMismatch(
		12,
		11,
		selectedID,
		executedID,
		SpeculativeRollbackParams{
			ReplayCtx:      &ReplayCtx{},
			LastSlotCtx:    lastSlotCtx,
			BlockStream:    stream,
			RestoreSysvars: func(*ReplayHeadSnapshot) error { return nil },
			RestoreAux:     func(*ReplayHeadSnapshot) error { return nil },
		},
	)
	if handled {
		t.Fatal("fork switch crossed finalized watermark")
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.finalityCursor != 11 || sr.pending[11] == nil || sr.store.LayerCount() != 1 {
		t.Fatalf("finalized tail changed: cursor=%d pending=%v layers=%d", sr.finalityCursor, sr.pending[11] != nil, sr.store.LayerCount())
	}
}

func TestForkSwitchRefusalPreservesFinalizedBlockForFold(t *testing.T) {
	committer := &testFoldCommitter{}
	sr := newFoldTestReplay(t, committer, 128)
	executedID := addFoldTestPending(t, sr, 11, 10)
	selectedID := solana.Hash{99}
	sr.mu.Lock()
	sr.finalityCursor = 11
	sr.mu.Unlock()

	stream := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{
		SourceType:                   blockstream.BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		TurbineRepairOnly:            true,
		StartSlot:                    11,
		EndSlot:                      20,
	})
	var lastSlotCtx *sealevel.SlotCtx
	if sr.HandleParentIdentityMismatch(
		12,
		11,
		selectedID,
		executedID,
		SpeculativeRollbackParams{
			ReplayCtx:      &ReplayCtx{},
			LastSlotCtx:    &lastSlotCtx,
			BlockStream:    stream,
			RestoreSysvars: func(*ReplayHeadSnapshot) error { return nil },
			RestoreAux:     func(*ReplayHeadSnapshot) error { return nil },
		},
	) {
		t.Fatal("fork switch crossed finalized watermark")
	}
	committer.mu.Lock()
	commitsBeforeFlush := len(committer.through)
	committer.mu.Unlock()
	if commitsBeforeFlush != 0 {
		t.Fatalf("durable writes before forced fold = %d, want 0", commitsBeforeFlush)
	}

	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 10 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Slot:            11,
			Kind:            alpenglow.ChainDecisionKindBlock,
			Block:           alpenglow.BlockID{Slot: 11, Hash: executedID},
			CertificateType: alpenglow.CertificateFinalizeFast,
		}, true
	}
	if err := sr.FlushFinalized(&persistedTracker{}, decisionSource); err != nil {
		t.Fatal(err)
	}

	committer.mu.Lock()
	committedThrough := append([]uint64(nil), committer.through...)
	committer.mu.Unlock()
	if len(committedThrough) != 1 || committedThrough[0] != 11 {
		t.Fatalf("durable commits = %v, want [11]", committedThrough)
	}
	sr.mu.Lock()
	durable := sr.headSnapshot
	sr.mu.Unlock()
	if durable == nil || durable.Slot != 11 || durable.AlpenglowBlockID != executedID {
		t.Fatalf("durable identity = %+v, want finalized block %s at slot 11", durable, executedID)
	}
}

func TestCertifiedSkipDropsExecutedTailBeforePromotion(t *testing.T) {
	sr, stream, lastSlotCtx := speculativeTailForDecisionTest(t, solana.Hash{3})
	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 10 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{Slot: 11, Kind: alpenglow.ChainDecisionKindSkip}, true
	}

	switched, err := sr.ReconcileCertifiedTail(decisionSource, SpeculativeRollbackParams{
		ReplayCtx:      &ReplayCtx{},
		LastSlotCtx:    lastSlotCtx,
		BlockStream:    stream,
		RestoreSysvars: func(*ReplayHeadSnapshot) error { return nil },
		RestoreAux:     func(*ReplayHeadSnapshot) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !switched || sr.LayerCount() != 0 || (*lastSlotCtx).Slot != 10 {
		t.Fatalf("skip reconciliation switched=%v layers=%d head=%v", switched, sr.LayerCount(), *lastSlotCtx)
	}

	sr.mu.Lock()
	err = sr.advanceFinalityLocked(decisionSource)
	prefix := sr.store.PromotionPrefix(sr.finalityCursor, 0)
	sr.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(prefix) != 0 {
		t.Fatalf("certified skip left %d account layers eligible for promotion", len(prefix))
	}
}

func TestCertifiedAlternateBlockDropsExecutedTail(t *testing.T) {
	executedID := solana.Hash{3}
	selectedID := solana.Hash{4}
	sr, stream, lastSlotCtx := speculativeTailForDecisionTest(t, executedID)
	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 10 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Slot:  11,
			Kind:  alpenglow.ChainDecisionKindBlock,
			Block: alpenglow.BlockID{Slot: 11, Hash: selectedID},
		}, true
	}

	switched, err := sr.ReconcileCertifiedTail(decisionSource, SpeculativeRollbackParams{
		ReplayCtx:      &ReplayCtx{},
		LastSlotCtx:    lastSlotCtx,
		BlockStream:    stream,
		RestoreSysvars: func(*ReplayHeadSnapshot) error { return nil },
		RestoreAux:     func(*ReplayHeadSnapshot) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !switched || sr.LayerCount() != 0 || (*lastSlotCtx).Slot != 10 {
		t.Fatalf("block reconciliation switched=%v layers=%d head=%v", switched, sr.LayerCount(), *lastSlotCtx)
	}
}

func speculativeTailForDecisionTest(t *testing.T, executedID solana.Hash) (*SpeculativeReplay, *blockstream.BlockSource, **sealevel.SlotCtx) {
	t.Helper()
	sr := NewSpeculativeReplay()
	sr.Enable()
	sr.mu.Lock()
	sr.committedSlot = 10
	sr.finalityCursor = 10
	sr.headSnapshot = &ReplayHeadSnapshot{
		Slot: 10, ParentSlot: 9, FinalBankhash: make([]byte, 32), AcctsLtHash: &lthash.LtHash{},
		HasAlpenglowIdentity: true, AlpenglowBlockID: solana.Hash{1}, AlpenglowChainedRoot: solana.Hash{2},
	}
	sr.store.SetFinalizedSlot(10)
	sr.mu.Unlock()

	slotCtx := &sealevel.SlotCtx{Slot: 11, ParentSlot: 10, FinalBankhash: make([]byte, 32), AcctsLtHash: &lthash.LtHash{}, AcctMapsMu: &sync.Mutex{}}
	if err := sr.TrackPending(&DeferredBlockCommit{
		SlotCtx: slotCtx, BlockSlot: 11, BlockHeight: 11, Bankhash: make([]byte, 32),
		HasAlpenglowBlockID: true, AlpenglowBlockID: executedID,
		HasAlpenglowChainedRoot: true, AlpenglowChainedRoot: solana.Hash{5},
		ModifiedAccts: []*accounts.Account{{Key: solana.PublicKey{6}, Lamports: 1}},
	}, &ReplayCtx{}); err != nil {
		t.Fatal(err)
	}
	stream := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{
		SourceType: blockstream.BlockSourceTurbine, TurbineBindAddr: "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true, TurbineRepairOnly: true, StartSlot: 11, EndSlot: 20,
	})
	var lastSlotCtx *sealevel.SlotCtx
	return sr, stream, &lastSlotCtx
}
