package replay

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

type testFoldCommitter struct {
	mu      sync.Mutex
	through []uint64
	started chan struct{}
	release chan struct{}
}

func (f *testFoldCommitter) CommitBatch(
	deltas []accounts.SlotDelta,
	through uint64,
	_ map[uint64][32]byte,
	_ []byte,
) (accountsdb.BatchCommitResult, error) {
	if f.started != nil {
		select {
		case <-f.started:
		default:
			close(f.started)
		}
	}
	if f.release != nil {
		<-f.release
	}
	f.mu.Lock()
	f.through = append(f.through, through)
	f.mu.Unlock()
	return accountsdb.BatchCommitResult{ThroughSlot: through, Keys: len(deltas)}, nil
}

func newFoldTestReplay(t *testing.T, committer batchCommitter, batchSlots int) *SpeculativeReplay {
	t.Helper()
	sr := NewSpeculativeReplay()
	sr.Enable()
	sr.mu.Lock()
	sr.committedSlot = 10
	sr.finalityCursor = 10
	sr.headSnapshot = foldTestSnapshot(10, 9)
	sr.store.SetFinalizedSlot(10)
	sr.configureCommitterLocked(committer, nil, "", batchSlots)
	sr.mu.Unlock()
	t.Cleanup(sr.Close)
	return sr
}

func foldTestSnapshot(slot, parent uint64) *ReplayHeadSnapshot {
	var bankhash [32]byte
	bankhash[0] = byte(slot)
	return &ReplayHeadSnapshot{
		Slot:           slot,
		ParentSlot:     parent,
		BlockHeight:    slot,
		Epoch:          1,
		FinalBankhash:  append([]byte(nil), bankhash[:]...),
		Blockhash:      bankhash,
		AcctsLtHash:    &lthash.LtHash{},
		Capitalization: 1,
	}
}

func addFoldTestPending(t *testing.T, sr *SpeculativeReplay, slot, parent uint64) solana.Hash {
	t.Helper()
	blockID := speculativeTestHash(byte(slot))
	var bankhash [32]byte
	bankhash[0] = byte(slot)
	account := &accounts.Account{Key: solana.PublicKey{byte(slot)}, Lamports: slot}
	slotCtx := &sealevel.SlotCtx{
		Slot:          slot,
		ParentSlot:    parent,
		Epoch:         1,
		FinalBankhash: append([]byte(nil), bankhash[:]...),
		Blockhash:     bankhash,
		AcctsLtHash:   &lthash.LtHash{},
		AcctMapsMu:    &sync.Mutex{},
	}
	if err := sr.TrackPending(&DeferredBlockCommit{
		SlotCtx:                 slotCtx,
		ModifiedAccts:           []*accounts.Account{account},
		BlockSlot:               slot,
		BlockHeight:             slot,
		Bankhash:                append([]byte(nil), bankhash[:]...),
		HasAlpenglowBlockID:     true,
		AlpenglowBlockID:        blockID,
		HasAlpenglowChainedRoot: true,
		AlpenglowChainedRoot:    speculativeTestHash(byte(slot + 1)),
	}, &ReplayCtx{Capitalization: 1}); err != nil {
		t.Fatal(err)
	}
	sr.mu.Lock()
	snapshot := sr.snapshots[slot]
	clock := sealevel.SysvarClock{Slot: slot, Epoch: 1}
	recent := sealevel.SysvarRecentBlockhashes{{
		Blockhash:     bankhash,
		FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5_000},
	}}
	slotHashes := sealevel.SysvarSlotHashes{{Slot: slot, Hash: bankhash}}
	snapshot.Clock = &clock
	snapshot.RecentBlockhashes = &recent
	snapshot.SlotHashes = &slotHashes
	sr.mu.Unlock()
	return blockID
}

func TestAsyncFoldRetainsOverlayUntilCommitCompletes(t *testing.T) {
	committer := &testFoldCommitter{started: make(chan struct{}), release: make(chan struct{})}
	sr := newFoldTestReplay(t, committer, 2)
	var durableRoots []uint64
	sr.SetDurableRootSink(func(slot uint64) { durableRoots = append(durableRoots, slot) })
	id11 := addFoldTestPending(t, sr, 11, 10)
	id12 := addFoldTestPending(t, sr, 12, 11)
	decisions := map[uint64]alpenglow.ChainDecision{
		10: {Slot: 11, Kind: alpenglow.ChainDecisionKindBlock, Block: alpenglow.BlockID{Slot: 11, Hash: id11}, CertificateType: alpenglow.CertificateFinalizeFast},
		11: {Slot: 12, Kind: alpenglow.ChainDecisionKindBlock, Block: alpenglow.BlockID{Slot: 12, Hash: id12}, CertificateType: alpenglow.CertificateFinalizeFast},
	}
	source := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		decision, ok := decisions[anchor]
		return decision, ok
	}

	sr.TryFlushPending(nil, &persistedTracker{}, nil, source)
	select {
	case <-committer.started:
	case <-time.After(time.Second):
		t.Fatal("fold worker did not start")
	}
	if got := sr.LayerCount(); got != 2 {
		t.Fatalf("layers while fold is in flight = %d, want 2", got)
	}
	if got, _ := sr.DurableHead(); got != 10 {
		t.Fatalf("durable slot advanced before commit: %d", got)
	}
	if len(durableRoots) != 0 {
		t.Fatalf("durable root published before commit: %v", durableRoots)
	}

	close(committer.release)
	pt := &persistedTracker{}
	if err := sr.FlushFinalized(pt, source); err != nil {
		t.Fatal(err)
	}
	if got := sr.LayerCount(); got != 0 {
		t.Fatalf("layers after fold = %d, want 0", got)
	}
	if got, _ := sr.DurableHead(); got != 12 {
		t.Fatalf("durable slot after fold = %d, want 12", got)
	}
	if slot, _ := pt.Get(); slot != 12 {
		t.Fatalf("persisted tracker slot = %d, want 12", slot)
	}
	if len(durableRoots) != 1 || durableRoots[0] != 12 {
		t.Fatalf("durable roots after commit = %v, want [12]", durableRoots)
	}
}

func TestFoldFinalityWalkCrossesCertifiedSkip(t *testing.T) {
	committer := &testFoldCommitter{}
	sr := newFoldTestReplay(t, committer, 1)
	id12 := addFoldTestPending(t, sr, 12, 10)
	source := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		switch anchor {
		case 10:
			return alpenglow.ChainDecision{Slot: 11, Kind: alpenglow.ChainDecisionKindSkip, CertificateType: alpenglow.CertificateSkip}, true
		case 11:
			return alpenglow.ChainDecision{
				Slot:            12,
				Kind:            alpenglow.ChainDecisionKindBlock,
				Block:           alpenglow.BlockID{Slot: 12, Hash: id12},
				CertificateType: alpenglow.CertificateFinalizeFast,
			}, true
		default:
			return alpenglow.ChainDecision{}, false
		}
	}

	sr.TryFlushPending(nil, &persistedTracker{}, nil, source)
	if err := sr.FlushFinalized(&persistedTracker{}, source); err != nil {
		t.Fatal(err)
	}
	if got, _ := sr.DurableHead(); got != 12 {
		t.Fatalf("durable slot = %d, want 12 after certified skip", got)
	}
}

func setFoldTestRewardState(t *testing.T, sr *SpeculativeReplay, slot, spoolSlot, partitions, remaining uint64) {
	t.Helper()
	sr.mu.Lock()
	defer sr.mu.Unlock()
	snapshot := sr.snapshots[slot]
	if snapshot == nil {
		t.Fatalf("slot %d has no snapshot", slot)
	}
	snapshot.Aux = ReplayAuxState{
		PartitionedEpochRewardsEnabled: true,
		PartitionedRewardsInfo: &rewards.PartitionedRewardDistributionInfo{
			SpoolSlot:                    spoolSlot,
			NumRewardPartitions:          partitions,
			NumRewardPartitionsRemaining: remaining,
		},
	}
}

func TestFoldForcesBoundaryBeforePartitionedRewards(t *testing.T) {
	sr := newFoldTestReplay(t, &testFoldCommitter{}, 4)
	addFoldTestPending(t, sr, 11, 10)
	addFoldTestPending(t, sr, 12, 11)
	addFoldTestPending(t, sr, 13, 12)
	setFoldTestRewardState(t, sr, 12, 12, 3, 3)
	setFoldTestRewardState(t, sr, 13, 12, 3, 2)

	sr.mu.Lock()
	sr.finalityCursor = 13
	job, err := sr.buildFoldJobLocked(false)
	sr.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.through != 11 || len(job.deltas) != 1 {
		t.Fatalf("boundary fold = %+v, want one-slot fold through 11", job)
	}
}

func TestFoldDoesNotPersistIncompletePartitionedRewards(t *testing.T) {
	sr := newFoldTestReplay(t, &testFoldCommitter{}, 2)
	addFoldTestPending(t, sr, 11, 10)
	addFoldTestPending(t, sr, 12, 11)
	setFoldTestRewardState(t, sr, 11, 11, 3, 3)
	setFoldTestRewardState(t, sr, 12, 11, 3, 2)

	sr.mu.Lock()
	sr.finalityCursor = 12
	job, err := sr.buildFoldJobLocked(true)
	sr.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatalf("incomplete rewards window produced fold through %d", job.through)
	}
}

func TestFoldPersistsCompletedPartitionedRewardsAtomically(t *testing.T) {
	sr := newFoldTestReplay(t, &testFoldCommitter{}, 2)
	addFoldTestPending(t, sr, 11, 10)
	addFoldTestPending(t, sr, 12, 11)
	addFoldTestPending(t, sr, 13, 12)
	setFoldTestRewardState(t, sr, 11, 11, 3, 3)
	setFoldTestRewardState(t, sr, 12, 11, 3, 2)
	setFoldTestRewardState(t, sr, 13, 11, 3, 0)

	sr.mu.Lock()
	sr.finalityCursor = 13
	job, err := sr.buildFoldJobLocked(false)
	sr.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.through != 13 || len(job.deltas) != 3 {
		t.Fatalf("completed rewards fold = %+v, want atomic three-slot fold through 13", job)
	}
}

func TestSuccessfulFoldCleansCompletedRewardSpool(t *testing.T) {
	dir := t.TempDir()
	const spoolSlot = uint64(8)
	const partitions = uint64(3)
	for partition := uint64(0); partition < partitions; partition++ {
		path := filepath.Join(dir, fmt.Sprintf("reward_spool_%d_p%d.bin", spoolSlot, partition))
		if err := os.WriteFile(path, []byte("reward"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := foldTestSnapshot(12, 11)
	snapshot.Aux = ReplayAuxState{
		PartitionedEpochRewardsEnabled: true,
		PartitionedRewardsInfo: &rewards.PartitionedRewardDistributionInfo{
			SpoolDir:                     dir,
			SpoolSlot:                    spoolSlot,
			NumRewardPartitions:          partitions,
			NumRewardPartitionsRemaining: 0,
		},
	}
	sr := NewSpeculativeReplay()
	sr.applyFoldResultLocked(nil, speculativeFoldResult{job: &speculativeFoldJob{
		through:  12,
		bankhash: make([]byte, 32),
		snapshot: snapshot,
	}})

	for partition := uint64(0); partition < partitions; partition++ {
		path := filepath.Join(dir, fmt.Sprintf("reward_spool_%d_p%d.bin", spoolSlot, partition))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("reward spool partition %d was not removed: %v", partition, err)
		}
	}
}

func TestFoldResumeContextCarriesEpochConsensusMetadata(t *testing.T) {
	snapshot := foldTestSnapshot(12, 11)
	snapshot.HasAlpenglowIdentity = true
	snapshot.AlpenglowBlockID = solana.Hash{1}
	snapshot.AlpenglowChainedRoot = solana.Hash{2}
	snapshot.EpochResume = &EpochResumeMetadata{
		ComputedEpochStakes: map[uint64]string{7: "stakes"},
		AuthorizedVoters:    map[string][]string{"vote": {"voter"}},
	}

	resume, err := resumeContextFromSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if resume.ComputedEpochStakes[7] != "stakes" || len(resume.EpochAuthorizedVoters["vote"]) != 1 {
		t.Fatalf("epoch metadata missing from fold context: %+v", resume)
	}
}

func TestFoldWaitsForLocalReplaySnapshot(t *testing.T) {
	committer := &testFoldCommitter{}
	sr := newFoldTestReplay(t, committer, 1)
	blockID := addFoldTestPending(t, sr, 11, 10)
	sr.mu.Lock()
	sr.pending[11].AwaitingReplaySnapshot = true
	sr.pending[11].CapturedSnapshot = sr.snapshots[11]
	sr.mu.Unlock()

	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 10 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Slot:            11,
			Kind:            alpenglow.ChainDecisionKindBlock,
			Block:           alpenglow.BlockID{Slot: 11, Hash: blockID},
			CertificateType: alpenglow.CertificateFinalizeFast,
		}, true
	}

	sr.TryFlushPending(nil, &persistedTracker{}, nil, decisionSource)
	if err := sr.Err(); err != nil {
		t.Fatalf("early finalization made fold unhealthy: %v", err)
	}
	committer.mu.Lock()
	commitsBeforeSnapshot := len(committer.through)
	committer.mu.Unlock()
	if commitsBeforeSnapshot != 0 {
		t.Fatalf("fold committed before local replay snapshot: %d", commitsBeforeSnapshot)
	}

	if err := sr.FinalizePendingSnapshot(11, &ReplayCtx{Capitalization: 1}); err != nil {
		t.Fatal(err)
	}
	if err := sr.FlushFinalized(&persistedTracker{}, decisionSource); err != nil {
		t.Fatal(err)
	}
	committer.mu.Lock()
	committed := append([]uint64(nil), committer.through...)
	committer.mu.Unlock()
	if len(committed) != 1 || committed[0] != 11 {
		t.Fatalf("commits after local replay snapshot = %v, want [11]", committed)
	}
}
