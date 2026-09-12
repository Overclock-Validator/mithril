package consensus

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func TestInvalidationWinsDecisiveBlockIDPublicationRace(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	blockID := alpenglow.BlockID{Slot: 20, Hash: solana.Hash{20}}
	cert := alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarize,
		Slot:              blockID.Slot,
		BlockHash:         blockID.Hash,
		SignatureVerified: true,
		StakeVerified:     true,
	}
	if _, err := engine.ensureChain().ObserveCertificate(cert); err != nil {
		t.Fatal(err)
	}

	var published atomic.Int32
	engine.SetAlpenglowBlockIDSink(func(uint64, solana.Hash) { published.Add(1) })
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	engine.beforeBlockIDPublication = func() {
		once.Do(func() { close(reached) })
		<-release
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.observeVotorBlockID(alpenglow.NewCertificateMessage(cert))
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("publication did not reach deterministic barrier")
	}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(blockID, "invalid body"); err == nil {
		t.Fatal("decisively certified invalid block did not latch safety fault")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publication did not leave deterministic barrier")
	}
	if got := published.Load(); got != 0 {
		t.Fatalf("invalid block ID reached sink %d times", got)
	}
	engine.blockIDSinkMu.RLock()
	_, cached := engine.recentBlockIDs[blockID.Slot]
	engine.blockIDSinkMu.RUnlock()
	if cached {
		t.Fatal("invalid block ID entered publication cache")
	}

	// SetSink replays under the same tombstone lock and must not resurrect it.
	engine.SetAlpenglowBlockIDSink(func(uint64, solana.Hash) { published.Add(1) })
	if got := published.Load(); got != 0 {
		t.Fatalf("SetSink replay published invalid block ID %d times", got)
	}
}

func TestInvalidFallbackParentCorrectionSelectsSibling(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	low := alpenglow.BlockID{Slot: 3, Hash: solana.Hash{1}}
	high := alpenglow.BlockID{Slot: 3, Hash: solana.Hash{2}}
	for _, candidate := range []alpenglow.BlockID{low, high} {
		if _, accepted := engine.acceptVerifiedCertificate(alpenglow.Certificate{
			Type: alpenglow.CertificateNotarizeFallback, Slot: candidate.Slot, BlockHash: candidate.Hash,
			SignatureVerified: true, StakeVerified: true,
		}); !accepted {
			t.Fatalf("fallback certificate for %s was not accepted", candidate)
		}
	}
	if got := engine.AlpenglowBlockProductionParent(4); got.Kind != alpenglow.BlockProductionParentReady || got.Parent != low {
		t.Fatalf("pre-invalidation parent = %+v, want %v", got, low)
	}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(low, "duplicate transaction messages"); err != nil {
		t.Fatal(err)
	}
	if got := engine.AlpenglowBlockProductionParent(4); got.Kind != alpenglow.BlockProductionParentReady || got.Parent != high {
		t.Fatalf("corrected parent = %+v, want %v", got, high)
	}
}

func TestInvalidationCascadesPoolTombstonesToObservedDescendants(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	parent := alpenglow.BlockID{Slot: 2, Hash: solana.Hash{2}}
	child := alpenglow.BlockID{Slot: 3, Hash: solana.Hash{1}}
	sibling := alpenglow.BlockID{Slot: 3, Hash: solana.Hash{3}}
	engine.ObserveAlpenglowCandidateBlock(alpenglow.ReplayBlockObservation{
		Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash,
	})
	for _, candidate := range []alpenglow.BlockID{child, sibling} {
		engine.acceptVerifiedCertificate(alpenglow.Certificate{
			Type: alpenglow.CertificateNotarizeFallback, Slot: candidate.Slot, BlockHash: candidate.Hash,
			SignatureVerified: true, StakeVerified: true,
		})
	}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(parent, "invalid body"); err != nil {
		t.Fatal(err)
	}
	if !engine.ensureChain().IsObjectivelyInvalidBlock(child) {
		t.Fatal("chain did not cascade invalidity to observed child")
	}
	if got := engine.AlpenglowBlockProductionParent(4); got.Kind != alpenglow.BlockProductionParentReady || got.Parent != sibling {
		t.Fatalf("pool retained cascaded invalid child: parent=%+v want sibling=%v", got, sibling)
	}
}

func TestObserveBlockDropsChildOfInvalidParentBeforeVoterStaging(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	parent := alpenglow.BlockID{Slot: 10, Hash: solana.Hash{10}}
	child := alpenglow.BlockID{Slot: 11, Hash: solana.Hash{11}}
	sibling := alpenglow.BlockID{Slot: 11, Hash: solana.Hash{12}}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(parent, "invalid body"); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []alpenglow.BlockID{child, sibling} {
		engine.acceptVerifiedCertificate(alpenglow.Certificate{
			Type: alpenglow.CertificateNotarizeFallback, Slot: candidate.Slot, BlockHash: candidate.Hash,
			SignatureVerified: true, StakeVerified: true,
		})
	}
	err = engine.ObserveBlock(context.Background(), BlockObservation{Block: &block.Block{
		Slot:                      child.Slot,
		SourceParentSlot:          parent.Slot,
		AlpenglowBlockID:          [32]byte(child.Hash),
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    [32]byte(parent.Hash),
		HasAlpenglowParentBlockID: true,
	}})
	if !errors.Is(err, ErrObjectivelyInvalidAlpenglowBlock) {
		t.Fatalf("invalid child replay error = %v, want %v", err, ErrObjectivelyInvalidAlpenglowBlock)
	}
	if !engine.ensureChain().IsObjectivelyInvalidBlock(child) {
		t.Fatal("invalid parent did not tombstone replay child")
	}
	engine.observedReplayMu.Lock()
	_, staged := engine.observedReplayBlocks[child.Slot]
	engine.observedReplayMu.Unlock()
	if staged {
		t.Fatal("invalid child was staged for OnReplayResult/voter")
	}
	if got := engine.ensureObserver().Snapshot().ReplayBlocksObserved; got != 0 {
		t.Fatalf("invalid child reached observer history: %d", got)
	}
	if got := engine.AlpenglowBlockProductionParent(12); got.Kind != alpenglow.BlockProductionParentReady || got.Parent != sibling {
		t.Fatalf("parent-first cascade retained invalid child in pool: got %+v want %v", got, sibling)
	}
}

func TestVoterRejectsStaleInvalidParentReadyHistory(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	parent := alpenglow.BlockID{Slot: 3, Hash: solana.Hash{3}}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(parent, "invalid body"); err != nil {
		t.Fatal(err)
	}
	voter := &alpenglowVoter{
		engine:  engine,
		history: alpenglow.NewVoteHistory(solana.PublicKey{}, 0),
		pending: make(map[uint64][]pendingVotorBlock),
	}
	event := alpenglow.ConsensusEvent{Kind: alpenglow.ConsensusEventParentReady, Slot: 4, Block: parent}
	if err := voter.handle(voterEvent{kind: voterEventConsensus, consensus: event}); err != nil {
		t.Fatal(err)
	}
	if voter.history.IsParentReady(4, parent) {
		t.Fatal("stale queued invalid ParentReady entered history")
	}
	// Preserve an already-consumed historical edge, but prove it cannot
	// authorize a future child after the parent is tombstoned.
	voter.history.AddParentReady(4, parent)
	child := alpenglow.BlockID{Slot: 4, Hash: solana.Hash{4}}
	if voted, err := voter.tryNotar(child, parent); err != nil || voted {
		t.Fatalf("stale invalid parent authorized child: voted=%v err=%v", voted, err)
	}
}

func TestFinalVoteGuardSeesInvalidationAfterEligibilityCheck(t *testing.T) {
	identity := voterTestKey(81)
	authorized := voterTestKey(82)
	voteAccount := solana.PublicKey(voterTestKey(83).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	blockID := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	if err := engine.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	engine.SetAlpenglowRoot(root)

	voter, err := newAlpenglowVoter(engine, VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		ReadyToVote:     func(uint64) bool { return true },
	}, root)
	if err != nil {
		t.Fatal(err)
	}
	voter.closeOnce.Do(func() { close(voter.done) })
	voter.wg.Wait()
	t.Cleanup(func() { _ = voter.broadcaster.Close() })
	voter.sets[set.Epoch] = cloneValidatorSet(set)
	voter.history.AddParentReady(blockID.Slot, root)
	if err := voter.history.AddVote(alpenglow.NewNotarizationVote(blockID.Slot, blockID.Hash)); err != nil {
		t.Fatal(err)
	}
	voter.history.AddBlockNotarized(blockID)

	reached := make(chan struct{})
	release := make(chan struct{})
	voter.beforeVoteGuard = func(guarded alpenglow.BlockID) {
		if guarded != blockID {
			t.Errorf("final vote guard = %v, want %v", guarded, blockID)
		}
		close(reached)
		<-release
	}
	type outcome struct {
		voted bool
		err   error
	}
	result := make(chan outcome, 1)
	go func() {
		voted, err := voter.tryFinal(blockID)
		result <- outcome{voted: voted, err: err}
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("final vote did not reach its invalidation guard")
	}
	if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(blockID, "invalid body"); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case got := <-result:
		if got.err != nil || got.voted {
			t.Fatalf("final vote survived prior invalidation: voted=%v err=%v", got.voted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("final vote guard deadlocked")
	}
	if voter.history.IsOver(blockID.Slot) {
		t.Fatal("invalid block finalization was persisted in vote history")
	}
}

func TestInvalidBlockPublicationTombstonesPruneAtDurableRoot(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatal(err)
	}
	old := alpenglow.BlockID{Slot: 10, Hash: solana.Hash{10}}
	root := alpenglow.BlockID{Slot: 11, Hash: solana.Hash{11}}
	for _, candidate := range []alpenglow.BlockID{old, root} {
		if err := engine.ObserveObjectivelyInvalidAlpenglowBlock(candidate, "invalid body"); err != nil {
			t.Fatal(err)
		}
	}
	engine.PruneAlpenglowBefore(root.Slot)
	engine.blockIDSinkMu.RLock()
	_, oldRetained := engine.invalidBlockIDs[old]
	_, rootRetained := engine.invalidBlockIDs[root]
	engine.blockIDSinkMu.RUnlock()
	if oldRetained || !rootRetained {
		t.Fatalf("invalid publication tombstones after prune: old=%v root=%v", oldRetained, rootRetained)
	}
}
