package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestChainGenerationLeaseSerializesResetAndLocalPublication(t *testing.T) {
	ResetChainTip()
	t.Cleanup(ResetChainTip)
	InitChainTip(nil, nil, 0, solana.Hash{})
	generation := ChainTipParentContext().Generation
	release, ok := AcquireChainTipGeneration(generation)
	require.True(t, ok)
	resetStarted, resetDone := make(chan struct{}), make(chan struct{})
	go func() {
		close(resetStarted)
		ResetChainTip()
		close(resetDone)
	}()
	<-resetStarted
	select {
	case <-resetDone:
		t.Fatal("reset crossed a producer's final publication lease")
	case <-time.After(10 * time.Millisecond):
	}
	// The producer registers its handoff before releasing the lease. Reset
	// must clear that registration after joining, not clear too early.
	RegisterLocalLeaderCommit(&sealevel.SlotCtx{Slot: 123})
	release()
	<-resetDone
	_, exists := TakeLocalLeaderCommit(123)
	require.False(t, exists)
	staleRelease, ok := AcquireChainTipGeneration(generation)
	require.False(t, ok)
	require.Nil(t, staleRelease)
	zeroRelease, ok := AcquireChainTipGeneration(0)
	require.False(t, ok)
	require.Nil(t, zeroRelease)
}

func TestReplayInvalidationRetainsFrozenLocalCommit(t *testing.T) {
	ResetChainTip()
	t.Cleanup(ResetChainTip)
	InitChainTip(nil, nil, 0, solana.Hash{})
	generation := ChainTipParentContext().Generation
	bank := &sealevel.SlotCtx{Slot: 124}
	RegisterLocalLeaderCommit(bank)
	InvalidateChainTipForReplay()
	release, ok := AcquireChainTipGeneration(generation)
	require.False(t, ok)
	require.Nil(t, release)
	commit, exists := TakeLocalLeaderCommit(bank.Slot)
	require.True(t, exists, "accepting the producer block must keep its frozen bank")
	require.Same(t, bank, commit.SlotCtx)
	require.Zero(t, ChainTipParentContext().Slot)
}

func TestReplayInvalidationRetainsAcceptedFeaturesForTPUAdmission(t *testing.T) {
	ResetChainTip()
	t.Cleanup(ResetChainTip)
	acceptedFeatures := features.NewFeaturesDefault()
	acceptedFeatures.EnableFeature(features.EnableTxV1, 100)
	UpdateChainTipFromSlotCtx(&sealevel.SlotCtx{Slot: 123}, acceptedFeatures, NewTransactionStatusCache().View(), ChainTipIdentity{})
	before := ChainTipParentContext()
	require.True(t, ChainTipFeatureActive(features.EnableTxV1))
	InvalidateChainTipForReplay()
	require.True(t, ChainTipFeatureActive(features.EnableTxV1), "replay must not disable TPU transaction versions while processing a child")
	require.True(t, ChainTipFeatures().IsActive(features.EnableTxV1))
	after := ChainTipParentContext()
	require.Greater(t, after.Generation, before.Generation)
	require.Zero(t, after.Slot)
	require.Zero(t, after.Bankhash)
	require.Nil(t, after.BankSysvars)
	require.Nil(t, after.UnrootedRead)
	require.Nil(t, after.TransactionStatuses)
	require.True(t, after.Features.IsActive(features.EnableTxV1))
	ResetChainTip()
	require.False(t, ChainTipFeatureActive(features.EnableTxV1), "real rewind clears features until the selected parent is restored")
}
