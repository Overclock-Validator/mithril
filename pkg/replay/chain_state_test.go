package replay

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestChainTipTracksReplayedSlot(t *testing.T) {
	t.Cleanup(ResetChainTip)
	parentLtHash := new(lthash.LtHash).InitWithHash(make([]byte, 2048))
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.AccountsLtHash, 1)
	InitChainTip(parentLtHash, feats, 10, solana.Hash{})
	initialGeneration := ChainTipParentContext().Generation

	updated := new(lthash.LtHash).InitWithHash(make([]byte, 2048))
	updated.Add(parentLtHash)
	epochStakeKey := solana.PublicKey{8}
	nanoClock := &accounts.Account{Key: NanosecondClockAccountAddr(), Lamports: 1, Data: make([]byte, 8)}
	binary.LittleEndian.PutUint64(nanoClock.Data, 1234)
	clock := sealevel.SysvarClock{Slot: 200, UnixTimestamp: 5678}
	bankSysvars, err := sealevel.NewBankSysvars(200, &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     clock.MustMarshal(),
	})
	require.NoError(t, err)
	slotCtx := &sealevel.SlotCtx{
		Slot:                   200,
		Accounts:               accounts.NewMemAccounts(),
		NumSignatures:          11,
		AcctsLtHash:            updated,
		Features:               feats,
		FinalBankhash:          append([]byte{7}, make([]byte, 31)...),
		Blockhash:              solana.Hash{9},
		LatestEvictedBlockhash: [32]byte{6},
		VoteAccts:              map[solana.PublicKey]uint64{epochStakeKey: 55},
		TotalEpochStake:        99,
	}
	require.NoError(t, slotCtx.PublishBankSysvars(bankSysvars))
	require.NoError(t, slotCtx.SetAccount(nanoClock.Key, nanoClock))
	statuses := NewTransactionStatusCache().View()
	identity := ChainTipIdentity{
		AlpenglowBlockID:              solana.Hash{3},
		HasAlpenglowBlockID:           true,
		AlpenglowChainedMerkleRoot:    solana.Hash{4},
		HasAlpenglowChainedMerkleRoot: true,
	}
	UpdateChainTipFromSlotCtxWithBankMetadata(slotCtx, feats, statuses, identity, ChainTipBankMetadata{BlockHeight: 190})
	binary.LittleEndian.PutUint64(nanoClock.Data, 9999)

	tip := ChainTipParentContext()
	require.Greater(t, tip.Generation, initialGeneration)
	require.Equal(t, uint64(200), tip.Slot)
	require.Equal(t, solana.Hash{7}, tip.Bankhash)
	require.Equal(t, identity.AlpenglowBlockID, tip.AlpenglowBlockID)
	require.True(t, tip.HasAlpenglowBlockID)
	require.Equal(t, identity.AlpenglowChainedMerkleRoot, tip.AlpenglowChainedMerkleRoot)
	require.True(t, tip.HasAlpenglowChainedMerkleRoot)
	require.Equal(t, solana.Hash{9}, tip.LastEntryHash)
	require.Equal(t, solana.Hash{9}, tip.LastBlockhash)
	require.Equal(t, uint64(190), tip.BlockHeight)
	require.Equal(t, [32]byte{6}, tip.LatestEvictedBlockhash)
	require.Equal(t, uint64(55), tip.EpochStakes[epochStakeKey])
	require.Equal(t, uint64(99), tip.TotalEpochStake)
	require.True(t, tip.HasNanosecondClockAccount)
	require.NotNil(t, tip.NanosecondClockAccount)
	require.Equal(t, uint64(1234), binary.LittleEndian.Uint64(tip.NanosecondClockAccount.Data))
	require.Same(t, bankSysvars, tip.BankSysvars)
	gotClock, ok := tip.BankSysvars.Clock()
	require.True(t, ok)
	require.Equal(t, clock, gotClock)
	require.Equal(t, uint64(11), tip.PrevNumSigs)
	require.NotNil(t, tip.AcctsLtHash)
	require.True(t, tip.AcctsLtHash.Equals(updated))
	require.True(t, tip.Features.IsActive(features.AccountsLtHash))
	require.Same(t, statuses, tip.TransactionStatuses)
}

func TestResetChainTipClearsTransactionStatuses(t *testing.T) {
	statuses := NewTransactionStatusCache().View()
	UpdateChainTipFromSlotCtx(&sealevel.SlotCtx{Slot: 1}, nil, statuses, ChainTipIdentity{
		AlpenglowBlockID:              solana.Hash{1},
		HasAlpenglowBlockID:           true,
		AlpenglowChainedMerkleRoot:    solana.Hash{2},
		HasAlpenglowChainedMerkleRoot: true,
	})
	before := ChainTipParentContext()
	require.Same(t, statuses, before.TransactionStatuses)

	ResetChainTip()
	after := ChainTipParentContext()
	require.Greater(t, after.Generation, before.Generation)
	require.Nil(t, after.TransactionStatuses)
	require.False(t, after.HasAlpenglowBlockID)
	require.False(t, after.HasAlpenglowChainedMerkleRoot)
	require.Nil(t, after.BankSysvars)
	require.Nil(t, after.EpochStakes)
	require.Zero(t, after.TotalEpochStake)
	require.Zero(t, after.BlockHeight)
	require.Zero(t, after.LastBlockhash)
	require.Zero(t, after.LatestEvictedBlockhash)
	require.False(t, after.HasNanosecondClockAccount)
	require.Nil(t, after.NanosecondClockAccount)
}

func TestChainTipPreservesPrefundedNanosecondClockAccount(t *testing.T) {
	t.Cleanup(ResetChainTip)
	nanoClock := &accounts.Account{
		Key:      NanosecondClockAccountAddr(),
		Lamports: 42,
		Owner:    [32]byte{7},
		// An empty data payload is valid before the first footer populates the
		// known PDA and is still part of the AccountsLtHash before-image.
	}
	slotCtx := &sealevel.SlotCtx{Slot: 12, Accounts: accounts.NewMemAccounts()}
	require.NoError(t, slotCtx.SetAccount(nanoClock.Key, nanoClock))
	UpdateChainTipFromSlotCtx(slotCtx, nil, nil, ChainTipIdentity{})

	tip := ChainTipParentContext()
	require.True(t, tip.HasNanosecondClockAccount)
	require.NotNil(t, tip.NanosecondClockAccount)
	require.Equal(t, nanoClock.Lamports, tip.NanosecondClockAccount.Lamports)
	require.Equal(t, nanoClock.Owner, tip.NanosecondClockAccount.Owner)
	require.Empty(t, tip.NanosecondClockAccount.Data)
}

func TestInitChainTipFailsClosedWithoutCompleteReplayParent(t *testing.T) {
	t.Cleanup(ResetChainTip)
	statuses := NewTransactionStatusCache().View()
	InitChainTip(nil, nil, 0, solana.Hash{}, statuses)

	tip := ChainTipParentContext()
	require.NotZero(t, tip.Generation)
	require.Same(t, statuses, tip.TransactionStatuses)
	require.Zero(t, tip.Slot)
	require.Zero(t, tip.Bankhash)
	require.False(t, tip.HasAlpenglowBlockID)
	require.False(t, tip.HasAlpenglowChainedMerkleRoot)
	require.Nil(t, tip.PrevFeeGovernor)
}

func TestChainTipFeatureActiveTracksPublishedTip(t *testing.T) {
	t.Cleanup(ResetChainTip)
	ResetChainTip()
	require.False(t, ChainTipFeatureActive(features.EnableTxV1))

	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.EnableTxV1, 42)
	InitChainTip(nil, feats, 0, solana.Hash{})
	require.True(t, ChainTipFeatureActive(features.EnableTxV1))

	feats.DisableFeature(features.EnableTxV1)
	// InitChainTip publishes a clone, so caller mutation cannot race with or
	// alter the admission view.
	require.True(t, ChainTipFeatureActive(features.EnableTxV1))
	ResetChainTip()
	require.False(t, ChainTipFeatureActive(features.EnableTxV1))
}

func TestChainTipFeatureActiveConcurrentPublication(t *testing.T) {
	t.Cleanup(ResetChainTip)
	active := features.NewFeaturesDefault()
	active.EnableFeature(features.EnableTxV1, 1)
	inactive := features.NewFeaturesDefault()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1_000; i++ {
			if i%2 == 0 {
				InitChainTip(nil, active, 0, solana.Hash{})
			} else {
				InitChainTip(nil, inactive, 0, solana.Hash{})
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			_ = ChainTipFeatureActive(features.EnableTxV1)
		}
	}()
	wg.Wait()
}
