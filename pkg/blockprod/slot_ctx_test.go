package blockprod

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type panicLeaderAccountReader struct{}

func (panicLeaderAccountReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	panic("leader preparation consulted mutable parent account reader")
}

func leaderTestEpochScheduleAccount(t *testing.T, schedule sealevel.SysvarEpochSchedule) *accounts.Account {
	t.Helper()
	var data bytes.Buffer
	enc := bin.NewBinEncoder(&data)
	require.NoError(t, enc.WriteUint64(schedule.SlotsPerEpoch, bin.LE))
	require.NoError(t, enc.WriteUint64(schedule.LeaderScheduleSlotOffset, bin.LE))
	require.NoError(t, enc.WriteBool(schedule.Warmup))
	require.NoError(t, enc.WriteUint64(schedule.FirstNormalEpoch, bin.LE))
	require.NoError(t, enc.WriteUint64(schedule.FirstNormalSlot, bin.LE))
	return &accounts.Account{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1, Data: data.Bytes()}
}

func TestNewLeaderSlotCtxInheritsAcctsLtHashAndFeatures(t *testing.T) {
	parentLtHash := new(lthash.LtHash).InitWithHash(make([]byte, 2048))
	parentFeatures := features.NewFeaturesDefault()
	parentFeatures.EnableFeature(features.AccountsLtHash, 1)
	parentFeatures.EnableFeature(features.RemoveAccountsDeltaHash, 2)

	epochSchedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch: 54000,
	}
	slotCtx, err := NewLeaderSlotCtx(100, 99, nil, ParentContext{
		PrevNumSigs:            42,
		AcctsLtHash:            parentLtHash,
		Features:               parentFeatures,
		ParentLastBlockhash:    solana.Hash{7},
		LatestEvictedBlockhash: [32]byte{8},
		EpochStakes:            map[solana.PublicKey]uint64{solana.PublicKey{9}: 10},
		TotalEpochStake:        10,
	}, epochSchedule)
	require.NoError(t, err)
	require.NotNil(t, slotCtx.AcctsLtHash)
	require.True(t, slotCtx.AcctsLtHash.Equals(parentLtHash))
	require.True(t, slotCtx.Features.IsActive(features.AccountsLtHash))
	require.True(t, slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash))
	require.False(t, slotCtx.Features.IsActive(features.FormalizeLoadedTransactionDataSize))
	// Leader feature state is an independent, exact clone of its parent bank;
	// locally enabling a child feature must not mutate the replay parent.
	slotCtx.Features.EnableFeature(features.FormalizeLoadedTransactionDataSize, 100)
	require.False(t, parentFeatures.IsActive(features.FormalizeLoadedTransactionDataSize))
	require.Equal(t, uint64(0), slotCtx.NumSignatures)
	require.Equal(t, uint64(0), slotCtx.Epoch) // slot 100 with default schedule
	require.Equal(t, [32]byte{7}, slotCtx.LastBlockhash)
	require.Equal(t, [32]byte{8}, slotCtx.LatestEvictedBlockhash)
	require.Equal(t, uint64(10), slotCtx.VoteAccts[solana.PublicKey{9}])
	require.Equal(t, uint64(10), slotCtx.TotalEpochStake)
}

func TestNewLeaderSlotCtxInstallsPinnedBankState(t *testing.T) {
	const parentSlot = uint64(99)
	clock := sealevel.SysvarClock{Slot: parentSlot, UnixTimestamp: 1234}
	clockAcct := &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     clock.MustMarshal(),
	}
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000}
	parentSysvars, err := sealevel.NewBankSysvars(parentSlot, clockAcct, leaderTestEpochScheduleAccount(t, schedule))
	require.NoError(t, err)

	slotCtx, err := NewLeaderSlotCtx(100, parentSlot, nil, ParentContext{
		BankSysvars:            parentSysvars,
		ParentLastBlockhash:    solana.Hash{7},
		LatestEvictedBlockhash: [32]byte{8},
	}, &schedule)
	require.NoError(t, err)
	require.NotNil(t, slotCtx.BankSysvars())
	require.Equal(t, uint64(100), slotCtx.BankSysvars().Slot())
	gotClock, ok := slotCtx.BankSysvars().Clock()
	require.True(t, ok)
	require.Equal(t, clock, gotClock)

	currentClock, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)
	parentClock, err := slotCtx.GetParentAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)
	require.Equal(t, clockAcct.Data, currentClock.Data)
	require.Equal(t, clockAcct.Data, parentClock.Data)
	require.NotSame(t, currentClock, parentClock)

	// Every absent cached sysvar and the optional alpenclock PDA are pinned as
	// tombstones, preventing fallback into a newer unrooted replay generation.
	for _, pubkey := range []solana.PublicKey{sealevel.SysvarRentAddr, replay.NanosecondClockAccountAddr()} {
		current, err := slotCtx.GetAccount(pubkey)
		require.NoError(t, err)
		require.Zero(t, current.Lamports)
		parent, err := slotCtx.GetParentAccount(pubkey)
		require.NoError(t, err)
		require.Zero(t, parent.Lamports)
	}
}

func TestNewLeaderSlotCtxInstallsPresentNanosecondClockIndependently(t *testing.T) {
	const parentSlot = uint64(99)
	clock := sealevel.SysvarClock{Slot: parentSlot, UnixTimestamp: 1234}
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000}
	parentSysvars, err := sealevel.NewBankSysvars(parentSlot, &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     clock.MustMarshal(),
	}, leaderTestEpochScheduleAccount(t, schedule))
	require.NoError(t, err)
	nanoClockAddr := replay.NanosecondClockAccountAddr()
	nanoClock := &accounts.Account{
		Key:      nanoClockAddr,
		Lamports: 42,
		Data:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}

	slotCtx, err := NewLeaderSlotCtx(100, parentSlot, nil, ParentContext{
		BankSysvars:               parentSysvars,
		NanosecondClockAccount:    nanoClock,
		HasNanosecondClockAccount: true,
	}, &schedule)
	require.NoError(t, err)

	current, err := slotCtx.GetAccount(nanoClockAddr)
	require.NoError(t, err)
	parent, err := slotCtx.GetParentAccount(nanoClockAddr)
	require.NoError(t, err)
	require.Equal(t, nanoClock.Data, current.Data)
	require.Equal(t, nanoClock.Data, parent.Data)

	// Updating the child account must leave the pinned parent before-image and
	// the published ChainTip copy untouched.
	current.Data[0] = 9
	require.NoError(t, slotCtx.SetAccount(nanoClockAddr, current))
	parent, err = slotCtx.GetParentAccount(nanoClockAddr)
	require.NoError(t, err)
	require.Equal(t, byte(1), parent.Data[0])
	require.Equal(t, byte(1), nanoClock.Data[0])
}

func TestPrepareLeaderSlotSysvarsUsesPinnedParentSnapshot(t *testing.T) {
	const parentSlot = uint64(99)
	const childSlot = uint64(100)
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000, LeaderScheduleSlotOffset: 54_000}
	parentClock := sealevel.SysvarClock{Slot: parentSlot, Epoch: 0, UnixTimestamp: 1_234}
	parentSlotHashes := sealevel.SysvarSlotHashes{{Slot: 98, Hash: [32]byte{1}}}
	parentSysvars, err := sealevel.NewBankSysvars(parentSlot,
		&accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: parentClock.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: parentSlotHashes.MustMarshal()},
		leaderTestEpochScheduleAccount(t, schedule),
	)
	require.NoError(t, err)

	previousClock := sealevel.SysvarCache.Clock
	previousSlotHashes := sealevel.SysvarCache.SlotHashes
	t.Cleanup(func() {
		sealevel.SysvarCache.Clock = previousClock
		sealevel.SysvarCache.SlotHashes = previousSlotHashes
	})
	conflictingClock := sealevel.SysvarClock{Slot: 9_999, UnixTimestamp: 9_999}
	conflictingSlotHashes := sealevel.SysvarSlotHashes{{Slot: 9_999, Hash: [32]byte{9}}}
	sealevel.SysvarCache.Clock.Sysvar = &conflictingClock
	sealevel.SysvarCache.SlotHashes.Sysvar = &conflictingSlotHashes

	slotCtx, err := NewLeaderSlotCtx(childSlot, parentSlot, nil, ParentContext{
		BankSysvars:  parentSysvars,
		UnrootedRead: panicLeaderAccountReader{},
	}, &schedule)
	require.NoError(t, err)
	parentBankhash := solana.Hash{7}
	require.NoError(t, replay.PrepareLeaderSlotSysvars(slotCtx, &b.Block{
		Slot: childSlot, ParentSlot: parentSlot, Epoch: 0, ParentBankhash: parentBankhash,
	}, true))

	childClock, ok := slotCtx.BankSysvars().Clock()
	require.True(t, ok)
	require.Equal(t, childSlot, childClock.Slot)
	require.Equal(t, parentClock.UnixTimestamp, childClock.UnixTimestamp)
	childSlotHashes, ok := slotCtx.BankSysvars().SlotHashes()
	require.True(t, ok)
	require.NotEmpty(t, childSlotHashes)
	require.Equal(t, parentSlot, childSlotHashes[0].Slot)
	require.Equal(t, [32]byte(parentBankhash), childSlotHashes[0].Hash)

	// The immutable parent generation remains unchanged.
	unchangedClock, _ := parentSysvars.Clock()
	require.Equal(t, parentClock, unchangedClock)
	unchangedSlotHashes, _ := parentSysvars.SlotHashes()
	require.Equal(t, parentSlotHashes, unchangedSlotHashes)
}
