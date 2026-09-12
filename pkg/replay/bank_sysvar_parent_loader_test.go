package replay

import (
	"bytes"
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// parentSnapshotOnlySource permits the normal transaction-account batch but
// fails the test if a parent-derived bank tries to reload any lifecycle sysvar
// individually from the tail/AccountsDB.
type parentSnapshotOnlySource struct {
	t *testing.T
}

func (s *parentSnapshotOnlySource) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	s.t.Fatalf("unexpected parent-derived account read for %s", pubkey)
	return nil, nil
}

func (s *parentSnapshotOnlySource) GetAccountsBatch(_ context.Context, _ uint64, pubkeys []solana.PublicKey) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pubkeys))
	for i, pubkey := range pubkeys {
		out[i] = &accounts.Account{Key: pubkey}
	}
	return out, nil
}

func marshalStakeHistoryForParentLoader(t *testing.T, value *sealevel.SysvarStakeHistory) []byte {
	t.Helper()
	var data bytes.Buffer
	require.NoError(t, value.MarshalWithEncoder(bin.NewBinEncoder(&data)))
	return data.Bytes()
}

func marshalLastRestartSlotForParentLoader(t *testing.T, value sealevel.SysvarLastRestartSlot) []byte {
	t.Helper()
	var data bytes.Buffer
	require.NoError(t, bin.NewBinEncoder(&data).WriteUint64(value.LastRestartSlot, bin.LE))
	return data.Bytes()
}

func marshalEpochScheduleForParentLoader(t *testing.T, value sealevel.SysvarEpochSchedule) []byte {
	t.Helper()
	var data bytes.Buffer
	encoder := bin.NewBinEncoder(&data)
	require.NoError(t, encoder.WriteUint64(value.SlotsPerEpoch, bin.LE))
	require.NoError(t, encoder.WriteUint64(value.LeaderScheduleSlotOffset, bin.LE))
	require.NoError(t, encoder.WriteBool(value.Warmup))
	require.NoError(t, encoder.WriteUint64(value.FirstNormalEpoch, bin.LE))
	require.NoError(t, encoder.WriteUint64(value.FirstNormalSlot, bin.LE))
	return data.Bytes()
}

func TestParentDerivedBankLoadsLifecycleSysvarsFromExactSnapshot(t *testing.T) {
	parentSlot := uint64(7)
	clock := sealevel.SysvarClock{Slot: parentSlot, EpochStartTimestamp: 111, UnixTimestamp: 222}
	slotHashes := sealevel.SysvarSlotHashes{{Slot: 6, Hash: [32]byte{0x61}}}
	recent := sealevel.SysvarRecentBlockhashes{{
		Blockhash: [32]byte{0x71},
		FeeCalculator: sealevel.FeeCalculator{
			LamportsPerSignature: 5_000,
		},
	}}
	slotHistory := sealevel.SysvarSlotHistory{
		Bits: sealevel.SlotHistoryBitvec{
			Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{0x81}},
			Len:  64,
		},
		NextSlot: 8,
	}
	stakeHistory := sealevel.SysvarStakeHistory{{
		Epoch: 0,
		Entry: sealevel.StakeHistoryEntry{
			Effective: 91,
		},
	}}
	lastRestart := sealevel.SysvarLastRestartSlot{LastRestartSlot: 3}
	parentEpochSchedule := sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            100,
		LeaderScheduleSlotOffset: 100,
	}

	parentAccounts := []*accounts.Account{
		{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: slotHashes.MustMarshal()},
		{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()},
		{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: slotHistory.MustMarshal()},
		{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: marshalStakeHistoryForParentLoader(t, &stakeHistory)},
		{Key: sealevel.SysvarLastRestartSlotAddr, Lamports: 1, Data: marshalLastRestartSlotForParentLoader(t, lastRestart)},
		{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1, Data: marshalEpochScheduleForParentLoader(t, parentEpochSchedule)},
	}
	parentSnapshot, err := sealevel.NewBankSysvars(parentSlot, parentAccounts...)
	require.NoError(t, err)

	// Model the abandoned suffix: every global value that used to drive the
	// lifecycle loader disagrees with the retained parent generation.
	previousClock := sealevel.SysvarCache.Clock
	previousSlotHashes := sealevel.SysvarCache.SlotHashes
	previousRecent := sealevel.SysvarCache.RecentBlockHashes
	t.Cleanup(func() {
		sealevel.SysvarCache.Clock.Sysvar, sealevel.SysvarCache.Clock.Acct = previousClock.Sysvar, previousClock.Acct
		sealevel.SysvarCache.SlotHashes.Sysvar, sealevel.SysvarCache.SlotHashes.Acct = previousSlotHashes.Sysvar, previousSlotHashes.Acct
		sealevel.SysvarCache.RecentBlockHashes.Sysvar, sealevel.SysvarCache.RecentBlockHashes.Acct = previousRecent.Sysvar, previousRecent.Acct
	})
	staleClock := sealevel.SysvarClock{Slot: 999, EpochStartTimestamp: 999, UnixTimestamp: 999}
	staleSlotHashes := sealevel.SysvarSlotHashes{{Slot: 998, Hash: [32]byte{0xEE}}}
	staleRecent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{0xEF}}}
	sealevel.SysvarCache.Clock.Sysvar = &staleClock
	sealevel.SysvarCache.Clock.Acct = &accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: staleClock.MustMarshal()}
	sealevel.SysvarCache.SlotHashes.Sysvar = &staleSlotHashes
	sealevel.SysvarCache.SlotHashes.Acct = &accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: staleSlotHashes.MustMarshal()}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &staleRecent
	sealevel.SysvarCache.RecentBlockHashes.Acct = &accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: staleRecent.MustMarshal()}

	block := &b.Block{
		Slot:           8,
		ParentSlot:     parentSlot,
		ParentBankhash: [32]byte{0x88},
		PrevFeeRateGovernor: &sealevel.FeeRateGovernor{
			TargetLamportsPerSignature: 5_000,
			LamportsPerSignature:       5_000,
		},
	}
	epochSchedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            100,
		LeaderScheduleSlotOffset: 0, // deliberately stale external schedule
	}
	_, parentAccts, _, childSnapshot, err := loadBlockAccountsAndUpdateSysvars(
		&parentSnapshotOnlySource{t: t}, block, epochSchedule, true, parentSnapshot,
	)
	require.NoError(t, err)

	childClock, ok := childSnapshot.Clock()
	require.True(t, ok)
	require.Equal(t, uint64(8), childClock.Slot)
	require.Equal(t, int64(111), childClock.EpochStartTimestamp)
	require.Equal(t, int64(222), childClock.UnixTimestamp,
		"Alpenglow bank start preserves the exact parent timestamp")
	require.Equal(t, uint64(1), childClock.LeaderScheduleEpoch,
		"Clock derivation uses the retained bank's EpochSchedule, not the stale external pointer")
	childSlotHashes, ok := childSnapshot.SlotHashes()
	require.True(t, ok)
	require.Len(t, childSlotHashes, 2)
	require.Equal(t, parentSlot, childSlotHashes[0].Slot)
	require.Equal(t, [32]byte{0x88}, childSlotHashes[0].Hash)
	require.Equal(t, uint64(6), childSlotHashes[1].Slot)
	childRecent, ok := childSnapshot.RecentBlockhashes()
	require.True(t, ok)
	require.Equal(t, [32]byte{0x71}, childRecent[0].Blockhash,
		"unchanged RecentBlockhashes are shared from the exact parent")

	for _, parentAccount := range parentAccounts {
		got, getErr := parentAccts.GetAccountWithoutLock(parentAccount.Key)
		require.NoError(t, getErr)
		require.Equal(t, parentAccount.Data, got.Data,
			"parent before-image for %s must come from retained BankSysvars", parentAccount.Key)
	}
}
