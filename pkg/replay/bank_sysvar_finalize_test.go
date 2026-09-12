package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestFinalizeBankSysvarsWritesBackFrozenState(t *testing.T) {
	const slot = uint64(77)
	recent := make(sealevel.SysvarRecentBlockhashes, 150)
	for i := range recent {
		recent[i] = sealevel.RecentBlockHashesEntry{
			Blockhash:     [32]byte{byte(i + 1)},
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: uint64(4_000 + i)},
		}
	}
	expectedEvicted := recent[len(recent)-1].Blockhash
	history := sealevel.SysvarSlotHistory{
		Bits: sealevel.SlotHistoryBitvec{
			Bits: sealevel.SlotHistoryInner{BlocksLen: 2, Blocks: []uint64{0, 0}},
			Len:  128,
		},
		NextSlot: slot,
	}
	recentAcct := &accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()}
	historyAcct := &accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: history.MustMarshal()}
	parentSnapshot, err := sealevel.NewBankSysvars(slot, recentAcct, historyAcct)
	require.NoError(t, err)

	mem := accounts.NewMemAccounts()
	slotCtx := &sealevel.SlotCtx{
		Slot:            slot,
		Accounts:        mem,
		FeeRateGovernor: &sealevel.FeeRateGovernor{LamportsPerSignature: 5_000},
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
	}
	require.NoError(t, slotCtx.SetAccount(recentAcct.Key, recentAcct))
	require.NoError(t, slotCtx.SetAccount(historyAcct.Key, historyAcct))
	require.NoError(t, slotCtx.PublishBankSysvars(parentSnapshot))
	slotCtx.Blockhash = [32]byte{0xA5}

	require.NoError(t, finalizeBankSysvars(slotCtx))
	require.Equal(t, expectedEvicted, slotCtx.LatestEvictedBlockhash)

	frozen := slotCtx.BankSysvars()
	require.NotSame(t, parentSnapshot, frozen)
	frozenRecent, ok := frozen.RecentBlockhashes()
	require.True(t, ok)
	require.Len(t, frozenRecent, 150)
	require.Equal(t, slotCtx.Blockhash, frozenRecent[0].Blockhash)
	require.Equal(t, uint64(5_000), frozenRecent[0].FeeCalculator.LamportsPerSignature)
	frozenHistory, ok := frozen.SlotHistory()
	require.True(t, ok)
	require.Equal(t, slot+1, frozenHistory.NextSlot)
	require.NotZero(t, frozenHistory.Bits.Bits.Blocks[(slot/64)%2]&(uint64(1)<<(slot%64)))

	storedRecent, err := slotCtx.GetAccount(sealevel.SysvarRecentBlockHashesAddr)
	require.NoError(t, err)
	storedHistory, err := slotCtx.GetAccount(sealevel.SysvarSlotHistoryAddr)
	require.NoError(t, err)
	frozenRecentRaw, ok := frozen.RawView(sealevel.SysvarRecentBlockHashesAddr)
	require.True(t, ok)
	frozenHistoryRaw, ok := frozen.RawView(sealevel.SysvarSlotHistoryAddr)
	require.True(t, ok)
	require.Equal(t, frozenRecentRaw, storedRecent.Data)
	require.Equal(t, frozenHistoryRaw, storedHistory.Data)

	// Copy-on-write finalization must not mutate the selected parent generation.
	parentRecent, ok := parentSnapshot.RecentBlockhashes()
	require.True(t, ok)
	require.Equal(t, expectedEvicted, parentRecent[len(parentRecent)-1].Blockhash)
	require.NotEqual(t, slotCtx.Blockhash, parentRecent[0].Blockhash)
	parentHistory, ok := parentSnapshot.SlotHistory()
	require.True(t, ok)
	require.Equal(t, slot, parentHistory.NextSlot)
}
