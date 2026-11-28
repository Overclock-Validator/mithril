package rpcserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type slotHistoryFoldGate struct{ entered, release chan struct{} }

func (g *slotHistoryFoldGate) AccountsChanged(_ uint64, _ []*accounts.Account, _ bool) {
	close(g.entered)
	<-g.release
}

func TestBlockProductionPinsPublishedSlotHistoryAcrossForwardFold(t *testing.T) {
	db := newRPCAccountsDB(t)
	store := func(slot uint64, bits uint64) error {
		h := sealevel.SysvarSlotHistory{NextSlot: slot + 1, Bits: sealevel.SlotHistoryBitvec{Len: 64, Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{bits}}}}
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: h.MustMarshal()}}}}, slot, nil, nil)
		return err
	}
	require.NoError(t, store(10, 1<<10))
	server := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}}
	server.SetRootedBankState(10, 10, 0)
	leader := solana.PublicKey{42}
	global.SetLeaderScheduleForEpoch(0, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{leader: {10}}, 0))
	t.Cleanup(func() { global.SetLeaderScheduleForEpoch(0, nil) })
	params := mustRawParams(t, []any{map[string]any{"range": map[string]any{"firstSlot": 10, "lastSlot": 10}}})
	query := func(ctx context.Context) (GetBlockProductionResp, error) {
		return server.GetBlockProduction(ctx, params)
	}
	original, err := query(t.Context())
	require.NoError(t, err)
	require.Equal(t, [2]uint64{1, 1}, original.Value.ByIdentity[leader.String()])

	gate := &slotHistoryFoldGate{make(chan struct{}), make(chan struct{})}
	unregister := db.ObserveAccountState(gate)
	result := make(chan error, 1)
	go func() { result <- store(20, 1<<20) }()
	var release sync.Once
	resume := func() { release.Do(func() { close(gate.release); require.NoError(t, <-result); unregister() }) }
	defer resume()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fold did not reach publication")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	during, err := query(ctx)
	cancel()
	require.NoError(t, err, "warm published SlotHistory must remain readable during the next forward fold")
	require.Equal(t, original, during)
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	_, err = query(ctx)
	require.ErrorIs(t, err, context.Canceled)
	server.SetHistoryRecoveryPending(true)
	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	_, err = query(ctx)
	cancel()
	require.Error(t, err, "recovery must not admit the old cached root")
	server.SetHistoryRecoveryPending(false)
	server.SetRootedBankState(10, 10, 0)
	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	_, err = query(ctx)
	cancel()
	require.Error(t, err, "republishing the same slot must not inherit the previous publication's cache")
	resume()
	server.SetRootedBankState(20, 20, 0)
	advanced, err := query(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(20), advanced.Context.Slot)
	require.Equal(t, [2]uint64{1, 0}, advanced.Value.ByIdentity[leader.String()])
	// A same-slot write is not a forward fold, even before the bank is republished.
	require.NoError(t, store(20, 1<<10))
	replaced, err := query(t.Context())
	require.NoError(t, err)
	require.Equal(t, [2]uint64{1, 1}, replaced.Value.ByIdentity[leader.String()])
}
