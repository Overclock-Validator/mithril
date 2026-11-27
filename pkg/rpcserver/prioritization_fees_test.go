package rpcserver

import (
	"encoding/binary"
	"testing"
	"time"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// pricedTransfer pays price micro-lamports per compute unit to move lamports
// from payer to to.
func pricedTransfer(t *testing.T, price uint64, payer, to solana.PublicKey) *solana.Transaction {
	t.Helper()
	data := make([]byte, 9)
	data[0] = 3
	binary.LittleEndian.PutUint64(data[1:], price)
	ixs := []solana.Instruction{
		solana.NewInstruction(solana.PublicKey(a.ComputeBudgetProgramAddr), nil, data),
		system.NewTransferInstruction(1, payer, to).Build(),
	}
	tx, err := solana.NewTransaction(ixs, solana.Hash{1}, solana.TransactionPayer(payer))
	require.NoError(t, err)
	tx.Signatures = make([]solana.Signature, 1)
	return tx
}

func recordFees(server *RpcServer, slot uint64, txs ...*solana.Transaction) {
	server.RecordPrioritizationFees(slot, features.NewFeaturesDefault(), txs)
}

func TestGetRecentPrioritizationFees(t *testing.T) {
	server, client, _ := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	ctx := t.Context()
	_, err := client.GetRecentPrioritizationFees(ctx, nil)
	require.ErrorContains(t, err, "rpc.serve wallet", "a node below the wallet level records no fees")
	server.EnablePrioritizationFees()
	alice, bob, pool, other := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}

	empty, err := client.GetRecentPrioritizationFees(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)

	// Slot 11: the cheapest transaction pays 100; writing the pool cost at
	// least 5000. Slot 10 is replayed after 11 and has no non-vote
	// transactions.
	recordFees(server, 11,
		pricedTransfer(t, 100, alice, bob),
		pricedTransfer(t, 7000, alice, pool),
		pricedTransfer(t, 5000, bob, pool),
	)
	recordFees(server, 10)
	require.Eventually(t, func() bool {
		fees, err := client.GetRecentPrioritizationFees(ctx, nil)
		return err == nil && len(fees) == 2
	}, 5*time.Second, time.Millisecond)

	fees, err := client.GetRecentPrioritizationFees(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 10, fees[0].Slot, "oldest first")
	require.EqualValues(t, 0, fees[0].PrioritizationFee)
	require.EqualValues(t, 11, fees[1].Slot)
	require.EqualValues(t, 100, fees[1].PrioritizationFee)

	fees, err = client.GetRecentPrioritizationFees(ctx, solana.PublicKeySlice{other, pool})
	require.NoError(t, err)
	require.EqualValues(t, 5000, fees[1].PrioritizationFee, "the lowest price that wrote the pool")
	fees, err = client.GetRecentPrioritizationFees(ctx, solana.PublicKeySlice{alice})
	require.NoError(t, err)
	require.EqualValues(t, 100, fees[1].PrioritizationFee)

	tooMany := make(solana.PublicKeySlice, maxPrioritizationFeeAccounts+1)
	_, err = client.GetRecentPrioritizationFees(ctx, tooMany)
	require.ErrorContains(t, err, "Too many inputs")
}

func TestPrioritizationFeesKeepRecentBlocks(t *testing.T) {
	var pf prioritizationFees
	for slot := uint64(1); slot <= recentPrioritizationFeeBlocks+10; slot++ {
		pf.insert(&blockFees{slot: slot, min: slot})
	}
	pf.insert(&blockFees{slot: 100, min: 7}) // the slot replayed on another fork
	got := pf.recent(nil)
	require.Len(t, got, recentPrioritizationFeeBlocks)
	require.EqualValues(t, 11, got[0].Slot)
	require.EqualValues(t, recentPrioritizationFeeBlocks+10, got[len(got)-1].Slot)
	require.EqualValues(t, 7, got[100-11].PrioritizationFee)
}
