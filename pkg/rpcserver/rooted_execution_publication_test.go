package rpcserver

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRootedExecutionPublicationRequiresMatchingDurableBank(t *testing.T) {
	db := newRPCAccountsDB(t)
	bank := simulationBankFixture(t)
	bank.Bankhash = solana.Hash{0x42}
	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
	server.SetRootedExecutionBank(bank)
	_, _, valid, err := server.selectSimulationBank("finalized", nil)
	require.NoError(t, err)
	require.True(t, valid(), "a restored snapshot does not need a process-local fold")

	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: bank.Slot}}, bank.Slot,
		map[uint64][32]byte{bank.Slot: bank.Bankhash}, nil)
	require.NoError(t, err)
	require.False(t, valid(), "a new fold invalidates the previous execution view")

	server.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
	wrong := *bank
	wrong.Bankhash = solana.Hash{0x43}
	server.SetRootedExecutionBank(&wrong)
	_, _, _, err = server.selectSimulationBank("finalized", nil)
	require.ErrorContains(t, err, "no rooted execution bank")

	server.SetRootedExecutionBank(bank)
	_, _, valid, err = server.selectSimulationBank("finalized", nil)
	require.NoError(t, err)
	require.True(t, valid())

	server.SetHistoryRecoveryPending(true)
	server.InvalidateHealth()
	server.SetRootedExecutionBank(bank)
	require.False(t, valid(), "a late execution publication does not reopen recovery")
}
