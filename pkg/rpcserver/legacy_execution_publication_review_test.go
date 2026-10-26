package rpcserver

import (
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLegacyExecutionPublicationBecomesReadyAfterStoreDrain(t *testing.T) {
	for _, publishAfterDrain := range []bool{false, true} {
		name := "during-store"
		if publishAfterDrain {
			name = "after-drain"
		}
		t.Run(name, func(t *testing.T) { testLegacyExecutionPublication(t, publishAfterDrain, 42) })
	}
	t.Run("genesis-slot", func(t *testing.T) { testLegacyExecutionPublication(t, true, 0) })
}

func TestLegacyExecutionPublicationAcceptsSnapshotBaseline(t *testing.T) {
	db := newRPCAccountsDB(t)
	db.RootedDurable = false
	bank := simulationBankFixture(t)
	bank.BlockHeight = 30
	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
	server.SetRootedExecutionBank(bank)
	_, _, valid, err := server.selectSimulationBank("finalized", nil)
	require.NoError(t, err)
	require.True(t, valid(), "a restored snapshot does not need a process-local legacy write callback")
}

func testLegacyExecutionPublication(t *testing.T, publishAfterDrain bool, slot uint64) {
	t.Helper()
	for _, hashCase := range []string{"matching", "missing", "mismatched", "stale"} {
		t.Run(hashCase, func(t *testing.T) {
			db := newRPCAccountsDB(t)
			db.RootedDurable = false
			bank := simulationBankFixtureAt(t, slot, solana.Hash{3}, 6000)
			bank.BlockHeight = 30
			bank.Bankhash = solana.Hash{0x42}
			bankCtx, err := newSimulationBankContext(bank, db, db)
			require.NoError(t, err)
			bankCtx.UnrootedRead = nil
			bankCtx.FinalBankhash = append([]byte(nil), bank.Bankhash[:]...)
			tx, wire := testLegacyTransaction(t)

			callbackEntered, unblockCallback := make(chan struct{}), make(chan struct{})
			var unblockOnce sync.Once
			unblock := func() { unblockOnce.Do(func() { close(unblockCallback) }) }
			defer unblock()
			var callbackErr error
			require.NoError(t, db.StoreAccounts([]*accounts.Account{{
				Key: tx.Message.AccountKeys[0], Lamports: 1_000_000, Owner: addresses.SystemProgramAddr,
			}}, bank.Slot, func() {
				if hashCase != "missing" {
					hash := bank.Bankhash
					if hashCase == "mismatched" {
						hash = solana.Hash{0x43}
					}
					callbackErr = db.StoreBankHashForSlot(bank.Slot, hash[:])
				}
				close(callbackEntered)
				<-unblockCallback
			}))
			select {
			case <-callbackEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("legacy store callback did not start")
			}
			_, stable := db.CommittedAccountVersion()
			require.False(t, stable, "the real store queue retains its request until the callback returns")

			replay.ResetChainTip()
			t.Cleanup(replay.ResetChainTip)
			replay.UpdateChainTipFromSlotCtxWithBankMetadata(bankCtx, bankCtx.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: bank.BlockHeight})
			server := &RpcServer{acctsDb: db}
			server.SetSlotCtx(bankCtx)
			server.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
			if !publishAfterDrain {
				server.SetRootedExecutionBank(bank)
				require.False(t, server.rootedBank.Load().executionReady)
			}
			require.False(t, replay.ChainTipParentContext().AccountVersionsReady)
			unblock()
			if hashCase == "stale" {
				require.Eventually(t, func() bool {
					_, stable := db.CommittedAccountVersion()
					return stable
				}, 5*time.Second, time.Millisecond)
				require.NoError(t, db.StoreAccounts([]*accounts.Account{{
					Key: tx.Message.AccountKeys[0], Lamports: 2_000_000, Owner: addresses.SystemProgramAddr,
				}}, bank.Slot, nil))
			}
			db.WaitForStoreWorker()
			require.NoError(t, callbackErr)
			_, stable = db.CommittedAccountVersion()
			require.True(t, stable)
			require.Equal(t, bank.Slot, db.CommittedAccountSlot())
			if publishAfterDrain {
				server.SetRootedExecutionBank(bank)
			}

			for _, commitment := range []string{"finalized", "processed"} {
				t.Run(commitment, func(t *testing.T) {
					response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
						"encoding": "base64", "commitment": commitment, "replaceRecentBlockhash": true,
					}}))
					if hashCase != "matching" {
						require.ErrorIs(t, err, errSimulationViewChanged)
						return
					}
					require.NoError(t, err, "the completed bank's exact account state is now durable and stable")
					require.Nil(t, response.Value.Err)
				})
			}
			if hashCase == "matching" {
				server.InvalidateHealth()
				_, _, _, err := server.selectSimulationBank("finalized", nil)
				require.ErrorContains(t, err, "recovering", "a valid late disk binding must not reopen a recovering execution bank")
			}
		})
	}
}
