package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type resultRecoveryEngine struct{ consensusengine.Engine }

func (resultRecoveryEngine) Snapshot() consensusengine.Snapshot { return consensusengine.Snapshot{} }

type resultRecoveryFault struct {
	panic   bool
	pending bool
}

func (*resultRecoveryFault) SetSlotCtx(*sealevel.SlotCtx)              {}
func (*resultRecoveryFault) SetRootedBankState(uint64, uint64, uint64) {}
func (f *resultRecoveryFault) SetHistoryRecoveryPending(pending bool)  { f.pending = pending }
func (*resultRecoveryFault) RecordEpochRewards(*block.Block) error     { return nil }
func (*resultRecoveryFault) PrepareEpochRewards(uint64) error          { return nil }
func (*resultRecoveryFault) SetRootedEpochRewardsSlot(uint64) error    { return nil }
func (f *resultRecoveryFault) RewindEpochRewards(uint64) error {
	if f.panic {
		panic("recovery fixture panic")
	}
	return errors.New("recovery fixture rejection")
}

func TestTransactionResultsRecoveryOwnerExit(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, panics := range []bool{false, true} {
			cache, err := txstatus.NewResultCache(resultRecoveryBank(1, 0, true).ID)
			require.NoError(t, err)
			require.NoError(t, cache.Publish(resultRecoveryBank(2, 1, true)))
			var session *replay.TransactionResultSession
			if enabled {
				session, err = replay.NewTransactionResultSession(cache, "verifying")
				require.NoError(t, err)
			}
			fault := &resultRecoveryFault{panic: panics}
			opts := &replay.ConsensusOpts{Alpenglow: true, Engine: resultRecoveryEngine{}, TransactionResults: session}
			run := func() {
				result := runReplayWithRecovery(context.Background(), nil, t.TempDir(), nil, nil, 1, 1,
					nil, "", "", "", "", "", 0, "", nil, "", 0, false, false, false,
					nil, nil, fault, nil, nil, opts, 100, time.Now())
				require.ErrorContains(t, result.Error, "recovery fixture rejection")
			}
			if panics {
				require.PanicsWithValue(t, "recovery fixture panic", run)
			} else {
				run()
			}
			require.Equal(t, enabled, fault.pending, "disabled result collection preserves existing early-exit gate behavior")
			view, err := cache.Lookup([]solana.Signature{{2}})
			if enabled {
				require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
			} else {
				require.NoError(t, err)
				require.True(t, view.Results[0].Found)
			}
		}
	}
}

type resultRecoveryPublisher struct {
	*rpcserver.RpcServer
	onRoot func()
}

func (p *resultRecoveryPublisher) SetRootedBankState(slot, height, count uint64) {
	if p.onRoot != nil {
		p.onRoot()
	}
	p.RpcServer.SetRootedBankState(slot, height, count)
}

func resultRecoveryBank(slot, parent uint64, complete bool) txstatus.ResultBank {
	return txstatus.ResultBank{ID: txstatus.ResultBankID{Slot: slot, Blockhash: solana.Hash{byte(slot)}},
		Parent: txstatus.ResultBankID{Slot: parent, Blockhash: solana.Hash{byte(parent)}}, Complete: complete,
		Results: []txstatus.SignatureResult{{Signature: solana.Signature{byte(slot)}}}}
}

func resultRecoveryFixture(t *testing.T) (string, *accountsdb.AccountsDb, *state.MithrilState, *rpcserver.RpcServer) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	for _, name := range []string{"largest_file_id", "bootstrap_high_file_id"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, 8), 0o644))
	}
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	var tip *state.ResumeContext
	for _, slot := range []uint64{2, 4} {
		tip = testDurableStatusContext(t, dir, slot)
		encoded, err := json.Marshal(tip)
		require.NoError(t, err)
		_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: solana.PublicKey{91}, Lamports: slot}}}}, slot, nil, encoded)
		require.NoError(t, err)
	}
	s := &state.MithrilState{LastRootedSlot: 4, LastRootedContext: tip, LastSlot: 4}
	server := rpcserver.NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, server.EnableBlockHistory(filepath.Join(dir, "rpc-block-history"), 100, 0))
	require.NoError(t, server.RecordBlockHistory(&block.Block{Slot: 4, ParentSlot: 2, Blockhash: solana.Hash{4}, LastBlockhash: solana.Hash{2}}))
	require.NoError(t, server.PrepareBlockHistory(4))
	require.NoError(t, server.SetRootedBlockHistorySlot(4))
	publishRPCBankState(server, s)
	return dir, db, s, server
}

func resultRecoveryHTTP(t *testing.T, server *rpcserver.RpcServer) string {
	t.Helper()
	request := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[4,{"transactionDetails":"none"}]}`))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code)
	return response.Body.String()
}

func TestTransactionResultsRecoveryDurablePublication(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "cache disabled", "malformed resume", "missing blockhash", "malformed blockhash", "missing blockhash disabled", "malformed blockhash disabled"} {
		t.Run(mode, func(t *testing.T) {
			dir, db, s, server := resultRecoveryFixture(t)
			cache, err := txstatus.NewResultCache(resultRecoveryBank(1, 0, true).ID)
			require.NoError(t, err)
			session, err := replay.NewTransactionResultSession(cache, "verifying")
			require.NoError(t, err)
			require.NoError(t, cache.Publish(resultRecoveryBank(2, 1, true)))
			require.NoError(t, cache.Publish(resultRecoveryBank(4, 2, true)))
			require.NoError(t, cache.Root(resultRecoveryBank(4, 2, true).ID))
			disabled := mode == "disabled" || strings.HasSuffix(mode, "blockhash disabled")
			unknownHash := strings.Contains(mode, "blockhash")
			if disabled {
				session = nil
			}
			if mode == "cache disabled" {
				session.Invalidate()
			}
			require.Contains(t, resultRecoveryHTTP(t, server), `"result"`)
			require.True(t, rewindStoreBelowDivergence(dir, db, s, 3, 100, server))
			account, err := db.GetAccount(2, solana.PublicKey{91})
			require.NoError(t, err)
			require.Equal(t, uint64(2), account.Lamports, "actual DB rewind completed")
			require.Contains(t, resultRecoveryHTTP(t, server), "unavailable during account recovery", "DB rewind alone must not reopen")
			root, err := server.GetSlot(context.Background(), jsonrpc.RawParams(`[]`))
			require.NoError(t, err)
			require.Equal(t, uint64(4), root, "RPC publication has not happened yet")
			publisher := &resultRecoveryPublisher{RpcServer: server, onRoot: func() {
				require.Contains(t, resultRecoveryHTTP(t, server), "unavailable during account recovery", "gate must remain closed during root publication")
				if mode == "enabled" || mode == "malformed resume" {
					view, err := cache.Lookup(nil)
					require.NoError(t, err)
					require.Equal(t, resultRecoveryBank(2, 1, true).ID, view.Root)
					require.Equal(t, view.Root, view.Tip)
				}
			}}
			// Preserve the shared parser's existing behavior. Missing/malformed
			// optional identity must latch only the result cache, never invent an ID.
			if strings.HasPrefix(mode, "missing blockhash") {
				s.LastRootedContext.Blockhash = ""
			}
			if strings.HasPrefix(mode, "malformed blockhash") {
				s.LastRootedContext.Blockhash = "bad-0"
			}
			if mode == "malformed resume" {
				before, err := cache.Lookup(nil)
				require.NoError(t, err)
				s.LastRootedContext.Clock = "!"
				_, err = finishReplayRecovery(publisher, s, session, nil, true)
				require.ErrorContains(t, err, "decode rooted clock")
				after, err := cache.Lookup(nil)
				require.NoError(t, err)
				require.Equal(t, before, after, "failed reconstruction must not reset from a guessed parent")
				require.Contains(t, resultRecoveryHTTP(t, server), "unavailable during account recovery")
				s.LastRootedContext.Clock = ""
			}
			rs, err := finishReplayRecovery(publisher, s, session, nil, true)
			require.NoError(t, err)
			require.Equal(t, uint64(2), rs.ParentSlot)
			if unknownHash {
				require.Equal(t, [32]byte{}, rs.LastBlockhash)
			} else {
				require.Equal(t, [32]byte{2}, rs.LastBlockhash)
			}
			require.NotContains(t, resultRecoveryHTTP(t, server), "unavailable during account recovery")
			require.Contains(t, resultRecoveryHTTP(t, server), "Block not available for slot 4")
			root, err = server.GetSlot(context.Background(), jsonrpc.RawParams(`[]`))
			require.NoError(t, err)
			require.Equal(t, uint64(2), root)
			view, err := cache.Lookup([]solana.Signature{{4}})
			if mode == "cache disabled" || (unknownHash && !disabled) {
				require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
			} else {
				require.NoError(t, err)
				require.Equal(t, disabled, view.Results[0].Found)
			}
		})
	}
}

func TestTransactionResultsRecoveryRetryPreservesRootedGap(t *testing.T) {
	_, _, s, server := resultRecoveryFixture(t)
	cache, err := txstatus.NewResultCache(resultRecoveryBank(1, 0, true).ID)
	require.NoError(t, err)
	session, err := replay.NewTransactionResultSession(cache, "verifying")
	require.NoError(t, err)
	require.ErrorIs(t, cache.Publish(resultRecoveryBank(2, 1, false)), txstatus.ErrResultUnavailable)
	require.NoError(t, cache.Publish(resultRecoveryBank(4, 2, true)))
	require.NoError(t, cache.Root(resultRecoveryBank(4, 2, true).ID))
	require.NoError(t, cache.Publish(resultRecoveryBank(6, 4, true)))
	server.SetHistoryRecoveryPending(true)
	_, err = finishReplayRecovery(server, s, session, nil, false)
	require.NoError(t, err)
	view, err := cache.Lookup([]solana.Signature{{4}})
	require.NoError(t, err)
	require.True(t, view.Results[0].Found)
	require.True(t, view.Results[0].Finalized)
	require.Equal(t, resultRecoveryBank(4, 2, true).ID, view.Tip)
	_, err = cache.Lookup([]solana.Signature{{6}})
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable, "ordinary retry must preserve rooted gap, not cold-reset")
	require.Contains(t, resultRecoveryHTTP(t, server), `"result"`, "optional gap does not strand block history")
	// A later invalid optional notification must not veto otherwise valid recovery.
	cache.Invalidate()
	server.SetHistoryRecoveryPending(true)
	_, err = finishReplayRecovery(server, s, session, nil, false)
	require.NoError(t, err)
	require.Contains(t, resultRecoveryHTTP(t, server), `"result"`)
}
