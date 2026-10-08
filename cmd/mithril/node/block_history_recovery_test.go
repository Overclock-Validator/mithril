package node

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestBlockHistoryHTTPAfterDurableRewind(t *testing.T) {
	for _, name := range []string{"rewind", "adoption failure", "prune failure", "accounts cleanup failure", "accounts precommit failure"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), make([]byte, 8), 0o644))
			db, err := accountsdb.OpenDb(dir)
			require.NoError(t, err)
			db.RootedDurable = true
			db.InitCaches()
			t.Cleanup(db.CloseDb)
			address := solana.PublicKey{91}
			var tip *state.ResumeContext
			var checkpoint string
			for _, slot := range []uint64{2, 4} {
				tip = testDurableStatusContext(t, dir, slot)
				if slot == 2 {
					checkpoint = filepath.Join(dir, replay.TransactionStatusCheckpointDirectory, tip.TransactionStatusCheckpoint.File)
				}
				encoded, err := json.Marshal(tip)
				require.NoError(t, err)
				_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: address, Lamports: slot}}}}, slot, nil, encoded)
				require.NoError(t, err)
			}
			server := rpcserver.NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 3}, solana.Hash{1})
			require.NoError(t, server.EnableBlockHistory(filepath.Join(dir, "rpc-block-history"), 100, 0))
			require.NoError(t, server.RecordBlockHistory(&block.Block{Slot: 4, ParentSlot: 2, Blockhash: solana.Hash{4}, LastBlockhash: solana.Hash{2}}))
			require.NoError(t, server.PrepareBlockHistory(4))
			require.NoError(t, server.SetRootedBlockHistorySlot(4))
			endpoint := httptest.NewServer(server)
			t.Cleanup(endpoint.Close)
			endpoint.Client().Timeout = time.Second
			blockRequest := `{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[4,{"transactionDetails":"none"}]}`
			call := func(request string) map[string]json.RawMessage {
				t.Helper()
				response, err := endpoint.Client().Post(endpoint.URL, "application/json", bytes.NewBufferString(request))
				require.NoError(t, err)
				defer response.Body.Close()
				payload, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				t.Logf("HTTP=%d response=%s", response.StatusCode, payload)
				var result map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(payload, &result))
				return result
			}
			require.Contains(t, string(call(blockRequest)["result"]), solana.Hash{4}.String())
			s := &state.MithrilState{LastRootedSlot: 4, LastRootedContext: tip, LastSlot: 4}
			var publisher replay.SlotCtxSetter = server
			if name == "adoption failure" {
				publisher = &historyAdoptionFault{RpcServer: server, checkpoint: checkpoint}
			}
			if name == "prune failure" {
				batchPath := filepath.Join(dir, "rpc-block-history", "batch-00000000000000000004.json")
				require.NoError(t, os.Remove(batchPath))
				require.NoError(t, os.Mkdir(batchPath, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(batchPath, "blocker"), nil, 0o644))
			}
			var accountFaultPath string
			if name == "accounts cleanup failure" {
				accountFaultPath = filepath.Join(db.AcctsDir, "rewound")
				require.NoError(t, os.WriteFile(accountFaultPath, nil, 0o644))
			}
			if name == "accounts precommit failure" {
				manifests, err := accountsdb.ListFoldManifests(db.AcctsDir)
				require.NoError(t, err)
				for _, manifest := range manifests {
					if manifest.ThroughSlot == 4 {
						accountFaultPath = manifest.Path + ".rewound"
					}
				}
				require.NotEmpty(t, accountFaultPath)
				require.NoError(t, os.Mkdir(accountFaultPath, 0o755))
			}
			require.Equal(t, name == "rewind", rewindStoreBelowDivergence(dir, db, s, 3, 100, publisher))
			if name == "adoption failure" {
				require.NoFileExists(t, checkpoint)
				require.Equal(t, uint64(4), s.LastRootedSlot, "adoption failed after the account root changed")
			}
			account, err := db.GetAccount(2, address)
			require.NoError(t, err)
			if name == "accounts precommit failure" {
				require.Equal(t, uint64(4), account.Lamports, "failed manifest parking must not alter accounts")
			} else {
				require.Equal(t, uint64(2), account.Lamports)
			}
			if name != "rewind" {
				for _, request := range []string{
					blockRequest,
					`{"jsonrpc":"2.0","id":1,"method":"minimumLedgerSlot","params":[]}`,
					`{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}`,
				} {
					reply := call(request)
					require.Empty(t, reply["result"])
					require.Contains(t, string(reply["error"]), "RPC history is unavailable during account recovery")
				}
				if accountFaultPath == "" {
					// Inspect the side-store watermark independently of the gate; recovery remains halted in production.
					server.SetHistoryRecoveryPending(false)
					require.JSONEq(t, `{"code":-32004,"message":"Block not available for slot 4"}`, string(call(blockRequest)["error"]))
					return
				}
				require.FileExists(t, filepath.Join(dir, "rpc-block-history", "batch-00000000000000000004.json"), "failed account rewind must retain history")
				require.NoError(t, os.Remove(accountFaultPath))
				require.True(t, rewindStoreBelowDivergence(dir, db, s, 3, 100, server))
			}
			require.Equal(t, uint64(2), s.LastRootedSlot)
			require.JSONEq(t, `{"code":-32004,"message":"Block not available for slot 4"}`, string(call(blockRequest)["error"]))
		})
	}
}

// Remove the checkpoint after the real DB rewind and history update, before adoption validates it again.
type historyAdoptionFault struct {
	*rpcserver.RpcServer
	checkpoint string
}

func (f *historyAdoptionFault) RewindBlockHistory(slot uint64) error {
	if err := f.RpcServer.RewindBlockHistory(slot); err != nil {
		return err
	}
	return os.Remove(f.checkpoint)
}
