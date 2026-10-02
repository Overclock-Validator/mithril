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
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestEpochRewardHTTPAfterDurableRewind(t *testing.T) {
	for _, name := range []string{"rewind", "prune failure", "accounts cleanup failure", "accounts precommit failure"} {
		t.Run(name, func(t *testing.T) {
			failPrune := name == "prune failure"
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
			for _, slot := range []uint64{2, 4} {
				tip = testDurableStatusContext(t, dir, slot)
				encoded, err := json.Marshal(tip)
				require.NoError(t, err)
				_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: address, Lamports: slot}}}}, slot, nil, encoded)
				require.NoError(t, err)
			}
			server := rpcserver.NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 3}, solana.Hash{1})
			require.NoError(t, server.EnableEpochRewards(filepath.Join(dir, "rpc-epoch-rewards"), 100, 0))
			record := &block.Block{Slot: 4, Epoch: 1, Rewards: []rpc.BlockReward{{Pubkey: address, Lamports: 2, PostBalance: 4, RewardType: rpc.RewardTypeVoting}}}
			require.NoError(t, server.RecordEpochRewards(record))
			require.NoError(t, server.PrepareEpochRewards(4))
			require.NoError(t, server.SetRootedEpochRewardsSlot(4))
			endpoint := httptest.NewServer(server)
			t.Cleanup(endpoint.Close)
			endpoint.Client().Timeout = time.Second
			call := func(minContextSlot uint64) map[string]json.RawMessage {
				t.Helper()
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "getInflationReward", "params": []any{[]string{address.String()}, map[string]any{"epoch": 0, "minContextSlot": minContextSlot}}})
				require.NoError(t, err)
				response, err := endpoint.Client().Post(endpoint.URL, "application/json", bytes.NewReader(body))
				require.NoError(t, err)
				defer response.Body.Close()
				payload, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				t.Logf("HTTP=%d response=%s", response.StatusCode, payload)
				var result map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(payload, &result))
				return result
			}
			want := `[{"epoch":0,"effectiveSlot":4,"amount":2,"postBalance":4,"commission":null}]`
			require.JSONEq(t, want, string(call(4)["result"]))
			// An in-memory fork switch above the durable root must retain its rewards.
			require.NoError(t, server.RewindEpochRewards(5))
			require.JSONEq(t, want, string(call(4)["result"]))

			s := &state.MithrilState{LastRootedSlot: 4, LastRootedContext: tip, LastSlot: 4}
			if failPrune {
				batchPath := filepath.Join(dir, "rpc-epoch-rewards", "batch-00000000000000000004.json")
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
			require.Equal(t, name == "rewind", rewindStoreBelowDivergence(dir, db, s, 3, 100, server))
			account, err := db.GetAccount(2, address)
			require.NoError(t, err)
			if name == "accounts precommit failure" {
				require.Equal(t, uint64(4), account.Lamports, "failed manifest parking must not alter accounts")
			} else {
				require.Equal(t, uint64(2), account.Lamports)
			}
			if name != "rewind" {
				for _, minContextSlot := range []uint64{2, 4} {
					reply := call(minContextSlot)
					require.Empty(t, reply["result"])
					require.Contains(t, string(reply["error"]), "RPC history is unavailable during account recovery")
				}
				if accountFaultPath == "" {
					// Inspect the side-store watermark independently of the gate; recovery remains halted in production.
					server.SetHistoryRecoveryPending(false)
					require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":2}}`, string(call(4)["error"]))
					require.JSONEq(t, `[null]`, string(call(2)["result"]))
					return
				}
				require.FileExists(t, filepath.Join(dir, "rpc-epoch-rewards", "batch-00000000000000000004.json"), "failed account rewind must retain rewards")
				require.NoError(t, os.Remove(accountFaultPath))
				require.True(t, rewindStoreBelowDivergence(dir, db, s, 3, 100, server))
			}
			require.Equal(t, uint64(2), s.LastRootedSlot)
			// Replay's startup hook must not restore the discarded durable reward.
			require.NoError(t, server.RewindEpochRewards(s.GetResumeSlot()))
			require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":2}}`, string(call(4)["error"]))
			require.JSONEq(t, `[null]`, string(call(2)["result"]))

			// A replacement on the selected fork becomes visible only after its fold.
			record.Rewards[0].Lamports, record.Rewards[0].PostBalance = 3, 5
			require.NoError(t, server.RecordEpochRewards(record))
			require.NoError(t, server.PrepareEpochRewards(4))
			require.JSONEq(t, `[null]`, string(call(2)["result"]))
			require.NoError(t, server.SetRootedEpochRewardsSlot(4))
			require.JSONEq(t, `[{"epoch":0,"effectiveSlot":4,"amount":3,"postBalance":5,"commission":null}]`, string(call(4)["result"]))
		})
	}
}
