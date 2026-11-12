package replay

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// TestParentAccountLockReplayMeasurement is an opt-in whole-ProcessBlock
// measurement, not a mainnet replay benchmark. The runner builds otherwise
// identical test binaries with the locked accessor and the previous unlocked
// accessor. Each block gets fresh child state over the same immutable parent,
// making the unlocked baseline valid only inside this isolated fixture.
//
// Timing includes ProcessBlock account loading, transaction execution, bank
// hashing and publication to a memory tail. It excludes transaction generation,
// signature verification (already marked verified), source/consensus work,
// durable folds, real AccountsDB I/O and the production outer SlotReplay loop.
// Run scripts/bench_parent_account_lock.py to build and compare both variants.
func TestParentAccountLockReplayMeasurement(t *testing.T) {
	output := os.Getenv("MITHRIL_PARENT_ACCOUNT_LOCK_BENCH_OUT")
	if output == "" {
		t.Skip("opt-in replay measurement; set MITHRIL_PARENT_ACCOUNT_LOCK_BENCH_OUT")
	}
	previousMetrics := metrics.GlobalBlockReplay
	t.Cleanup(func() { metrics.GlobalBlockReplay = previousMetrics })
	blocks := 100
	if raw := os.Getenv("MITHRIL_PARENT_ACCOUNT_LOCK_BENCH_BLOCKS"); raw != "" {
		var err error
		blocks, err = strconv.Atoi(raw)
		require.NoError(t, err)
		require.Greater(t, blocks, 0)
	}
	const warmupBlocks = 20
	const txParallelism = 4
	type measurement struct {
		TransactionsPerBlock int     `json:"transactions_per_block"`
		MeasuredBlocks       int     `json:"measured_blocks"`
		WarmupBlocks         int     `json:"warmup_blocks"`
		TransactionDigest    string  `json:"transaction_digest_sha256"`
		Bankhash             string  `json:"bankhash_hex"`
		ConsumedCUPerBlock   uint64  `json:"consumed_cu_per_block"`
		TotalConsumedCU      uint64  `json:"total_consumed_cu"`
		TotalProcessBlockNs  int64   `json:"total_process_block_ns"`
		ProcessBlockNs       []int64 `json:"process_block_ns"`
		CUPerNs              float64 `json:"cu_per_ns"`
	}
	var results []measurement
	for _, n := range []int{64, 256, 1024} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			env := newLifecycleEnv(t)
			txs := make([]*solana.Transaction, n)
			payers := make([]solana.PublicKey, n)
			destinations := make([]solana.PublicKey, n)
			digest := sha256.New()
			for i := range txs {
				seed := sha256.Sum256([]byte(fmt.Sprintf("parent-account-lock/payer/%d", i)))
				privateKey := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
				payers[i] = privateKey.PublicKey()
				destinations[i] = solana.PublicKey(sha256.Sum256([]byte(fmt.Sprintf("parent-account-lock/destination/%d", i))))
				for _, key := range []solana.PublicKey{payers[i], destinations[i]} {
					require.NoError(t, env.durable.SetAccountWithoutLock(key, &accounts.Account{
						Key: key, Lamports: 10_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: ^uint64(0),
					}))
				}
				tx, err := solana.NewTransaction([]solana.Instruction{
					system.NewTransferInstruction(1, payers[i], destinations[i]).Build(),
				}, txfixture.TestBlockhash(), solana.TransactionPayer(payers[i]))
				require.NoError(t, err)
				_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
					if key == payers[i] {
						return &privateKey
					}
					return nil
				})
				require.NoError(t, err)
				wire, err := tx.MarshalBinary()
				require.NoError(t, err)
				_, _ = digest.Write(wire)
				txs[i] = tx
			}
			result := measurement{TransactionsPerBlock: n, MeasuredBlocks: blocks, WarmupBlocks: warmupBlocks,
				TransactionDigest: hex.EncodeToString(digest.Sum(nil)), ProcessBlockNs: make([]int64, 0, blocks)}
			for iteration := -warmupBlocks; iteration < blocks; iteration++ {
				tail := &lifecycleTail{durable: env.durable}
				block := env.block(txs)
				block.MarkTransactionSignaturesVerified()
				statuses := NewTransactionStatusCache()
				persisted := &persistedTracker{}
				metrics.GlobalBlockReplay = metrics.BlockReplay{}
				start := time.Now()
				slotCtx, err := ProcessBlock(env.acctsDb, block, env.epochSchedule, txParallelism, nil, persisted, tail, statuses, false, env.parent)
				duration := time.Since(start).Nanoseconds()
				require.NoError(t, err)
				require.Equal(t, uint64(n), slotCtx.NumSignatures)
				require.Positive(t, slotCtx.TotalComputeUnitsConsumed)
				require.Len(t, tail.added, 1)
				bankhash := hex.EncodeToString(slotCtx.FinalBankhash)
				if iteration == -warmupBlocks {
					result.Bankhash = bankhash
					result.ConsumedCUPerBlock = slotCtx.TotalComputeUnitsConsumed
					// Check successful transfers, not merely accepted instruction
					// failures that would make the workload deceptively cheap.
					for i := range txs {
						payer, err := slotCtx.GetAccount(payers[i])
						require.NoError(t, err)
						require.Equal(t, uint64(9_994_999), payer.Lamports)
						dest, err := slotCtx.GetAccount(destinations[i])
						require.NoError(t, err)
						require.Equal(t, uint64(10_000_001), dest.Lamports)
					}
				}
				require.Equal(t, result.Bankhash, bankhash, "every run must start from identical parent state")
				require.Equal(t, result.ConsumedCUPerBlock, slotCtx.TotalComputeUnitsConsumed)
				if iteration >= 0 {
					result.ProcessBlockNs = append(result.ProcessBlockNs, duration)
					result.TotalProcessBlockNs += duration
					result.TotalConsumedCU += slotCtx.TotalComputeUnitsConsumed
				}
			}
			result.CUPerNs = float64(result.TotalConsumedCU) / float64(result.TotalProcessBlockNs)
			results = append(results, result)
		})
	}
	report := struct {
		GOMAXPROCS    int           `json:"gomaxprocs"`
		TxParallelism int           `json:"tx_parallelism"`
		Workloads     []measurement `json:"workloads"`
	}{runtime.GOMAXPROCS(0), txParallelism, results}
	data, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, append(data, '\n'), 0600))
}
