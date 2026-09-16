package blockprod

import (
	"crypto/ed25519"
	"crypto/sha256"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

const readonlyBlockAccepted = 48622 // 50M budget, including the next tx's upfront loaded-data reservation.
const readonlyActualCost = 1028

type readonlyBlockParent struct{ mem accounts.MemAccounts }

func (p readonlyBlockParent) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	raw := [32]byte(key)
	return p.mem.GetAccount(&raw)
}

type readonlyBlockFixture struct {
	wires  [][]byte
	txs    []*solana.Transaction
	payers []solana.PublicKey
	parent readonlyBlockParent
}

func makeReadonlyBlockFixture(tb testing.TB, count int) readonlyBlockFixture {
	tb.Helper()
	f := readonlyBlockFixture{parent: readonlyBlockParent{accounts.NewMemAccounts()}}
	keys := make([]ed25519.PrivateKey, 8)
	for i := range keys {
		seed := sha256.Sum256([]byte{byte(i), 73})
		keys[i] = ed25519.NewKeyFromSeed(seed[:])
		f.payers = append(f.payers, solana.PublicKeyFromBytes(keys[i].Public().(ed25519.PublicKey)))
	}
	pool := make([]solana.PublicKey, txfixture.ReadonlyPairPoolSize)
	for i := range pool {
		pool[i] = solana.PublicKey{byte(i + 1), 77}
		require.NoError(tb, f.parent.mem.SetAccountWithoutLock(pool[i], &accounts.Account{
			Key: pool[i], Lamports: 1_000_000, Data: make([]byte, 9), Owner: solana.PublicKey{11}, RentEpoch: math.MaxUint64,
		}))
	}
	for i := 0; i < count; i++ {
		wire, err := txfixture.ReadonlyPairWire(keys[i%8], txfixture.TestBlockhash(), pool, i/8)
		require.NoError(tb, err)
		tx, err := solana.TransactionFromBytes(wire)
		require.NoError(tb, err)
		f.wires = append(f.wires, wire)
		f.txs = append(f.txs, tx)
	}
	return f
}

func (f readonlyBlockFixture) bank(tb testing.TB, sink BatchSink) *TestEnv {
	tb.Helper()
	// Explicit active 200ms testnet budgets allow identical baseline/candidate
	// benchmark fixtures; LimitsForSlot has separate epoch-transition tests.
	limits := costmodel.DefaultLimits()
	limits.BlockCost, limits.WritableAccountCost = 50_000_000, 20_000_000
	limits.AllocatedDataSizeDelta, limits.MaxEntryBytes = 50_000_000, 10*1024*1024-48
	env := NewTestEnv(TestEnvConfig{Limits: limits, Sink: sink})
	env.SlotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	env.SlotCtx.UnrootedRead = f.parent
	for _, payer := range f.payers {
		require.NoError(tb, env.SlotCtx.Accounts.SetAccountWithoutLock(payer, &accounts.Account{
			Key: payer, Lamports: 10_000_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64,
		}))
	}
	return env
}

// This is a capacity/correctness test, not a 200ms deadline assertion. It creates
// a full synthetic block, validates fee/cost accounting, and round-trips every
// emitted entry through the production shred generator and decoder. No network.
func TestReadonlyPairBlockCapacityAndShredRoundTrip(t *testing.T) {
	f := makeReadonlyBlockFixture(t, readonlyBlockAccepted+1)
	sink := &captureSink{}
	env := f.bank(t, sink)
	defer env.Close()
	for i, tx := range f.txs {
		result, reason := env.Bank.ForgeTransaction(tx, len(f.wires[i]))
		if i < readonlyBlockAccepted {
			require.Equal(t, ForgeAccepted, result, "transaction %d", i)
		} else {
			require.Equal(t, ForgeDroppedCost, result)
			require.Equal(t, costmodel.ExceedBlockCost, reason)
		}
	}
	env.Bank.Freeze()
	require.Equal(t, uint64(readonlyBlockAccepted*readonlyActualCost), env.Bank.CostTracker().BlockCost())
	require.Equal(t, uint64(readonlyBlockAccepted), env.Bank.NumSignatures())
	require.Equal(t, uint64(readonlyBlockAccepted*5000), env.Bank.TxFeeAccumulator().TotalFees)
	require.Len(t, env.Bank.ForgedTransactions(), readonlyBlockAccepted)
	require.LessOrEqual(t, uint64(env.Bank.EntryBytes()), env.Bank.CostTracker().Limits().MaxEntryBytes)
	for i, payer := range f.payers {
		included := readonlyBlockAccepted / 8
		if i < readonlyBlockAccepted%8 {
			included++
		}
		acct, err := env.SlotCtx.GetAccount(payer)
		require.NoError(t, err)
		require.Equal(t, uint64(10_000_000_000-included*5000), acct.Lamports)
	}
	gen := turbine.ShredGenerator{Slot: 42, ParentSlot: 41, Version: 7}
	var root solana.Hash
	var nextData, nextCode uint32
	count, totalBytes := 0, 0
	previous := solana.Hash{0xab}
	for i, entries := range sink.batches {
		raw, err := marshalEntryBatchBytes(entries)
		require.NoError(t, err)
		require.Equal(t, len(raw), sink.bytes[i])
		totalBytes += len(raw)
		packets, chained, d, c, err := gen.MakeShredsFromData(txfixture.PayerPrivateKey(), raw, false, root, nextData, nextCode)
		require.NoError(t, err)
		root, nextData, nextCode = chained, d, c
		var shreds []*turbine.Shred
		for _, packet := range packets {
			sh, err := turbine.ParseShred(packet)
			require.NoError(t, err)
			if sh.Type == turbine.ShredTypeData {
				shreds = append(shreds, sh)
			}
		}
		decoded, err := turbine.DecodeEntriesFromDataShreds(shreds)
		require.NoError(t, err)
		require.Equal(t, entries, decoded)
		for _, entry := range decoded {
			require.Equal(t, turbine.NextAlpenglowEntryHash(previous, entry.NumHashes, entry.Txns), entry.Hash)
			previous = entry.Hash
			for _, tx := range entry.Txns {
				require.Equal(t, f.txs[count].Signatures, tx.Signatures)
				count++
			}
		}
	}
	require.Equal(t, readonlyBlockAccepted, count)
	require.Equal(t, env.Bank.EntryBytes(), totalBytes)
	require.Equal(t, env.Bank.EntryHash(), previous)
	require.Less(t, nextData, uint32(16384)) // Leaves room for header/footer/ending tick.
}

// One serial caller; signing and fixture/bank setup are excluded. Measures
// admission, execution, account publication, entry building and final flush.
// It excludes signature verification, actual AccountsDB, network and consensus.
func BenchmarkReadonlyPairFullBlock(b *testing.B) {
	f := makeReadonlyBlockFixture(b, readonlyBlockAccepted)
	for _, mode := range []string{"wire", "decoded"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				env := f.bank(b, nil)
				b.StartTimer()
				for j, wire := range f.wires {
					var result ForgeResult
					if mode == "wire" {
						result, _ = env.Bank.Forge(wire)
					} else {
						result, _ = env.Bank.ForgeTransaction(f.txs[j], len(wire))
					}
					if result != ForgeAccepted {
						b.Fatalf("transaction %d: %v", j, result)
					}
				}
				env.Bank.Freeze()
				b.StopTimer()
				if env.Bank.CostTracker().BlockCost() != readonlyBlockAccepted*readonlyActualCost {
					b.Fatal("unexpected block cost")
				}
				env.Close()
				b.StartTimer()
			}
			b.ReportMetric(float64(readonlyBlockAccepted), "tx/block")
		})
	}
}
