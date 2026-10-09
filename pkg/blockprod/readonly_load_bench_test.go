package blockprod

import (
	"crypto/ed25519"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
)

type readonlyBenchParent struct{ mem accounts.MemAccounts }

func (p readonlyBenchParent) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	raw := [32]byte(key)
	return p.mem.GetAccount(&raw)
}

// Only bank admission/execution/entry building are timed. The immutable parent
// is in memory; this excludes actual AccountsDB, network and signing costs.
func BenchmarkReadonlyPairBank(b *testing.B) {
	const perBank = 10000
	parent := readonlyBenchParent{accounts.NewMemAccounts()}
	pool := make([]solana.PublicKey, 128)
	for i := range pool {
		pool[i] = solana.PublicKey{byte(i + 1), 77}
		parent.mem.SetAccountWithoutLock(pool[i], &accounts.Account{Key: pool[i], Lamports: 1_000_000, Data: make([]byte, 9), Owner: solana.PublicKey{11}, RentEpoch: math.MaxUint64})
	}
	txs := make([]*solana.Transaction, perBank)
	key := txfixture.PayerPrivateKey()
	payer := key.PublicKey()
	hash := txfixture.TestBlockhash()
	for i := range txs {
		n := (i * 7919) % (128 * 127)
		a, c := n/127, n%127
		if c >= a {
			c++
		}
		msg := []byte{1, 0, 2, 3}
		msg = append(msg, payer[:]...)
		msg = append(msg, pool[a][:]...)
		msg = append(msg, pool[c][:]...)
		msg = append(msg, hash[:]...)
		msg = append(msg, 0)
		wire := []byte{1}
		wire = append(wire, ed25519.Sign(ed25519.PrivateKey(key), msg)...)
		wire = append(wire, msg...)
		var err error
		txs[i], err = solana.TransactionFromBytes(wire)
		if err != nil {
			b.Fatal(err)
		}
	}
	var env *TestEnv
	var prepared []*replay.PreparedTransaction
	defer func() {
		if env != nil {
			env.Close()
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%perBank == 0 {
			b.StopTimer()
			if env != nil {
				env.Close()
			}
			env = NewTestEnv(TestEnvConfig{})
			env.SlotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
			env.SlotCtx.UnrootedRead = parent
			env.Bank.preparer = replay.NewTransactionPreparer(env.SlotCtx.Features)
			if prepared == nil {
				prepared = make([]*replay.PreparedTransaction, perBank)
				for j, t := range txs {
					prepared[j] = env.Bank.preparer.Prepare(t)
					if prepared[j] == nil {
						b.Fatal("preparation failed")
					}
				}
			}
			b.StartTimer()
		}
		outcome, reason := env.Bank.ForgePreparedTransaction(txs[i%perBank], 198, prepared[i%perBank])
		if outcome != ForgeAccepted {
			b.Fatalf("%v / %v", outcome, reason)
		}
		if i%perBank == perBank-1 && env.Bank.CostTracker().BlockCost() != perBank*1028 {
			b.Fatalf("unexpected cost %d", env.Bank.CostTracker().BlockCost())
		}
	}
}
