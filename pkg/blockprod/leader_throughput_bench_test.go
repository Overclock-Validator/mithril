package blockprod

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
)

// BenchmarkWorkingBankHotAccounts measures admission, execution, publication,
// and entry batching. Signing and bank setup are outside the measured region.
// All wires are unique within a bank, so this cannot benchmark dedup rejection.
func BenchmarkWorkingBankHotAccounts(b *testing.B) { benchmarkWorkingBankHotAccounts(b, "wire") }
func BenchmarkWorkingBankDecodedHotAccounts(b *testing.B) {
	benchmarkWorkingBankHotAccounts(b, "decoded")
}
func BenchmarkWorkingBankPreparedHotAccounts(b *testing.B) {
	benchmarkWorkingBankHotAccounts(b, "prepared")
}

func benchmarkWorkingBankHotAccounts(b *testing.B, mode string) {
	const perBank = 10000
	for _, workload := range []string{"compute_budget", "transfer"} {
		b.Run(workload, func(b *testing.B) {
			wires := make([][]byte, perBank)
			for i := range wires {
				if workload == "transfer" {
					wires[i] = txfixture.MustSignedTransferWire(uint64(i))
					continue
				}
				tx, err := solana.NewTransaction([]solana.Instruction{
					computebudget.NewSetComputeUnitLimitInstruction(uint32(1000 + i)).Build(),
				}, txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
				if err != nil {
					b.Fatal(err)
				}
				key := txfixture.PayerPrivateKey()
				if _, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &key }); err != nil {
					b.Fatal(err)
				}
				wires[i], err = tx.MarshalBinary()
				if err != nil {
					b.Fatal(err)
				}
			}
			decoded := make([]*solana.Transaction, perBank)
			for i, wire := range wires {
				var err error
				decoded[i], err = solana.TransactionFromBytes(wire)
				if err != nil {
					b.Fatal(err)
				}
			}
			var prepared []*replay.PreparedTransaction
			var env *TestEnv
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
					env.SlotCtx.Features.EnableFeature(features.RaiseBlockLimitsTo100m, 0)
					if err := env.SlotCtx.Accounts.SetAccount(&addresses.ComputeBudgetProgramAddr, &accounts.Account{
						Key: addresses.ComputeBudgetProgramAddr, Lamports: 1,
						Owner: addresses.NativeLoaderAddr, Executable: true, RentEpoch: math.MaxUint64,
					}); err != nil {
						b.Fatal(err)
					}

					if mode == "prepared" {
						// Bind the immutable snapshot after all fixture feature setup is complete.
						env.Bank.preparer = replay.NewTransactionPreparer(env.SlotCtx.Features)
						if prepared == nil {
							prepared = make([]*replay.PreparedTransaction, perBank)
							for j, tx := range decoded {
								prepared[j] = env.Bank.preparer.Prepare(tx)
								if prepared[j] == nil {
									b.Fatal("preparation failed")
								}
							}
						}
					}
					b.StartTimer()
				}
				var result ForgeResult
				var reason costmodel.ExceedReason
				switch mode {
				case "prepared":
					result, reason = env.Bank.ForgePreparedTransaction(decoded[i%perBank], len(wires[i%perBank]), prepared[i%perBank])
				case "decoded":
					result, reason = env.Bank.ForgeTransaction(decoded[i%perBank], len(wires[i%perBank]))
				default:
					result, reason = env.Bank.Forge(wires[i%perBank])
				}
				if result != ForgeAccepted {
					b.Fatalf("transaction %d: %v / %v", i, result, reason)
				}
			}
		})
	}
}
