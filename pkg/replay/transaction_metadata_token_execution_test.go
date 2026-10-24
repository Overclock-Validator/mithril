package replay

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/token"
	"github.com/stretchr/testify/require"
)

func TestTransactionMetadataRealTokenExecution(t *testing.T) {
	for _, checked := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, removeADH := range []bool{false, true} {
				t.Run(fmt.Sprintf("checked=%t/rollback=%t/remove-ADH=%t", checked, failed, removeADH), func(t *testing.T) {
					var baseline []*accounts.Account
					var modified, writable map[solana.PublicKey]bool
					var baselineFee, baselineUnits uint64
					for _, enabled := range []bool{false, true} {
						ctx, _, _ := transactionMetadataCPIFixture(t, removeADH)
						put := func(key, owner solana.PublicKey, data []byte, executable bool) {
							require.NoError(t, ctx.SetAccount(key, &accounts.Account{
								Key: key, Owner: owner, Lamports: 100_000_000, Data: data,
								Executable: executable, RentEpoch: ^uint64(0),
							}))
						}
						put(solana.TokenProgramID, addresses.BpfLoader2Addr, fixtures.Load(t, "sbpf", "spl-token.so"), true)
						mint, source, destination := solana.PublicKey{0x51}, solana.PublicKey{0x53}, solana.PublicKey{0x54}
						payer := txfixture.PayerPubkey()
						tokenData := func(amount uint64) []byte {
							data := make([]byte, 165)
							copy(data, mint[:])
							copy(data[32:], payer[:])
							binary.LittleEndian.PutUint64(data[64:], amount)
							data[108] = 1
							return data
						}
						put(source, solana.TokenProgramID, tokenData(1_000_000), false)
						put(destination, solana.TokenProgramID, tokenData(5), false)
						mintData := make([]byte, 82)
						binary.LittleEndian.PutUint64(mintData[36:], 1_000_005)
						mintData[44], mintData[45] = 6, 1
						parentMint := &accounts.Account{
							Key: mint, Owner: solana.TokenProgramID, Lamports: 100_000_000,
							Data: mintData, RentEpoch: ^uint64(0),
						}
						var parentReads int
						var transfer solana.Instruction
						if checked {
							require.NoError(t, ctx.SetAccount(mint, parentMint.Clone()))
							transfer = token.NewTransferCheckedInstruction(1000, 6, source, mint, destination, payer, nil).Build()
						} else {
							// The mint is deliberately absent from both declared keys and
							// the block overlay. Only quiescent group hydration may read it.
							ctx.UnrootedRead = metadataAccountReader(func(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
								require.Equal(t, ctx.Slot, slot)
								if key != mint {
									return nil, accountsdb.ErrNoAccount
								}
								parentReads++
								return parentMint.Clone(), nil
							})
							transfer = token.NewTransferInstruction(1000, source, destination, payer, nil).Build()
						}
						instructions := []solana.Instruction{transfer}
						if failed {
							instructions = append(instructions, solana.NewInstruction(solana.SystemProgramID, nil, []byte{255}))
						}
						tx := signLifecycleInstructions(t, instructions...)
						require.Equal(t, checked, slices.Contains(tx.Message.AccountKeys, mint))
						var capture *transactionMetadataCapture
						var group *transactionTokenGroup
						if enabled {
							var err error
							group, err = prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
							require.NoError(t, err)
							capture = &transactionMetadataCapture{}
						}
						wantParentReads := 0
						if enabled && !checked {
							wantParentReads = 1
						}
						require.Equal(t, wantParentReads, parentReads)
						fee, units, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, capture)
						if failed {
							require.ErrorIs(t, err, sealevel.InstrErrInvalidInstructionData)
						} else {
							require.NoError(t, err)
						}
						require.NotNil(t, fee)
						require.Equal(t, uint64(5000), fee.TotalFee)
						require.NotZero(t, units)
						require.Equal(t, wantParentReads, parentReads, "transaction execution must not read the undeclared mint")
						state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
						if !checked {
							state = append(state, parentMint.Clone())
						}
						wantSource, wantDestination := uint64(999_000), uint64(1005)
						if failed {
							wantSource, wantDestination = 1_000_000, 5
						}
						for key, amount := range map[solana.PublicKey]uint64{source: wantSource, destination: wantDestination} {
							account, err := ctx.GetAccount(key)
							require.NoError(t, err)
							require.Equal(t, amount, binary.LittleEndian.Uint64(account.Data[64:72]))
						}
						if !enabled {
							baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
							modified, writable = maps.Clone(ctx.ModifiedAccts), maps.Clone(ctx.WritableAccts)
							continue
						}
						require.Equal(t, baseline, state)
						require.Equal(t, baselineFee, fee.TotalFee)
						require.Equal(t, baselineUnits, units)
						require.Equal(t, modified, ctx.ModifiedAccts)
						require.Equal(t, writable, ctx.WritableAccts)
						require.NoError(t, capture.Err)
						require.NotNil(t, capture.Record)
						metadata := capture.Record
						require.Equal(t, fee.TotalFee, metadata.Fee)
						require.Equal(t, units, metadata.ComputeUnitsConsumed)
						if failed {
							require.JSONEq(t, `{"InstructionError":[1,"InvalidInstructionData"]}`, string(metadata.Error))
						} else {
							require.JSONEq(t, "null", string(metadata.Error))
						}
						facts := func(sourceAmount, destinationAmount uint64) []transactionTokenFact {
							var result []transactionTokenFact
							for i, key := range tx.Message.AccountKeys {
								if key != source && key != destination {
									continue
								}
								amount := sourceAmount
								if key == destination {
									amount = destinationAmount
								}
								result = append(result, transactionTokenFact{
									AccountIndex: uint16(i), Mint: mint, Owner: payer, ProgramID: solana.TokenProgramID, Amount: amount,
								})
							}
							return result
						}
						require.Equal(t, facts(1_000_000, 5), metadata.preTokens.Balances)
						require.Equal(t, facts(wantSource, wantDestination), metadata.postTokens.Balances)
						pre, post, err := group.resolve(metadata.preTokens, metadata.postTokens)
						require.NoError(t, err)
						require.Equal(t, wantParentReads, parentReads, "ordered resolution must not read live mint state")
						for i, expected := range []struct {
							balances []transactionTokenBalance
							facts    []transactionTokenFact
						}{
							{pre, facts(1_000_000, 5)},
							{post, facts(wantSource, wantDestination)},
						} {
							require.Len(t, expected.balances, 2, "snapshot %d", i)
							for j, balance := range expected.balances {
								require.Equal(t, expected.facts[j], balance.transactionTokenFact)
								require.Equal(t, uint8(6), balance.Decimals)
								require.False(t, balance.HasExtensions)
							}
						}
					}
				})
			}
		}
	}
}
