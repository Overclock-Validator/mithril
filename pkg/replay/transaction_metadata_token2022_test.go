package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/token-2022"
	"github.com/stretchr/testify/require"
)

func TestTransactionMetadataRealToken2022Execution(t *testing.T) {
	dir := os.Getenv("MITHRIL_PROGRAM_BENCH_DIR")
	if dir == "" {
		t.Skip("set MITHRIL_PROGRAM_BENCH_DIR to the pinned external program fixtures")
	}
	elf, err := os.ReadFile(filepath.Join(dir, "token2022.so"))
	require.NoError(t, err)
	// This is the actual Token-2022 ELF pinned in docs/sbpf-interpreter-benchmarks.md.
	require.Equal(t, "a794161408080f690dac00832f45b3c3e2b71f1339586667ad1f979cf91d5b68", fmt.Sprintf("%x", sha256.Sum256(elf)))
	const initialSource = uint64(1<<53 + 123)
	const transferred = uint64(1000)
	for _, extensions := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, removeADH := range []bool{false, true} {
				t.Run(fmt.Sprintf("extensions=%t/rollback=%t/remove-ADH=%t", extensions, failed, removeADH), func(t *testing.T) {
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
						put(solana.Token2022ProgramID, addresses.BpfLoader2Addr, elf, true)
						mint, source, destination := solana.PublicKey{0x61}, solana.PublicKey{0x63}, solana.PublicKey{0x64}
						payer := txfixture.PayerPubkey()
						mintSize, accountSize := 82, 165
						var setup []solana.Instruction
						if extensions {
							// Token-2022 initializes the actual TLV entries: MintCloseAuthority
							// needs 166 + 4 + 32 bytes; ImmutableOwner needs 166 + 4 bytes.
							mintSize, accountSize = 202, 170
							setup = append(setup,
								token2022.NewInitializeMintCloseAuthorityInstruction(payer, mint).Build(),
								token2022.NewInitializeImmutableOwnerInstruction(source).Build(),
								token2022.NewInitializeImmutableOwnerInstruction(destination).Build(),
							)
						}
						put(mint, solana.Token2022ProgramID, make([]byte, mintSize), false)
						put(source, solana.Token2022ProgramID, make([]byte, accountSize), false)
						put(destination, solana.Token2022ProgramID, make([]byte, accountSize), false)
						setup = append(setup,
							token2022.NewInitializeMint2Instruction(6, payer, payer, mint).Build(),
							token2022.NewInitializeAccount3Instruction(payer, source, mint).Build(),
							token2022.NewInitializeAccount3Instruction(payer, destination, mint).Build(),
							token2022.NewMintToInstruction(initialSource, mint, source, payer, nil).Build(),
							token2022.NewMintToInstruction(5, mint, destination, payer, nil).Build(),
						)
						_, _, err := processTransaction(ctx, &sync.WaitGroup{}, signLifecycleInstructions(t, setup...), nil, nil, nil, false, nil)
						require.NoError(t, err, "real Token-2022 fixture initialization")
						mintAccount, err := ctx.GetAccount(mint)
						require.NoError(t, err)
						require.Equal(t, initialSource+5, binary.LittleEndian.Uint64(mintAccount.Data[36:44]))
						if extensions {
							require.Equal(t, byte(1), mintAccount.Data[165])
							require.Equal(t, []byte{3, 0, 32, 0}, mintAccount.Data[166:170])
							require.Equal(t, payer[:], mintAccount.Data[170:202])
							for _, key := range []solana.PublicKey{source, destination} {
								account, err := ctx.GetAccount(key)
								require.NoError(t, err)
								require.Equal(t, []byte{2, 7, 0, 0, 0}, account.Data[165:170])
							}
						}
						instructions := []solana.Instruction{
							token2022.NewTransferCheckedInstruction(transferred, 6, source, mint, destination, payer, nil).Build(),
						}
						if failed {
							instructions = append(instructions, solana.NewInstruction(solana.SystemProgramID, nil, []byte{255}))
						}
						tx := signLifecycleInstructions(t, instructions...)
						before := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
						var capture *transactionMetadataCapture
						var group *transactionTokenGroup
						if enabled {
							group, err = prepareTransactionTokenGroup(ctx, []*solana.Transaction{tx})
							require.NoError(t, err)
							capture = &transactionMetadataCapture{}
						}
						fee, units, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, capture)
						if failed {
							require.ErrorIs(t, err, sealevel.InstrErrInvalidInstructionData)
						} else {
							require.NoError(t, err)
						}
						require.NotNil(t, fee)
						require.Equal(t, uint64(5000), fee.TotalFee)
						require.NotZero(t, units)
						state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
						wantSource, wantDestination := initialSource-transferred, uint64(5)+transferred
						if failed {
							wantSource, wantDestination = initialSource, 5
						}
						for i, key := range tx.Message.AccountKeys {
							if key == payer {
								require.Equal(t, before[i].Lamports-fee.TotalFee, state[i].Lamports)
							}
							if key == source {
								require.Equal(t, wantSource, binary.LittleEndian.Uint64(state[i].Data[64:72]))
							}
							if key == destination {
								require.Equal(t, wantDestination, binary.LittleEndian.Uint64(state[i].Data[64:72]))
							}
							if failed && key != payer {
								require.Equal(t, before[i], state[i], "rollback account %s", key)
							}
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
						require.Equal(t, []solana.PublicKey(tx.Message.AccountKeys), metadata.AccountKeys)
						require.Equal(t, fee.TotalFee, metadata.Fee)
						require.Equal(t, units, metadata.ComputeUnitsConsumed)
						if failed {
							require.JSONEq(t, `{"InstructionError":[1,"InvalidInstructionData"]}`, string(metadata.Error))
						} else {
							require.JSONEq(t, "null", string(metadata.Error))
						}
						pre, post, err := group.resolve(metadata.preTokens, metadata.postTokens)
						require.NoError(t, err)
						for _, snapshot := range []struct {
							balances []transactionTokenBalance
							source   uint64
							dest     uint64
						}{
							{pre, initialSource, 5},
							{post, wantSource, wantDestination},
						} {
							require.Len(t, snapshot.balances, 2)
							want := map[solana.PublicKey]uint64{source: snapshot.source, destination: snapshot.dest}
							for _, balance := range snapshot.balances {
								key := tx.Message.AccountKeys[balance.AccountIndex]
								amount, ok := want[key]
								require.True(t, ok, "unexpected or duplicate token account %s", key)
								delete(want, key)
								require.Equal(t, amount, balance.Amount)
								require.Equal(t, mint, balance.Mint)
								require.Equal(t, payer, balance.Owner)
								require.Equal(t, solana.Token2022ProgramID, balance.ProgramID)
								require.Equal(t, uint8(6), balance.Decimals)
								require.Equal(t, extensions, balance.HasExtensions)
							}
							require.Empty(t, want)
						}
					}
				})
			}
		}
	}
}
