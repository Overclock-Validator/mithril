package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/token-2022"
	"github.com/stretchr/testify/require"
)

func historyConsumerCapture(t *testing.T, ctx *sealevel.SlotCtx, tx *solana.Transaction, wantErr error) *transactionMetadata {
	t.Helper()
	var capture blockMetadataCapture
	group := capture.beginGroup(ctx, []*solana.Transaction{tx}, 1)
	require.NoError(t, capture.Err)
	require.NotNil(t, group)
	fee, units, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, group.transaction(0))
	if wantErr == nil {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, wantErr)
	}
	require.NotNil(t, fee)
	require.NoError(t, group.captures[0].Err)
	require.NotNil(t, group.captures[0].Record)
	_, err = group.captures[0].Record.historyRecord()
	require.ErrorContains(t, err, "unresolved")
	capture.finishGroup(group)
	require.NoError(t, capture.Err)
	require.Len(t, capture.records, 1)
	record := capture.records[0]
	require.Equal(t, fee.TotalFee, record.Fee)
	require.Equal(t, units, record.ComputeUnitsConsumed)
	return record
}

func historyConsumerRoundTrip(t *testing.T, captured *transactionMetadata) *transactionhistory.Metadata {
	t.Helper()
	encoded, err := captured.historyRecord()
	require.NoError(t, err)
	wantWire, wantMetadata := slices.Clone(encoded.Transaction), slices.Clone(encoded.Metadata)
	// This isolated transaction uses a synthetic storage identity. Actual block
	// sealing/hash assertions live in TestBlockHistoryRecordRoundTrip.
	block := transactionhistory.Block{
		ID:         transactionhistory.BlockID{Slot: 42, Blockhash: [32]byte{1}},
		ParentSlot: 41, PreviousBlockhash: [32]byte{2}, Bankhash: [32]byte{3}, ParentBankhash: [32]byte{4},
		CaptureVersion: transactionhistory.CaptureVersion, Records: []transactionhistory.Record{encoded},
	}
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{5}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.WriteBlock(block))
	encoded.Transaction[0] ^= 255
	encoded.Metadata[0] ^= 255
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	refs, err := view.SignatureRefs(encoded.FirstSignature)
	require.NoError(t, err)
	require.Equal(t, []transactionhistory.Ref{{Block: block.ID, FirstSignature: encoded.FirstSignature}}, refs)
	point, err := view.Transaction(refs[0])
	require.NoError(t, err)
	require.Equal(t, wantWire, point.Transaction)
	require.Equal(t, wantMetadata, point.Metadata)
	whole, err := view.Block(block.ID)
	require.NoError(t, err)
	require.Equal(t, []transactionhistory.Record{point}, whole.Records)
	decoded, err := transactionhistory.UnmarshalMetadata(point.Metadata)
	require.NoError(t, err)
	require.Equal(t, captured.Transaction, point.Transaction)
	require.Equal(t, captured.AccountKeys, decoded.AccountKeys)
	require.Equal(t, captured.PreBalances, decoded.PreBalances)
	require.Equal(t, captured.PostBalances, decoded.PostBalances)
	require.Equal(t, captured.Fee, decoded.Fee)
	require.Equal(t, captured.ComputeUnitsConsumed, decoded.ComputeUnitsConsumed)
	require.JSONEq(t, string(captured.Error), string(decoded.Error))
	return decoded
}

func TestHistoryConsumerCPIStoreRoundTrip(t *testing.T) {
	for _, removeADH := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove-ADH=%t", removeADH), func(t *testing.T) {
			ctx, tx, pda := transactionMetadataCPIFixture(t, removeADH)
			captured := historyConsumerCapture(t, ctx, tx, nil)
			requireMetadataAllocateCPI(t, captured, pda)
			account, err := ctx.GetAccount(pda)
			require.NoError(t, err)
			require.Len(t, account.Data, 1337)
			decoded := historyConsumerRoundTrip(t, captured)
			require.Len(t, decoded.InnerInstructions, 1)
			require.Equal(t, uint8(0), decoded.InnerInstructions[0].Index)
			require.Len(t, decoded.InnerInstructions[0].Instructions, 1)
			instruction := decoded.InnerInstructions[0].Instructions[0]
			require.Equal(t, solana.SystemProgramID, decoded.AccountKeys[instruction.ProgramIDIndex])
			require.Equal(t, uint32(2), instruction.StackHeight)
			require.Len(t, instruction.Accounts, 1)
			require.Equal(t, pda, decoded.AccountKeys[instruction.Accounts[0]])
			want := binary.LittleEndian.AppendUint32(nil, 8)
			want = binary.LittleEndian.AppendUint64(want, 1337)
			require.Equal(t, want, instruction.Data)
			require.Empty(t, decoded.PreTokenBalances)
			require.Empty(t, decoded.PostTokenBalances)
			require.Nil(t, decoded.ReturnData)
		})
	}
}

func TestHistoryConsumerToken2022StoreRoundTrip(t *testing.T) {
	dir := os.Getenv("MITHRIL_PROGRAM_BENCH_DIR")
	if dir == "" {
		t.Skip("set MITHRIL_PROGRAM_BENCH_DIR to the pinned external program fixtures")
	}
	elf, err := os.ReadFile(filepath.Join(dir, "token2022.so"))
	require.NoError(t, err)
	require.Equal(t, "a794161408080f690dac00832f45b3c3e2b71f1339586667ad1f979cf91d5b68", fmt.Sprintf("%x", sha256.Sum256(elf)))
	const initial, moved = uint64(1<<53 + 123), uint64(1000)
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", failed), func(t *testing.T) {
			ctx, _, _ := transactionMetadataCPIFixture(t, true)
			put := func(key, owner solana.PublicKey, data []byte, executable bool) {
				require.NoError(t, ctx.SetAccount(key, &accounts.Account{
					Key: key, Owner: owner, Lamports: 100_000_000, Data: data,
					Executable: executable, RentEpoch: ^uint64(0),
				}))
			}
			mint, source, destination := solana.PublicKey{0x61}, solana.PublicKey{0x63}, solana.PublicKey{0x64}
			payer := txfixture.PayerPubkey()
			put(solana.Token2022ProgramID, addresses.BpfLoader2Addr, elf, true)
			put(mint, solana.Token2022ProgramID, make([]byte, 202), false)
			put(source, solana.Token2022ProgramID, make([]byte, 170), false)
			put(destination, solana.Token2022ProgramID, make([]byte, 170), false)
			setup := signLifecycleInstructions(t,
				token2022.NewInitializeMintCloseAuthorityInstruction(payer, mint).Build(),
				token2022.NewInitializeImmutableOwnerInstruction(source).Build(),
				token2022.NewInitializeImmutableOwnerInstruction(destination).Build(),
				token2022.NewInitializeMint2Instruction(6, payer, payer, mint).Build(),
				token2022.NewInitializeAccount3Instruction(payer, source, mint).Build(),
				token2022.NewInitializeAccount3Instruction(payer, destination, mint).Build(),
				token2022.NewMintToInstruction(initial, mint, source, payer, nil).Build(),
				token2022.NewMintToInstruction(5, mint, destination, payer, nil).Build(),
			)
			_, _, err := processTransaction(ctx, &sync.WaitGroup{}, setup, nil, nil, nil, false, nil)
			require.NoError(t, err)
			mintAccount, err := ctx.GetAccount(mint)
			require.NoError(t, err)
			require.Equal(t, []byte{1, 3, 0, 32, 0}, mintAccount.Data[165:170])
			for _, key := range []solana.PublicKey{source, destination} {
				account, err := ctx.GetAccount(key)
				require.NoError(t, err)
				require.Equal(t, []byte{2, 7, 0, 0, 0}, account.Data[165:170])
			}
			instructions := []solana.Instruction{token2022.NewTransferCheckedInstruction(moved, 6, source, mint, destination, payer, nil).Build()}
			var wantErr error
			if failed {
				instructions = append(instructions, solana.NewInstruction(solana.SystemProgramID, nil, []byte{255}))
				wantErr = sealevel.InstrErrInvalidInstructionData
			}
			tx := signLifecycleInstructions(t, instructions...)
			captured := historyConsumerCapture(t, ctx, tx, wantErr)
			for _, balances := range [][]transactionTokenBalance{captured.PreTokenBalances, captured.PostTokenBalances} {
				require.Len(t, balances, 2)
				for _, balance := range balances {
					require.True(t, balance.HasExtensions)
				}
			}
			decoded := historyConsumerRoundTrip(t, captured)
			wantSource, wantDestination := initial-moved, uint64(5)+moved
			if failed {
				wantSource, wantDestination = initial, 5
				require.JSONEq(t, `{"InstructionError":[1,"InvalidInstructionData"]}`, string(decoded.Error))
			} else {
				require.JSONEq(t, "null", string(decoded.Error))
			}
			for i, balances := range [][]transactionhistory.TokenBalance{decoded.PreTokenBalances, decoded.PostTokenBalances} {
				want := map[solana.PublicKey]uint64{source: initial, destination: 5}
				if i == 1 {
					want = map[solana.PublicKey]uint64{source: wantSource, destination: wantDestination}
				}
				require.Len(t, balances, 2)
				for _, balance := range balances {
					key := decoded.AccountKeys[balance.AccountIndex]
					amount, ok := want[key]
					require.True(t, ok)
					delete(want, key)
					require.Equal(t, amount, balance.Amount)
					require.Equal(t, uint8(6), balance.Decimals)
					require.Equal(t, mint, balance.Mint)
					require.Equal(t, payer, balance.Owner)
					require.Equal(t, solana.Token2022ProgramID, balance.ProgramID)
				}
				require.Empty(t, want)
			}
			for key, amount := range map[solana.PublicKey]uint64{source: wantSource, destination: wantDestination} {
				account, err := ctx.GetAccount(key)
				require.NoError(t, err)
				require.Equal(t, amount, binary.LittleEndian.Uint64(account.Data[64:72]))
			}
		})
	}
}

func TestHistoryConsumerVersionedStoreRoundTrip(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionV0, solana.MessageVersionV1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			var tx *solana.Transaction
			if version == solana.MessageVersionV0 {
				var err error
				tx, err = solana.TransactionFromBytes(signedV0TransferViaTableWire(t, 7))
				require.NoError(t, err)
				require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{lifecycleTableKey: {txfixture.DestPubkey()}}))
				require.NoError(t, tx.Message.ResolveLookups())
			} else {
				ctx.Features.EnableFeature(features.EnableTxV1, 0)
				tx = signedV1Transfer(t, solana.TransactionConfig{}.WithComputeUnitLimit(200_000).WithLoadedAccountsDataSizeLimit(54_321))
			}
			captured := historyConsumerCapture(t, ctx, tx, nil)
			decoded := historyConsumerRoundTrip(t, captured)
			require.Equal(t, []solana.PublicKey(tx.Message.AccountKeys), decoded.AccountKeys)
			encoded, err := captured.historyRecord()
			require.NoError(t, err)
			require.Equal(t, [64]byte(tx.Signatures[0]), encoded.FirstSignature)
			stored, err := solana.TransactionFromBytes(encoded.Transaction)
			require.NoError(t, err)
			require.Equal(t, version, stored.Message.GetVersion())
			if version == solana.MessageVersionV0 {
				require.Len(t, decoded.AccountKeys, len(stored.Message.AccountKeys)+1)
				require.Equal(t, txfixture.DestPubkey(), decoded.AccountKeys[len(decoded.AccountKeys)-1])
			}
		})
	}
}
