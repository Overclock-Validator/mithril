package replay

import (
	"encoding/binary"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func transactionMetadataCPIFixture(t *testing.T, removeADH bool) (*sealevel.SlotCtx, *solana.Transaction, solana.PublicKey) {
	t.Helper()
	ctx, cleanup := newCommitTestSlotCtx()
	t.Cleanup(cleanup)
	ctx.Features.EnableFeature(features.DisableAccountLoaderSpecialCase, 0)
	if removeADH {
		ctx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	}
	cache := accountsdb.NewWeightedCache[solana.PublicKey, *accountsdb.ProgramCacheEntry](1024, func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 })
	ctx.AccountsDb = &accountsdb.AccountsDb{ProgramCache: cache}
	put := func(key, owner solana.PublicKey, data []byte, executable bool) {
		require.NoError(t, ctx.Accounts.SetAccount((*[32]byte)(&key), &accounts.Account{
			Key: key, Owner: owner, Lamports: 100_000_000, Data: data,
			Executable: executable, RentEpoch: ^uint64(0),
		}))
	}
	program := solana.PublicKey{0x91}
	put(addresses.BpfLoader2Addr, addresses.NativeLoaderAddr, nil, true)
	put(program, addresses.BpfLoader2Addr, fixtures.Load(t, "sbpf", "cpi_rust_to_system_program_allocate.so"), true)
	pda, bump, err := solana.FindProgramAddress([][]byte{[]byte("You pass butter")}, program)
	require.NoError(t, err)
	put(pda, addresses.SystemProgramAddr, nil, false)
	instruction := solana.NewInstruction(program, solana.AccountMetaSlice{
		solana.Meta(addresses.SystemProgramAddr), solana.Meta(pda).WRITE(),
	}, []byte{bump})
	return ctx, signLifecycleInstructions(t, instruction), pda
}

func requireMetadataAllocateCPI(t *testing.T, metadata *transactionMetadata, pda solana.PublicKey) {
	t.Helper()
	require.Len(t, metadata.InnerInstructions, 1)
	inner := metadata.InnerInstructions[0]
	require.Equal(t, uint8(0), inner.Index)
	require.Len(t, inner.Instructions, 1)
	instruction := inner.Instructions[0]
	require.Equal(t, solana.PublicKey(addresses.SystemProgramAddr), metadata.AccountKeys[instruction.ProgramIdIndex])
	require.Equal(t, uint8(2), instruction.StackHeight)
	require.Len(t, instruction.Accounts, 1)
	require.Equal(t, pda, metadata.AccountKeys[instruction.Accounts[0]])
	data := binary.LittleEndian.AppendUint32(nil, 8)
	data = binary.LittleEndian.AppendUint64(data, 1337)
	require.Equal(t, data, instruction.Data)
}

func TestTransactionMetadataRealCPI(t *testing.T) {
	for _, removeADH := range []bool{false, true} {
		name := "with-ADH"
		if removeADH {
			name = "without-ADH"
		}
		t.Run(name, func(t *testing.T) {
			var baseline []*accounts.Account
			var baselineModified, baselineWritable map[solana.PublicKey]bool
			var baselineFee, baselineUnits uint64
			for _, enabled := range []bool{false, true} {
				name := "off"
				if enabled {
					name = "on"
				}
				t.Run(name, func(t *testing.T) {
					ctx, tx, pda := transactionMetadataCPIFixture(t, removeADH)
					var capture *transactionMetadataCapture
					if enabled {
						capture = &transactionMetadataCapture{}
					}
					fee, units, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, capture)
					require.NoError(t, err)
					require.NotNil(t, fee)
					require.Equal(t, uint64(5000), fee.TotalFee)
					require.NotZero(t, units)
					allocated, err := ctx.GetAccount(pda)
					require.NoError(t, err)
					require.Len(t, allocated.Data, 1337)
					var state []*accounts.Account
					for _, key := range tx.Message.AccountKeys {
						account, err := ctx.GetAccount(key)
						require.NoError(t, err)
						state = append(state, account)
					}
					if !enabled {
						baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
						baselineModified, baselineWritable = maps.Clone(ctx.ModifiedAccts), maps.Clone(ctx.WritableAccts)
						return
					}
					require.Equal(t, baseline, state)
					require.Equal(t, baselineModified, ctx.ModifiedAccts)
					require.Equal(t, baselineWritable, ctx.WritableAccts)
					require.Equal(t, baselineFee, fee.TotalFee)
					require.Equal(t, baselineUnits, units)
					require.NoError(t, capture.Err)
					require.NotNil(t, capture.Record)
					require.Equal(t, fee.TotalFee, capture.Record.Fee)
					require.Equal(t, units, capture.Record.ComputeUnitsConsumed)
					require.JSONEq(t, "null", string(capture.Record.Error))
					requireMetadataAllocateCPI(t, capture.Record, pda)
				})
			}
		})
	}
}

func TestTransactionMetadataCPIBufferOwnership(t *testing.T) {
	ctx, tx, pda := transactionMetadataCPIFixture(t, true)
	metadata, err := prepareTransactionMetadata(ctx, tx, nil)
	require.NoError(t, err)
	wire, keys := slices.Clone(metadata.Transaction), slices.Clone(metadata.AccountKeys)
	signatures := slices.Clone(tx.Signatures)
	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx: ctx, Transaction: tx, LeanResult: true, RecordInnerInstructions: true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ExecCtx)
	require.False(t, output.ExecCtx.IsSimulation)
	require.NoError(t, ApplySuccessfulTransaction(ctx, output))
	// Seed return data only to test copying; the allocation program emits none.
	returnProgram := tx.Message.AccountKeys[tx.Message.Instructions[0].ProgramIDIndex]
	returnData := []byte{1, 2, 3}
	output.ExecCtx.TransactionContext.SetReturnData(returnProgram, returnData)
	require.NoError(t, metadata.finish(ctx, tx, output, output.FeeInfo.TotalFee, nil))
	requireMetadataAllocateCPI(t, metadata, pda)
	require.NotNil(t, metadata.ReturnData)
	require.Equal(t, returnProgram, metadata.ReturnData.ProgramId)
	require.Len(t, output.ExecCtx.InnerInstrs, 1)
	output.ExecCtx.InnerInstrs[0].Data[0] ^= 0xff
	output.ExecCtx.InnerInstrs[0].Accounts[0] ^= 0xff
	returnData[0] ^= 0xff
	tx.Message.Instructions[0].Data[0] ^= 0xff
	tx.Message.AccountKeys[0][0] ^= 0xff
	tx.Signatures[0][0] ^= 0xff
	requireMetadataAllocateCPI(t, metadata, pda)
	require.Equal(t, wire, metadata.Transaction)
	require.Equal(t, keys, metadata.AccountKeys)
	require.Equal(t, []byte{1, 2, 3}, metadata.ReturnData.Data)
	decoded, err := solana.TransactionFromBytes(metadata.Transaction)
	require.NoError(t, err)
	require.Equal(t, signatures, decoded.Signatures)
}

func TestTransactionMetadataRejectsIncompleteCPI(t *testing.T) {
	ctx, tx, _ := transactionMetadataCPIFixture(t, true)
	metadata, err := prepareTransactionMetadata(ctx, tx, nil)
	require.NoError(t, err)
	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx: ctx, Transaction: tx, LeanResult: true,
		RecordInnerInstructions: true, boundedMetadata: true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NoError(t, ApplySuccessfulTransaction(ctx, output))
	before := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
	// The collector tests exercise rejection itself. A sticky capture error must
	// reject the record before reading post-state or publishing partial CPI.
	output.ExecCtx.InnerInstructionsCaptureError = sealevel.InstrErrCallDepth
	require.ErrorIs(t, metadata.finish(nil, tx, output, output.FeeInfo.TotalFee, nil), sealevel.InstrErrCallDepth)
	require.Empty(t, metadata.InnerInstructions)
	require.Empty(t, metadata.PostBalances)
	require.Equal(t, before, metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys))
}
