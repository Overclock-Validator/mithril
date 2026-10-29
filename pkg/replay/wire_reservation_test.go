package replay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Padded wires and synthetic outputs exercise capture, not program execution.
func padReservationWire(t *testing.T, tx *solana.Transaction) int {
	t.Helper()
	limit := txverify.MaxLegacyTransactionSize
	if tx.Message.GetVersion() == solana.MessageVersionV1 {
		limit = solana.MaxTransactionSizeV1
	}
	for range 3 {
		size, err := txverify.TransactionWireSize(tx)
		require.NoError(t, err)
		data := tx.Message.Instructions[0].Data
		if size == limit {
			require.NoError(t, txverify.SanitizeTransaction(tx))
			return size
		}
		if size < limit {
			tx.Message.Instructions[0].Data = append(data, make([]byte, limit-size)...)
		} else {
			require.GreaterOrEqual(t, len(data), size-limit)
			tx.Message.Instructions[0].Data = data[:len(data)-(size-limit)]
		}
	}
	t.Fatal("wire padding did not reach the unchanged version limit")
	return 0
}

func TestMetadataWireReservationExact(t *testing.T) {
	for _, profile := range []string{"short-legacy", "short-v1", "max-v1", "resolved256"} {
		t.Run(profile, func(t *testing.T) {
			ctx, tx, _ := accountingFixture(t, profile)
			wireSize, err := txverify.TransactionWireSize(tx)
			require.NoError(t, err)
			want := uint64(4096 + wireSize + len(tx.Message.AccountKeys)*1024)
			old := uint64(8192 + len(tx.Message.AccountKeys)*1024)
			var budget metadataBudget
			budget.used.Store(metadataBlockLimit - want)
			record, err := prepareTransactionMetadata(ctx, tx, &budget)
			require.NoError(t, err)
			require.Equal(t, want, record.charge)
			require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
			require.Equal(t, wireSize, len(record.Transaction))
			require.Equal(t, wireSize, cap(record.Transaction), "pinned SDK final wire allocation")
			require.Equal(t, uint64(solana.MaxTransactionSizeV1-wireSize), old-record.charge)
			if profile == "max-v1" {
				require.Equal(t, old, record.charge)
			}

			// A one-byte-short reservation must stop before any wire/account copy.
			// At most the fixed record header may escape; sanitizer costs are
			// measured separately, so a new serializer allocation is visible.
			preflightAllocs := testing.AllocsPerRun(10, func() {
				err = txverify.SanitizeTransaction(tx)
				if err == nil {
					_, err = txverify.TransactionWireSize(tx)
				}
			})
			require.NoError(t, err)
			before := uint64(metadataBlockLimit) - want + 1
			budget.used.Store(before)
			allocations := testing.AllocsPerRun(10, func() {
				record, err = prepareTransactionMetadata(nil, tx, &budget)
			})
			require.ErrorIs(t, err, errMetadataBudget)
			require.Nil(t, record)
			require.Equal(t, before, budget.used.Load())
			require.LessOrEqual(t, allocations-preflightAllocs, float64(1), "no encoded-wire or account copies before admission")
		})
	}
	var budget metadataBudget
	record, err := prepareTransactionMetadata(nil, nil, &budget)
	require.Error(t, err)
	require.Nil(t, record)
	require.Zero(t, budget.used.Load())
	_, tx, _ := accountingFixture(t, "max-v1")
	tx.Message.Instructions[0].Data = append(tx.Message.Instructions[0].Data, 0)
	record, err = prepareTransactionMetadata(nil, tx, &budget)
	require.Error(t, err)
	require.NotErrorIs(t, err, errMetadataBudget)
	require.Nil(t, record)
	require.Zero(t, budget.used.Load(), "wire limit remains a sanitizer boundary")
}

func TestMetadataWireReservationCombined(t *testing.T) {
	for _, profile := range []string{"max-v1", "resolved256", "token7", "token9", "token33", "token254", "cpi64groups"} {
		t.Run(profile, func(t *testing.T) {
			fixture := profile
			if profile == "cpi64groups" {
				fixture = "max-v1"
			}
			ctx, tx, _ := accountingFixture(t, fixture)
			_, _, output := accountingFixture(t, "dynamic32")
			output.ExecCtx.InnerInstrs = output.ExecCtx.InnerInstrs[:16]
			if profile == "cpi64groups" {
				for len(tx.Message.Instructions) < txverify.MaxInstructionsPerMessage {
					tx.Message.Instructions = append(tx.Message.Instructions, solana.CompiledInstruction{ProgramIDIndex: 2, Accounts: []uint16{0}, Data: []byte{1}})
				}
				output.ExecCtx.InnerInstrs = nil
				for i := range txverify.MaxInstructionsPerMessage {
					output.ExecCtx.InnerInstrs = append(output.ExecCtx.InnerInstrs, sealevel.RecordedInnerInstr{
						TopLevelIdx: uint8(i), ProgramIdIndex: 2, StackHeight: 2, Accounts: []uint8{0}, Data: []byte{1}})
				}
			}
			wireSize := padReservationWire(t, tx)
			index := uint8(255)
			output.ProcessingResult.TransactionError = &TransactionError{ErrorType: TransactionErrorInstructionError,
				InstructionIndex: &index, InstructionError: errors.New(strings.Repeat("\x00", metadataErrorTextLimit))}
			var capture blockMetadataCapture
			group := capture.beginGroup(ctx, []*solana.Transaction{tx}, 0)
			require.NoError(t, capture.Err)
			require.NotNil(t, group)
			record, err := prepareTransactionMetadata(ctx, tx, &capture.budget)
			require.NoError(t, err)
			baseCharge := record.charge
			require.NoError(t, record.finish(ctx, tx, output, 5000, &capture.budget))
			require.Greater(t, record.charge, baseCharge)
			require.LessOrEqual(t, record.charge, uint64(metadataRecordLimit))
			if profile == "cpi64groups" {
				require.Len(t, record.InnerInstructions, txverify.MaxInstructionsPerMessage)
				for i, inner := range record.InnerInstructions {
					require.Equal(t, uint8(i), inner.Index)
					require.Len(t, inner.Instructions, 1)
				}
			}
			require.JSONEq(t, `{"InstructionError":[255,"`+strings.Repeat(`\u0000`, metadataErrorTextLimit)+`"]}`, string(record.Error))

			// Fixed fields and worst escaped error fit the unchanged residual;
			// token maps and per-key arrays remain a separate 1,024/key obligation.
			fixed := uint64(unsafe.Sizeof(*record)) + uint64(cap(record.Error)) + uint64(unsafe.Sizeof(TransactionReturnData{}))
			require.LessOrEqual(t, fixed, uint64(4096))
			visible, maps := accountingRecord(record)
			pre, post, err := group.tokens.resolve(record.preTokens, record.postTokens)
			require.NoError(t, err)
			overlap := uint64(cap(pre)+cap(post)) * uint64(unsafe.Sizeof(transactionTokenBalance{}))
			require.LessOrEqual(t, visible+maps+overlap, record.charge, "lower tally with raw/resolved overlap, not map-bucket or allocator proof")
			group.captures[0].Record = record
			capture.finishGroup(group)
			require.NoError(t, capture.Err)
			require.Nil(t, record.preTokens.Mints)
			require.Nil(t, record.postTokens.Mints)
			if strings.HasPrefix(profile, "token") {
				require.Len(t, record.PreTokenBalances, len(tx.Message.AccountKeys)-2)
				require.Len(t, record.PostTokenBalances, len(tx.Message.AccountKeys)-2)
			}
			converted, err := record.historyRecord()
			require.NoError(t, err)
			require.Equal(t, record.Transaction, converted.Transaction)
			require.NotEmpty(t, converted.Metadata)
			t.Logf("profile=%s wire=%d keys=%d fixed=%d lower_overlap=%d charge=%d converted=%d", profile, wireSize,
				len(tx.Message.AccountKeys), fixed, visible+maps+overlap, record.charge, len(converted.Transaction)+len(converted.Metadata))
		})
	}
}

func TestMetadataWireReservationCombinedRefusal(t *testing.T) {
	ctx, tx, _ := accountingFixture(t, "token254")
	padReservationWire(t, tx)
	_, _, output := accountingFixture(t, "dynamic32")
	var budget metadataBudget
	record, err := prepareTransactionMetadata(ctx, tx, &budget)
	require.NoError(t, err)
	before := record.charge
	// Maximum keys plus 32 large CPI copies/logs exceed the unchanged 1-MiB
	// record cap. A nil context makes any late post-state read fail loudly.
	require.ErrorIs(t, record.finish(nil, tx, output, 0, &budget), errMetadataBudget)
	require.Equal(t, before, record.charge)
	require.Equal(t, before, budget.used.Load())
	require.Nil(t, record.PostBalances)
	require.Nil(t, record.Error)
	require.Nil(t, record.InnerInstructions)
	require.Nil(t, record.LogMessages)
	require.Nil(t, record.ReturnData)
	output.ExecCtx = nil
	output.ProcessingResult.TransactionError = &TransactionError{ErrorType: TransactionErrorInstructionError,
		InstructionError: errors.New(strings.Repeat("x", metadataErrorTextLimit+1))}
	require.ErrorIs(t, record.finish(nil, tx, output, 0, &budget), errMetadataBudget)
	require.Equal(t, before, record.charge)
	require.Nil(t, record.PostBalances)
	require.Nil(t, record.Error)
}

// These fixtures exercise capture/encoding, not execution or signature validity.
func accountingFixture(t *testing.T, name string) (*sealevel.SlotCtx, *solana.Transaction, LoadAndExecuteTransactionOutput) {
	t.Helper()
	var output LoadAndExecuteTransactionOutput
	if name == "resolved256" || strings.HasPrefix(name, "token") {
		count := 254
		if name != "resolved256" {
			var err error
			count, err = strconv.Atoi(strings.TrimPrefix(name, "token"))
			require.NoError(t, err)
		}
		require.Contains(t, []int{7, 9, 33, 254}, count)
		payer, mint, table := solana.PublicKey{1}, solana.PublicKey{4}, solana.PublicKey{3}
		ctx := tokenCaptureContext(t, tokenCaptureMint(mint, solana.TokenProgramID, 6, 82))
		tx := tokenCaptureTransaction(payer)
		if name == "resolved256" {
			tx.Message.AccountKeys[1] = solana.SystemProgramID
		}
		_, err := tx.Message.SetVersion(solana.MessageVersionV0)
		require.NoError(t, err)
		keys, indexes := make(solana.PublicKeySlice, count), make([]uint8, count)
		for i := range keys {
			keys[i][0] = 128
			binary.LittleEndian.PutUint64(keys[i][1:9], uint64(i))
			indexes[i] = uint8(i)
			account := tokenCaptureAccount(keys[i], mint, solana.TokenProgramID, 100, 165)
			if name == "resolved256" {
				account = &accounts.Account{Key: keys[i], Lamports: 1, Owner: solana.SystemProgramID}
			}
			require.NoError(t, ctx.SetAccount(keys[i], account))
		}
		tx.Message.AddressTableLookups = []solana.MessageAddressTableLookup{{AccountKey: table, WritableIndexes: indexes}}
		require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: keys}))
		require.NoError(t, tx.Message.ResolveLookups())
		return ctx, tx, output
	}
	ctx, cleanup := newCommitTestSlotCtx()
	t.Cleanup(cleanup)
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	switch name {
	case "short-legacy":
	case "short-v1", "max-v1":
		tx = signedV1Transfer(t, solana.TransactionConfig{})
		if name == "max-v1" {
			size, err := txverify.TransactionWireSize(tx)
			require.NoError(t, err)
			tx.Message.Instructions[0].Data = append(tx.Message.Instructions[0].Data, make([]byte, solana.MaxTransactionSizeV1-size)...)
		}
	case "dynamic32":
		logs := sealevel.NewBoundedLogRecorder()
		for range 10_000 {
			logs.Log("")
		}
		logs.Log("limit marker")
		exec := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{}, Log: logs}
		for range 32 {
			exec.InnerInstrs = append(exec.InnerInstrs, sealevel.RecordedInnerInstr{ProgramIdIndex: 2, StackHeight: 2, Accounts: []uint8{0, 1}, Data: bytes.Repeat([]byte{1}, 8193)})
		}
		exec.TransactionContext.SetReturnData(solana.SystemProgramID, bytes.Repeat([]byte{1}, 1024))
		output.ExecCtx = exec
	default:
		t.Fatalf("unrecognized accounting profile %q", name)
	}
	return ctx, tx, output
}

// This is a reachable-capacity tally, not unique heap ownership or RSS. Map
// payload is separate because len(map) does not expose bucket/growth capacity.
func accountingRecord(record *transactionMetadata) (visible, mapPayload uint64) {
	visible = uint64(unsafe.Sizeof(*record)) + uint64(cap(record.Transaction)+cap(record.Error))
	visible += uint64(cap(record.AccountKeys))*uint64(unsafe.Sizeof(solana.PublicKey{})) + uint64(cap(record.PreBalances)+cap(record.PostBalances))*8
	visible += uint64(cap(record.preTokens.Balances)+cap(record.postTokens.Balances)) * uint64(unsafe.Sizeof(transactionTokenFact{}))
	visible += uint64(cap(record.PreTokenBalances)+cap(record.PostTokenBalances)) * uint64(unsafe.Sizeof(transactionTokenBalance{}))
	mapPayload = uint64(len(record.preTokens.Mints)+len(record.postTokens.Mints)) * uint64(unsafe.Sizeof(solana.PublicKey{})+unsafe.Sizeof(tokenMintState{}))
	visible += uint64(cap(record.InnerInstructions)) * uint64(unsafe.Sizeof(InnerInstructionsList{}))
	for _, group := range record.InnerInstructions {
		visible += uint64(cap(group.Instructions)) * uint64(unsafe.Sizeof(CompiledInstruction{}))
		for _, instruction := range group.Instructions {
			visible += uint64(cap(instruction.Data) + cap(instruction.Accounts))
		}
	}
	visible += uint64(cap(record.LogMessages)) * uint64(unsafe.Sizeof(""))
	for _, message := range record.LogMessages {
		visible += uint64(len(message))
	}
	if record.ReturnData != nil {
		visible += uint64(unsafe.Sizeof(*record.ReturnData)) + uint64(cap(record.ReturnData.Data))
	}
	return visible, mapPayload
}
