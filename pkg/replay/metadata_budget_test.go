package replay

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestMetadataBudgetReservationBoundaries(t *testing.T) {
	var budget metadataBudget
	require.True(t, budget.reserve(metadataBlockLimit))
	require.True(t, budget.reserve(0))
	require.False(t, budget.reserve(1))
	require.False(t, budget.reserve(^uint64(0)))
	require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())

	var record transactionMetadata
	require.NoError(t, record.reserve(nil, metadataRecordLimit))
	require.NoError(t, record.reserve(nil, 0))
	require.ErrorIs(t, record.reserve(nil, 1), errMetadataBudget)
	require.ErrorIs(t, record.reserve(nil, ^uint64(0)), errMetadataBudget)
	require.Equal(t, uint64(metadataRecordLimit), record.charge)
	var refused transactionMetadata
	require.ErrorIs(t, refused.reserve(&budget, 1), errMetadataBudget)
	require.Zero(t, refused.charge)
	require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
}

func TestMetadataBudgetConcurrentReservations(t *testing.T) {
	var budget metadataBudget
	var accepted atomic.Uint64
	var wg sync.WaitGroup
	for range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if budget.reserve(metadataRecordLimit) {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(metadataBlockLimit/metadataRecordLimit), accepted.Load())
	require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
	require.False(t, budget.reserve(1))
}

func TestMetadataBudgetGroupPreflight(t *testing.T) {
	// These test the allocation preflight, not transaction validity. In
	// particular, a single message with this many keys cannot execute.
	txs := make([]*solana.Transaction, metadataTransactionLimit+1)
	for i := range txs {
		txs[i] = &solana.Transaction{}
	}
	require.NoError(t, metadataGroupCharge(txs[:metadataTransactionLimit], 0))
	require.ErrorIs(t, metadataGroupCharge(txs, 0), errMetadataBudget)
	require.NoError(t, metadataGroupCharge(txs[:1], metadataTransactionLimit-1))
	require.ErrorIs(t, metadataGroupCharge(txs[:2], metadataTransactionLimit-1), errMetadataBudget)

	keys := make([]solana.PublicKey, metadataGroupKeyLimit+1)
	exactKeys := (metadataGroupLimit - 128) / 384
	require.Equal(t, metadataGroupLimit, exactKeys*384+128)
	require.NoError(t, metadataGroupCharge([]*solana.Transaction{{Message: solana.Message{AccountKeys: keys[:exactKeys]}}}, 0))
	require.ErrorIs(t, metadataGroupCharge([]*solana.Transaction{{Message: solana.Message{AccountKeys: keys[:exactKeys+1]}}}, 0), errMetadataBudget)

	validTx := transferTransactions(t, 1, 7)[0]
	for _, tc := range []struct {
		name    string
		txs     []*solana.Transaction
		workers int
	}{
		{"transaction count", txs, 0},
		{"key count", []*solana.Transaction{{Message: solana.Message{AccountKeys: keys}}}, 0},
		{"group charge", []*solana.Transaction{{Message: solana.Message{AccountKeys: keys[:exactKeys+1]}}}, 0},
		{"worker profile", []*solana.Transaction{validTx}, metadataWorkerLimit + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capture blockMetadataCapture
			var group *transactionMetadataGroup
			allocations := testing.AllocsPerRun(10, func() {
				capture = blockMetadataCapture{}
				// No SlotCtx exists: admission must stop before hydration.
				group = capture.beginGroup(nil, tc.txs, tc.workers)
			})
			require.Nil(t, group)
			require.ErrorIs(t, capture.Err, errMetadataBudget)
			require.Nil(t, capture.records)
			require.Zero(t, capture.budget.used.Load())
			require.Zero(t, allocations, "refusal guard only, not full execution")
		})
	}
}

func TestMetadataBudgetPrepareRefusesBeforeReads(t *testing.T) {
	tx := transferTransactions(t, 1, 8)[0]
	var budget metadataBudget
	budget.used.Store(metadataBlockLimit)
	metadata, err := prepareTransactionMetadata(nil, tx, &budget)
	require.ErrorIs(t, err, errMetadataBudget)
	require.Nil(t, metadata)
	require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
}

func TestMetadataBudgetExecutionCharge(t *testing.T) {
	ctx := &sealevel.ExecutionCtx{
		TransactionContext: &sealevel.TransactionCtx{},
		Log:                &sealevel.LogRecorder{Logs: []string{"abc", ""}},
		InnerInstrs: []sealevel.RecordedInnerInstr{
			{Data: []byte{1, 2, 3, 4}, Accounts: []uint8{1, 2}},
			{Data: []byte{5}},
		},
	}
	ctx.TransactionContext.SetReturnData(solana.PublicKey{1}, []byte{1, 2, 3, 4, 5})
	const charge = 2*256 + 2*(4+2+1) + 2*32 + 3 + 5
	var budget metadataBudget
	budget.used.Store(metadataBlockLimit - charge)
	metadata := transactionMetadata{charge: metadataRecordLimit - charge}
	require.NoError(t, metadata.reserveExecution(&budget, ctx))
	require.Equal(t, uint64(metadataRecordLimit), metadata.charge)
	require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
	metadata = transactionMetadata{charge: metadataRecordLimit - charge + 1}
	before := metadata.charge
	require.ErrorIs(t, metadata.reserveExecution(nil, ctx), errMetadataBudget)
	require.Equal(t, before, metadata.charge)
}

func TestMetadataBudgetCPIAllocatedCapacity(t *testing.T) {
	ctx := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{}}
	data, indices := make([]byte, 8193), make([]uint8, 255)
	for range 32 {
		ctx.InnerInstrs = append(ctx.InnerInstrs, sealevel.RecordedInnerInstr{Data: data, Accounts: indices})
	}
	metadata := transactionMetadata{charge: metadataRecordBaseCharge}
	require.NoError(t, metadata.reserveExecution(nil, ctx))
	assembled := AssembleInnerInstructions(ctx)
	ownedCapacity := uint64(cap(assembled)) * uint64(unsafe.Sizeof(InnerInstructionsList{}))
	for _, group := range assembled {
		ownedCapacity += uint64(cap(group.Instructions)) * uint64(unsafe.Sizeof(CompiledInstruction{}))
		for _, instruction := range group.Instructions {
			ownedCapacity += uint64(cap(instruction.Data) + cap(instruction.Accounts))
		}
	}
	t.Logf("CPI count=%d payload_length=%d copied_data_capacity=%d owned_capacity=%d charge=%d", len(ctx.InnerInstrs), len(data), cap(assembled[0].Instructions[0].Data), ownedCapacity, metadata.charge)
	require.GreaterOrEqual(t, metadata.charge, ownedCapacity, "charge must cover copied capacities, not only payload lengths")

	// The collector can retain this many attempted invocations, but the
	// conservative final-copy budget may make the record unavailable. This
	// does not claim that 64 successful CPIs fit the execution trace limit.
	ctx.InnerInstrs = append(ctx.InnerInstrs, ctx.InnerInstrs...)
	metadata = transactionMetadata{charge: metadataRecordBaseCharge}
	require.ErrorIs(t, metadata.finish(nil, transferTransactions(t, 1, 10)[0], LoadAndExecuteTransactionOutput{ExecCtx: ctx}, 0, nil), errMetadataBudget)
	require.Nil(t, metadata.InnerInstructions)
}

func TestMetadataBudgetFinishRefusesBeforeCopies(t *testing.T) {
	tx := transferTransactions(t, 1, 9)[0]
	for _, kind := range []string{"logs", "inner instructions", "return data"} {
		t.Run(kind, func(t *testing.T) {
			ctx := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{}}
			switch kind {
			case "logs":
				ctx.Log = &sealevel.LogRecorder{Logs: []string{"owned only after admission"}}
			case "inner instructions":
				ctx.InnerInstrs = []sealevel.RecordedInnerInstr{{Data: []byte{1}, Accounts: []uint8{0}}}
			case "return data":
				ctx.TransactionContext.SetReturnData(solana.PublicKey{1}, []byte{1})
			}
			var budget metadataBudget
			budget.used.Store(metadataBlockLimit)
			metadata := transactionMetadata{charge: metadataRecordBaseCharge, AccountKeys: tx.Message.AccountKeys}
			var err error
			allocations := testing.AllocsPerRun(10, func() {
				// Nil SlotCtx makes a late post-state read fail loudly. This is
				// a collector-output boundary test, not a VM execution fixture.
				err = metadata.finish(nil, tx, LoadAndExecuteTransactionOutput{ExecCtx: ctx}, 0, &budget)
			})
			require.ErrorIs(t, err, errMetadataBudget)
			require.Equal(t, uint64(metadataRecordBaseCharge), metadata.charge)
			require.Nil(t, metadata.PostBalances)
			require.Nil(t, metadata.InnerInstructions)
			require.Nil(t, metadata.LogMessages)
			require.Nil(t, metadata.ReturnData)
			require.Nil(t, metadata.Error)
			require.Zero(t, allocations, "no owned post-state/collector copies before admission")
		})
	}
}

func TestMetadataBudgetErrorJSONBoundary(t *testing.T) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx := signLifecycleInstructions(t, system.NewTransferInstruction(1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
	metadata, err := prepareTransactionMetadata(ctx, tx, nil)
	require.NoError(t, err)
	output := LoadAndExecuteTransactionOutput{ProcessingResult: TransactionProcessingResult{TransactionError: &TransactionError{
		ErrorType: TransactionErrorInstructionError, InstructionError: errors.New(strings.Repeat("\x00", metadataErrorTextLimit)),
	}}}
	require.NoError(t, metadata.finish(ctx, tx, output, 0, nil))
	require.JSONEq(t, `{"InstructionError":[0,"`+strings.Repeat(`\u0000`, metadataErrorTextLimit)+`"]}`, string(metadata.Error))
	require.Less(t, len(metadata.Transaction)+len(metadata.Error), metadataRecordBaseCharge)
	output.ProcessingResult.TransactionError.InstructionError = errors.New(strings.Repeat("x", metadataErrorTextLimit+1))
	metadata = &transactionMetadata{AccountKeys: tx.Message.AccountKeys}
	require.ErrorIs(t, metadata.finish(nil, tx, output, 0, nil), errMetadataBudget)
	require.Nil(t, metadata.PostBalances)
	require.Nil(t, metadata.Error)
}

func TestMetadataBudgetSurvivesCaptureReset(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(fmt.Sprintf("exhausted=%t", exhausted), func(t *testing.T) {
			var baseline []*accounts.Account
			var baselineFee fees.TxFeeInfo
			var baselineUnits uint64
			for enabled := range 2 {
				ctx, cleanup := newCommitTestSlotCtx()
				func() {
					defer cleanup()
					tx := signLifecycleInstructions(t, system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
					var budget metadataBudget
					if exhausted {
						budget.used.Store(metadataBlockLimit)
					}
					var capture *transactionMetadataCapture
					if enabled == 1 {
						capture = &transactionMetadataCapture{Record: &transactionMetadata{}, Err: errors.New("stale"), budget: &budget}
					}
					var wg sync.WaitGroup
					fee, units, err := processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
					wg.Wait()
					require.NoError(t, err)
					require.NotNil(t, fee)
					state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
					if enabled == 0 {
						baseline, baselineFee, baselineUnits = state, *fee, units
						return
					}
					require.Equal(t, baseline, state)
					require.Equal(t, baselineFee, *fee)
					require.Equal(t, baselineUnits, units)
					require.Same(t, &budget, capture.budget)
					if exhausted {
						require.ErrorIs(t, capture.Err, errMetadataBudget)
						require.Nil(t, capture.Record)
						require.Equal(t, uint64(metadataBlockLimit), budget.used.Load())
					} else {
						require.NoError(t, capture.Err)
						require.NotNil(t, capture.Record)
						require.Equal(t, capture.Record.charge, budget.used.Load())
						require.Positive(t, budget.used.Load())
					}
				}()
			}
		})
	}
}

func TestMetadataBudgetStreamingExhaustionPreservesExecution(t *testing.T) {
	txs := transferTransactions(t, 5, 100)
	wireSize, err := txverify.TransactionWireSize(txs[0])
	require.NoError(t, err)
	charge := uint64(metadataRecordBaseCharge - solana.MaxTransactionSizeV1 + wireSize + len(txs[0].Message.AccountKeys)*metadataRecordKeyCharge)
	// Native transfers now capture two stable logs, including per-entry overhead.
	charge += uint64(2*32 + len("Program "+solana.SystemProgramID.String()+" invoke [1]") + len("Program "+solana.SystemProgramID.String()+" success"))
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			var outcomes [2]groupExecutionOutcome
			var states [2][]*accounts.Account
			for enabled := range 2 {
				env := newGroupExecutionEnv(t, workers, 10_000_000)
				func() {
					defer env.cleanup()
					defer env.exec.close()
					var capture blockMetadataCapture
					if enabled == 1 {
						// Seed prior aggregate usage instead of allocating 64 MiB.
						capture.budget.used.Store(metadataBlockLimit - 3*charge)
						env.exec.metadata = &capture
					}
					require.NoError(t, env.exec.executeTransactionGroup(txs[:2], nil, false))
					if enabled == 1 {
						require.NoError(t, capture.Err)
						require.Len(t, capture.records, 2)
						require.Equal(t, uint64(metadataBlockLimit)-charge, capture.budget.used.Load())
					}
					require.NoError(t, env.exec.executeTransactionGroup(txs[2:4], nil, false))
					require.NoError(t, env.exec.executeTransactionGroup(txs[4:], nil, false))
					outcomes[enabled] = blockMetadataGroupOutcome(t, env)
					states[enabled] = metadataFailureState(t, env.exec.slotCtx.Accounts, txs[0].Message.AccountKeys)
					if enabled == 1 {
						require.ErrorIs(t, capture.Err, errMetadataBudget)
						require.Nil(t, capture.Record)
						require.Nil(t, capture.records, "no earlier group records may survive overflow")
						require.Equal(t, uint64(metadataBlockLimit), capture.budget.used.Load())
					}
				}()
			}
			require.Equal(t, outcomes[0], outcomes[1])
			require.Equal(t, states[0], states[1])
		})
	}
}

func TestMetadataBudgetWorkerRefusalPreservesExecution(t *testing.T) {
	txs := transferTransactions(t, metadataWorkerLimit+1, 200)
	var outcomes [2]groupExecutionOutcome
	var states [2][]*accounts.Account
	for enabled := range 2 {
		env := newGroupExecutionEnv(t, metadataWorkerLimit+1, 10_000_000)
		func() {
			defer env.cleanup()
			defer env.exec.close()
			var capture blockMetadataCapture
			if enabled == 1 {
				env.exec.metadata = &capture
			}
			require.NoError(t, env.exec.executeTransactionGroup(txs, nil, false))
			outcomes[enabled] = blockMetadataGroupOutcome(t, env)
			states[enabled] = metadataFailureState(t, env.exec.slotCtx.Accounts, txs[0].Message.AccountKeys)
			if enabled == 1 {
				require.ErrorIs(t, capture.Err, errMetadataBudget)
				require.Nil(t, capture.records)
				require.Zero(t, capture.budget.used.Load())
			}
		}()
	}
	require.Equal(t, outcomes[0], outcomes[1])
	require.Equal(t, states[0], states[1])
}

func TestMetadataBudgetProcessBlockStartsFresh(t *testing.T) {
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 1, 300)
	want := lifecycleWholeBlock(t, env, txs, 0)
	tail := &lifecycleTail{durable: env.durable}
	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	capture := blockMetadataCapture{Record: &blockMetadata{}, Err: errors.New("previous block"), records: []*transactionMetadata{{}}}
	capture.budget.used.Store(metadataBlockLimit)
	ctx, err := processBlock(env.acctsDb, block, env.epochSchedule, 0, nil,
		&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, &capture)
	require.NoError(t, err)
	require.NoError(t, capture.Err)
	require.NotNil(t, capture.Record)
	require.Len(t, capture.Record.Transactions, 1)
	require.Equal(t, capture.Record.Transactions[0].charge, capture.budget.used.Load())
	require.Less(t, capture.budget.used.Load(), uint64(metadataBlockLimit))
	got := lifecycleOutcomeOf(t, ctx, tail)
	requireSameLifecycleOutcome(t, want, got)
	require.Equal(t, want.delta, got.delta)
}
