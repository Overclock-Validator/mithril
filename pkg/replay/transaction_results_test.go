package replay

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func resultSessionFor(t *testing.T, block *b.Block) (*TransactionResultSession, *txstatus.ResultCache) {
	t.Helper()
	cache, err := txstatus.NewResultCache(txstatus.ResultBankID{Slot: block.ParentSlot, Blockhash: block.LastBlockhash})
	require.NoError(t, err)
	session, err := NewTransactionResultSession(cache, "verifying")
	require.NoError(t, err)
	return session, cache
}

func TestTransactionResultsCommitParity(t *testing.T) {
	for _, kind := range []string{"success", "rollback", "fees-only", "no-op", "rejected"} {
		for _, removeADH := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/adhRemoved%v", kind, removeADH), func(t *testing.T) {
				var baseline []*accounts.Account
				var baselineModified, baselineWritable map[solana.PublicKey]bool
				var baselineFee, baselineCU uint64
				var baselineErr string
				for _, enabled := range []bool{false, true} {
					ctx, cleanup := newCommitTestSlotCtx()
					func() {
						defer cleanup()
						if removeADH {
							ctx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
						}
						instructions := []solana.Instruction{system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build()}
						if kind == "rollback" {
							instructions = append(instructions, system.NewTransferInstruction(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
						}
						tx := signLifecycleInstructions(t, instructions...)
						if kind == "fees-only" {
							missingProgram := solana.PublicKey{0xD6}
							require.NoError(t, ctx.SetAccount(missingProgram, &accounts.Account{Key: missingProgram}))
							tx = signLifecycleInstructions(t, solana.NewInstruction(missingProgram, nil, nil))
						}
						if kind == "no-op" || kind == "rejected" {
							ctx.Features.EnableFeature(features.EnableTxV1, 0)
							if kind == "no-op" {
								ctx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)
							}
							tx = signedV1Transfer(t, solana.TransactionConfig{}.WithComputeUnitLimit(12_345).WithLoadedAccountsDataSizeLimit(54_321))
							payer, err := ctx.GetAccount(txfixture.PayerPubkey())
							require.NoError(t, err)
							payer.Owner = addresses.VoteProgramAddr
							require.NoError(t, ctx.SetAccount(txfixture.PayerPubkey(), payer))
						}
						var capture *transactionResultCapture
						if enabled {
							capture = &transactionResultCapture{}
							if kind == "rejected" {
								capture.recorded = true
								capture.result.Signature = solana.Signature{255}
							}
						}
						var wg sync.WaitGroup
						fee, cu, executionErr := processTransactionWithResult(ctx, &wg, tx, nil, nil, nil, false, nil, capture)
						wg.Wait()
						require.Equal(t, kind != "success", executionErr != nil)
						require.Equal(t, kind == "rejected", fee == nil)
						var feeTotal uint64
						if fee != nil {
							feeTotal = fee.TotalFee
						}
						var state []*accounts.Account
						for _, key := range tx.Message.AccountKeys {
							account, err := ctx.GetAccount(key)
							require.NoError(t, err)
							state = append(state, account.Clone())
						}
						if !enabled {
							baseline, baselineFee, baselineCU = state, feeTotal, cu
							baselineErr = fmt.Sprint(executionErr)
							baselineModified, baselineWritable = maps.Clone(ctx.ModifiedAccts), maps.Clone(ctx.WritableAccts)
							return
						}
						require.Equal(t, baseline, state)
						require.Equal(t, baselineModified, ctx.ModifiedAccts)
						require.Equal(t, baselineWritable, ctx.WritableAccts)
						require.Equal(t, baselineFee, feeTotal)
						require.Equal(t, baselineCU, cu)
						require.Equal(t, baselineErr, fmt.Sprint(executionErr))
						require.NoError(t, capture.err)
						require.Equal(t, kind != "rejected", capture.recorded, "committed outcome must be observed")
						if kind == "rejected" {
							require.Zero(t, capture.result)
							return
						}
						require.Equal(t, tx.Signatures[0], capture.result.Signature)
						switch kind {
						case "success":
							require.Nil(t, capture.result.Error, "success is nil, not JSON null")
							require.Equal(t, uint64(10_000_008), state[1].Lamports)
						case "rollback":
							require.JSONEq(t, `{"InstructionError":[1,{"Custom":1}]}`, string(capture.result.Error))
							require.Equal(t, uint64(10_000_000), state[1].Lamports)
							require.Equal(t, uint64(5_000), feeTotal)
						case "no-op":
							require.JSONEq(t, `"InvalidAccountForFee"`, string(capture.result.Error))
							require.Zero(t, feeTotal)
							require.Equal(t, uint64(12_345), cu)
						case "fees-only":
							require.JSONEq(t, `"ProgramAccountNotFound"`, string(capture.result.Error))
							require.Equal(t, uint64(5_000), feeTotal)
							require.Zero(t, cu)
							require.Equal(t, uint64(10_000_000_000-5_000), state[0].Lamports)
						}
					}()
				}
			})
		}
	}
}

func TestTransactionResultsWholeBlockAcceptance(t *testing.T) {
	for _, workers := range []int{0, 4} {
		for _, mode := range []string{"history off", "history busy", "rejected", "cache unavailable"} {
			t.Run(fmt.Sprintf("workers%d/%s", workers, mode), func(t *testing.T) {
				env := newLifecycleEnv(t)
				txs := transferTransactions(t, 12, 999_000)
				want := lifecycleWholeBlock(t, env, txs, workers)
				block := env.block(txs)
				block.MarkTransactionSignaturesVerified()
				session, cache := resultSessionFor(t, block)
				if mode == "cache unavailable" {
					cache.Invalidate()
				}
				var history *TransactionHistorySession
				if mode == "history busy" {
					entered, release := make(chan struct{}), make(chan struct{})
					history = &TransactionHistorySession{write: func(transactionhistory.Block) error { close(entered); <-release; return nil }}
					defer func() { close(release); requireHistorySessionStopped(t, history, nil) }()
					capture := history.begin()
					capture.Record = &blockMetadata{Slot: 6}
					history.finish(capture, true)
					<-entered
				}
				tail := &lifecycleTail{durable: env.durable}
				ctx, err := processBlockWithCaptures(env.acctsDb, block, env.epochSchedule, workers, nil,
					&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, history, session)
				require.NoError(t, err)
				require.Equal(t, want, lifecycleOutcomeOf(t, ctx, tail))
				require.True(t, session.current.sealed)
				require.True(t, session.current.bank.Complete)
				signatures := make([]solana.Signature, len(txs))
				for i, tx := range txs {
					signatures[i] = tx.Signatures[0]
				}
				before, err := cache.Lookup(signatures)
				if mode != "cache unavailable" {
					require.NoError(t, err)
					for _, result := range before.Results {
						require.False(t, result.Found)
					}
				}
				if mode == "rejected" {
					// Models a summary recorder or Engine.OnReplayResult rejection;
					// this is not a complete ReplayBlocks consensus-engine fixture.
					session.discard()
					got, err := cache.Lookup(signatures)
					require.NoError(t, err)
					require.Equal(t, before, got)
					return
				}
				err = session.acceptBlock(block)
				require.Nil(t, session.current)
				if mode == "cache unavailable" {
					require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
					return
				}
				require.NoError(t, err)
				got, err := cache.Lookup(signatures)
				require.NoError(t, err)
				require.Equal(t, resultBlockID(block), got.Tip)
				for i, result := range got.Results {
					require.True(t, result.Found)
					require.False(t, result.Finalized)
					if i < 2 {
						require.Nil(t, result.Error)
					} else {
						require.JSONEq(t, `{"InsufficientFundsForRent":{"account_index":0}}`, string(result.Error))
					}
				}
			})
		}
	}
}

func TestTransactionResultsStreamAndAbort(t *testing.T) {
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 12, 999_000)
	for _, suffix := range []int{0, 3, 12} {
		t.Run(fmt.Sprintf("suffix%d", suffix), func(t *testing.T) {
			var capture blockResultCapture
			splits := []int{3, 7}
			if suffix == len(txs) {
				splits = nil
			}
			want, _ := lifecycleStream(t, env, txs, splits, suffix, 4)
			got, _ := lifecycleStreamResults(t, env, txs, splits, suffix, 4, nil, &capture)
			require.Equal(t, want, got)
			require.NoError(t, capture.err)
			require.True(t, capture.sealed)
			require.True(t, capture.bank.Complete)
			require.Len(t, capture.bank.Results, len(txs))
			for i, result := range capture.bank.Results {
				require.Equal(t, txs[i].Signatures[0], result.Signature)
			}
		})
	}
	h := newStreamingTestHarness(t)
	var capture blockResultCapture
	h.env.exec.results = &capture
	require.NoError(t, h.env.exec.executeTransactionGroup(txs[:1], nil, false))
	require.Len(t, capture.records, 1)
	require.Error(t, h.env.exec.executeTransactionGroup(txs[:1], nil, false), "duplicate aborts the candidate")
	h.exec.discard("duplicate result candidate")
	require.Error(t, capture.err)
	require.Nil(t, capture.records)
	require.False(t, capture.sealed)
}

func TestTransactionResultsBoundsErrorsAndOwnership(t *testing.T) {
	tx := transferTransactions(t, 1, 1)[0]
	index := uint8(0)
	for _, code := range []uint32{0, 6001} {
		var capture transactionResultCapture
		capture.finish(tx, &TransactionError{ErrorType: TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: sealevel.InstrErrCustomCode{Code: code}})
		require.NoError(t, capture.err)
		require.JSONEq(t, fmt.Sprintf(`{"InstructionError":[0,{"Custom":%d}]}`, code), string(capture.result.Error))
	}
	for _, instructionErr := range []error{sealevel.InstrErrCustom, fmt.Errorf("%s", strings.Repeat("x", metadataErrorTextLimit+1))} {
		var capture transactionResultCapture
		capture.finish(tx, &TransactionError{ErrorType: TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: instructionErr})
		require.Error(t, capture.err)
		require.False(t, capture.recorded)
	}
	for _, malformed := range []*TransactionError{
		{ErrorType: TransactionErrorType(-1)},
		{ErrorType: TransactionErrorType(999)},
		{ErrorType: TransactionErrorInstructionError, InstructionError: sealevel.InstrErrInvalidArgument},
		{ErrorType: TransactionErrorInstructionError, InstructionIndex: &index},
		{ErrorType: TransactionErrorInsufficientFundsForRent},
		{ErrorType: TransactionErrorProgramExecutionTemporarilyRestricted},
		{ErrorType: TransactionErrorDuplicateInstruction},
	} {
		var capture transactionResultCapture
		capture.finish(tx, malformed)
		require.ErrorIs(t, capture.err, txstatus.ErrResultInput)
		require.False(t, capture.recorded)
	}
	var cumulative blockResultCapture
	require.Len(t, cumulative.beginGroup(txstatus.MaxResultBankEntries), txstatus.MaxResultBankEntries)
	require.Nil(t, cumulative.beginGroup(1), "streaming groups share one count ceiling")
	require.Nil(t, cumulative.records)
	var capture blockResultCapture
	require.Nil(t, capture.beginGroup(txstatus.MaxResultBankEntries+1))
	require.Nil(t, capture.records, "count bound precedes allocation")
	block := &b.Block{Slot: 8, ParentSlot: 7, Blockhash: [32]byte{8}, LastBlockhash: [32]byte{7}}
	session, cache := resultSessionFor(t, block)
	session.current = &capture
	capture.seal(block)
	require.ErrorIs(t, session.acceptBlock(block), txstatus.ErrResultUnavailable)
	_, err := cache.Lookup([]solana.Signature{tx.Signatures[0]})
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable, "a known capture gap is not a clean miss")
	empty := &b.Block{Slot: 9, ParentSlot: 8, Blockhash: [32]byte{9}, LastBlockhash: [32]byte{8}}
	session.begin().seal(empty)
	require.NoError(t, session.acceptBlock(empty), "empty produced bank is a complete node")
	session.begin().seal(&b.Block{Slot: 10, ParentSlot: 9, Blockhash: [32]byte{10}, LastBlockhash: [32]byte{9}})
	require.ErrorIs(t, session.acceptBlock(empty), txstatus.ErrResultLineage)
	require.True(t, session.disabled)
}

func TestTransactionResultsObserverAndDisabled(t *testing.T) {
	block := &b.Block{ParentSlot: 7, LastBlockhash: [32]byte{7}}
	session, cache := resultSessionFor(t, block)
	_, err := NewTransactionResultSession(cache, "validator")
	require.Error(t, err)
	valid := ConsensusOpts{Alpenglow: true, Engine: historySessionEngine{}, TransactionResults: session}
	require.NoError(t, session.validateReplay(&valid, nil))
	for _, opts := range []ConsensusOpts{{}, {Alpenglow: true}, {Alpenglow: true, Engine: historySessionEngine{voting: true}}} {
		opts.TransactionResults = session
		result := ReplayBlocks(context.Background(), nil, "", nil, nil, 8, 8, nil, "", "", "", "", "", 0, "", nil, "", 0, false, false, false, nil, nil, nil, nil, &opts, nil)
		require.Error(t, result.Error)
	}
	require.Error(t, session.validateReplay(&valid, &BlockFetchOpts{LocalBlocks: make(chan *b.Block)}))
	require.Error(t, session.validateReplay(&valid, &BlockFetchOpts{LocalLeaderForSlot: func(uint64) bool { return false }}))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		var disabled *TransactionResultSession
		var capture *blockResultCapture
		if disabled.begin() != nil || capture.beginGroup(3) != nil || disabled.acceptBlock(nil) != nil {
			t.Fatal("disabled capture admitted work")
		}
		disabled.discard()
		capture.seal(nil)
	}))
	session.disable()
	_, err = cache.Lookup(nil)
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
}

func TestTransactionResultsMissingAcceptedCapture(t *testing.T) {
	first := &b.Block{Slot: 8, ParentSlot: 7, Blockhash: [32]byte{8}, LastBlockhash: [32]byte{7}}
	session, cache := resultSessionFor(t, first)
	require.ErrorIs(t, session.acceptBlock(first), txstatus.ErrResultUnavailable)
	second := &b.Block{Slot: 9, ParentSlot: 8, Blockhash: [32]byte{9}, LastBlockhash: [32]byte{8}}
	tx := transferTransactions(t, 1, 1)[0]
	second.Transactions = []*solana.Transaction{tx}
	capture := session.begin()
	capture.beginGroup(1).transaction(0).finish(tx, nil)
	capture.seal(second)
	require.NoError(t, session.acceptBlock(second), "a real later capture retains exact accepted lineage")
	got, err := cache.Lookup([]solana.Signature{tx.Signatures[0]})
	require.NoError(t, err)
	require.True(t, got.Results[0].Found)
	require.Equal(t, uint64(9), got.Results[0].Slot)
	_, err = cache.Lookup([]solana.Signature{tx.Signatures[0], {255}})
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable, "the earlier gap cannot become a clean missing result")
	require.ErrorIs(t, session.acceptBlock(second), txstatus.ErrResultInput, "duplicate consumption cannot become empty success")
	_, err = cache.Lookup([]solana.Signature{tx.Signatures[0]})
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
	session, cache = resultSessionFor(t, first)
	require.ErrorIs(t, session.acceptBlock(nil), txstatus.ErrResultLineage)
	_, err = cache.Lookup(nil)
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
}
