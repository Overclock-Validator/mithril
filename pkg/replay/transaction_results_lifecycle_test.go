package replay

import (
	"encoding/base64"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This pins owner wiring, not a substitute for driving an entire ReplayBlocks
// retry. The runner supplies the exact source file used by its frozen overlay.
func TestTransactionResultsLifecycleOwnerWiring(t *testing.T) {
	filename := os.Getenv("RESULT_LIFECYCLE_BLOCK")
	if filename == "" {
		filename = "block.go"
	}
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	require.NoError(t, err)
	rootCalls, callbackCalls, sourceOnly := 0, 0, 0
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "applyPromotionBookkeeping" {
				callbackCalls++
			}
		}
		if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Lhs) == 1 && len(assign.Rhs) == 1 {
			name, named := assign.Lhs[0].(*ast.Ident)
			fn, closure := assign.Rhs[0].(*ast.FuncLit)
			if named && closure && name.Name == "applyPromotionBookkeeping" {
				ast.Inspect(fn.Body, func(inner ast.Node) bool {
					if call, ok := inner.(*ast.CallExpr); ok {
						if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "publishRootedTransactionResults" {
							rootCalls++
						}
					}
					return true
				})
			}
		}
		if branch, ok := node.(*ast.IfStmt); ok {
			negated, ok := branch.Cond.(*ast.UnaryExpr)
			if !ok || negated.Op != token.NOT {
				return true
			}
			call, ok := negated.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != "parentSwitchNeedsStateUnwind" {
				return true
			}
			sourceOnly++
			ast.Inspect(branch.Body, func(inner ast.Node) bool {
				if name, ok := inner.(*ast.Ident); ok {
					require.NotEqual(t, "transactionResults", name.Name, "source-only branch must not mutate result generation")
				}
				return true
			})
		}
		return true
	})
	require.Equal(t, 1, rootCalls, "real promotion owner must call result publication")
	require.Equal(t, 2, callbackCalls, "async and forced-fold paths share the owner callback")
	require.Equal(t, 2, sourceOnly, "certified and source-repair switches preserve the executed parent")
}

type resultPublicationProbe struct {
	pending      bool
	root         uint64
	onGate       func(bool)
	onRoot       func(uint64)
	onInvalidate func()
	invalidated  bool
}

func (*resultPublicationProbe) SetSlotCtx(*sealevel.SlotCtx) {}
func (p *resultPublicationProbe) InvalidateHealth() {
	if p.onInvalidate != nil {
		p.onInvalidate()
	}
	p.invalidated = true
}
func (p *resultPublicationProbe) SetHistoryRecoveryPending(pending bool) {
	if p.onGate != nil {
		p.onGate(pending)
	}
	p.pending = pending
}
func (p *resultPublicationProbe) SetRootedBankState(slot, _, _ uint64) {
	if p.onRoot != nil {
		p.onRoot(slot)
	}
	p.root = slot
}

func resultLifecycleContext(slot uint64) *state.ResumeContext {
	hash := solana.Hash{byte(slot)}
	return &state.ResumeContext{Slot: slot, Bankhash: hash.String(), Blockhash: hash.String(),
		AcctsLtHash:       base64.StdEncoding.EncodeToString(make([]byte, 2048)),
		RecentBlockhashes: []state.BlockhashEntry{{Blockhash: hash.String()}}, EvictedBlockhash: solana.Hash{99}.String()}
}

func resultLifecycleBank(slot, parent uint64, complete bool) txstatus.ResultBank {
	return txstatus.ResultBank{ID: txstatus.ResultBankID{Slot: slot, Blockhash: solana.Hash{byte(slot)}},
		Parent: txstatus.ResultBankID{Slot: parent, Blockhash: solana.Hash{byte(parent)}}, Complete: complete,
		Results: []txstatus.SignatureResult{{Signature: solana.Signature{byte(slot)}}}}
}

func TestTransactionResultsLifecycleSeed(t *testing.T) {
	for _, mismatch := range []string{"none", "stale suffix", "same slot fork", "zero hash"} {
		t.Run(mismatch, func(t *testing.T) {
			session, cache := resultSessionFor(t, &b.Block{ParentSlot: 5, LastBlockhash: solana.Hash{5}})
			parent := txstatus.ResultBankID{Slot: 5, Blockhash: solana.Hash{5}}
			switch mismatch {
			case "stale suffix":
				require.NoError(t, cache.Publish(resultLifecycleBank(8, 5, true)))
			case "same slot fork":
				parent.Blockhash[1] = 1
			case "zero hash":
				parent.Blockhash = solana.Hash{}
			}
			err := session.checkParent(parent)
			_, lookupErr := cache.Lookup(nil)
			if mismatch == "none" {
				require.NoError(t, err)
				require.NoError(t, lookupErr)
			} else {
				require.ErrorIs(t, err, txstatus.ErrResultLineage)
				require.ErrorIs(t, lookupErr, txstatus.ErrResultUnavailable)
			}
		})
	}
}

func TestTransactionResultsLifecycleFold(t *testing.T) {
	for _, force := range []bool{false, true} {
		for _, optionalFailure := range []bool{false, true} {
			session, cache := resultSessionFor(t, &b.Block{ParentSlot: 1, LastBlockhash: solana.Hash{1}})
			fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 5}
			batch := 2
			if force {
				batch = 3 // forced partial chunk, as at shutdown
			}
			tail := newUnrootedTail(&fakeDurable{}, fc, 512, batch, "")
			parent := uint64(1)
			for _, slot := range []uint64{5, 8} {
				require.NoError(t, cache.Publish(resultLifecycleBank(slot, parent, true)))
				tail.Add(slot, []*accounts.Account{testAccount(1, slot)}, testHashBytes(byte(slot)))
				tail.SetContext(slot, resultLifecycleContext(slot))
				parent = slot
			}
			job, err := tail.buildFoldJob(9, force) // 9 is a skipped frontier, not a produced bank.
			require.NoError(t, err)
			require.NotNil(t, job)
			require.Equal(t, uint64(8), job.through)
			before, err := cache.Lookup(nil)
			require.NoError(t, err)
			require.ErrorContains(t, runFoldJob(fc, job), "commit boom")
			after, err := cache.Lookup(nil)
			require.NoError(t, err)
			require.Equal(t, before, after, "failed durable fold cannot advance result root")
			fc.failOn = 0
			// The independent success case needs a fresh job; failed jobs release their payload.
			job, err = tail.buildFoldJob(9, force)
			require.NoError(t, err)
			require.NotNil(t, job)
			require.NoError(t, runFoldJob(fc, job))
			require.Equal(t, []uint64{5, 8}, fc.committed)
			rooted := tail.applyFoldJob(job)
			if optionalFailure {
				rooted.Blockhash = "bad-0"
			}
			publisher := &resultPublicationProbe{onRoot: func(slot uint64) {
				require.Equal(t, uint64(8), slot)
				view, err := cache.Lookup(nil)
				if optionalFailure {
					require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
				} else {
					require.NoError(t, err)
					require.Equal(t, resultLifecycleBank(8, 5, true).ID, view.Root, "cache root must precede RPC root")
				}
			}}
			publishRootedTransactionResults(session, publisher, job.through, rooted)
			require.Equal(t, uint64(8), publisher.root, "optional cache failure cannot suppress account root")
			publishRootedTransactionResults(nil, publisher, job.through, rooted)
			require.Equal(t, uint64(8), publisher.root)
		}
	}
}

func TestTransactionResultsLifecycleTailUnwind(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	for _, mode := range []string{"success", "missing sysvars", "cache unavailable", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			session, cache := resultSessionFor(t, &b.Block{ParentSlot: 5, LastBlockhash: solana.Hash{5}})
			require.NoError(t, cache.Publish(resultLifecycleBank(7, 5, true)))
			require.NoError(t, cache.Publish(resultLifecycleBank(8, 7, true)))
			if mode == "disabled" {
				session = nil
			}
			tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
			tail.Add(7, []*accounts.Account{testAccount(1, 71)}, testHashBytes(7))
			var sysvars *sealevel.BankSysvars
			if mode != "missing sysvars" {
				sysvars = testUnwindBankSysvars(t, 7, 700)
			}
			tail.SetContext(7, resultLifecycleContext(7), sysvars)
			tail.Add(8, []*accounts.Account{testAccount(1, 81)}, testHashBytes(8))
			publisher := &resultPublicationProbe{}
			publisher.onGate = func(pending bool) {
				if pending {
					account, err := tail.GetAccount(0, testKey(1))
					require.NoError(t, err)
					require.Equal(t, uint64(81), account.Lamports, "recovery gate must close before account eviction")
					view, err := cache.Lookup([]solana.Signature{{8}})
					require.NoError(t, err)
					require.True(t, view.Results[0].Found)
				}
			}
			publisher.onInvalidate = func() {
				require.Equal(t, mode != "disabled", publisher.pending)
				account, err := tail.GetAccount(0, testKey(1))
				require.NoError(t, err)
				require.Equal(t, uint64(81), account.Lamports, "health must invalidate before account eviction")
			}
			rs, _, reason := tryInLoopUnwindWithResults(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{},
				&sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, 0, nil, session, publisher, publisher)
			require.True(t, publisher.invalidated)
			account, err := tail.GetAccount(0, testKey(1))
			require.NoError(t, err)
			require.Equal(t, uint64(71), account.Lamports)
			require.Equal(t, mode != "disabled", publisher.pending, "mutated tail must remain gated until owner finishes")
			if mode == "missing sysvars" {
				require.Nil(t, rs)
				require.Equal(t, unwindFallbackMissingSysvars, reason)
				return
			}
			require.NotNil(t, rs)
			require.Equal(t, [32]byte{7}, rs.LastBlockhash)
			if mode == "cache unavailable" {
				cache.Invalidate()
			}
			session.finishUnwind(publisher, rs)
			require.False(t, publisher.pending, "valid account recovery must reopen even if status failed")
			view, err := cache.Lookup([]solana.Signature{{7}, {8}})
			if mode == "cache unavailable" {
				require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
			} else {
				require.NoError(t, err)
				require.True(t, view.Results[0].Found)
				require.Equal(t, mode == "disabled", view.Results[1].Found)
			}
		})
	}
}

func TestTransactionResultsLifecycleTermination(t *testing.T) {
	newSession := func() (*TransactionResultSession, *txstatus.ResultCache) {
		s, cache := resultSessionFor(t, &b.Block{ParentSlot: 5, LastBlockhash: solana.Hash{5}})
		require.NoError(t, cache.Publish(resultLifecycleBank(7, 5, true)))
		return s, cache
	}
	t.Run("retry retains outcomes until exact recovery", func(t *testing.T) {
		s, cache := newSession()
		p := &resultPublicationProbe{}
		s.replayStopped(p, &CertifiedSwitch{Slot: 7})
		require.True(t, p.pending)
		view, err := cache.Lookup([]solana.Signature{{7}})
		require.NoError(t, err)
		require.True(t, view.Results[0].Found)
		s.endReplay(p, true)
		require.NoError(t, s.Unwind(txstatus.ResultBankID{Slot: 5, Blockhash: solana.Hash{5}}))
	})
	t.Run("non retryable", func(t *testing.T) {
		s, cache := newSession()
		s.replayStopped(&resultPublicationProbe{}, errors.New("execution stopped"))
		_, err := cache.Lookup([]solana.Signature{{7}})
		require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
		require.ErrorIs(t, s.Reset(txstatus.ResultBankID{Slot: 5, Blockhash: solana.Hash{5}}), txstatus.ErrResultUnavailable)
	})
	t.Run("panic preserves original failure and closes visibility", func(t *testing.T) {
		s, cache := newSession()
		p := &resultPublicationProbe{}
		s.begin()
		require.PanicsWithValue(t, "worker failure", func() {
			defer s.endReplay(p, false)
			panic("worker failure")
		})
		require.Nil(t, s.current)
		require.True(t, p.pending)
		_, err := cache.Lookup(nil)
		require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
	})
	t.Run("source only skipped suffix", func(t *testing.T) {
		_, cache := newSession()
		before, err := cache.Lookup(nil)
		require.NoError(t, err)
		// This is the real branch predicate; a source cursor alone is not a bank.
		require.False(t, parentSwitchNeedsStateUnwind(8, 7))
		after, err := cache.Lookup(nil)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}
