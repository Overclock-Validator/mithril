package replay

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// memBlockSource serves a fixed parent state to the group loader the way the
// batch loader sees AccountsDB/the unrooted tail: a missing key yields a
// zero-lamport placeholder, never an error.
type memBlockSource struct {
	mem accounts.MemAccounts
}

func (s *memBlockSource) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, err := s.mem.GetAccountWithoutLock(pubkey); err == nil {
		return acct.Clone(), nil
	}
	return &accounts.Account{Key: pubkey}, nil
}

func (s *memBlockSource) GetAccountsBatch(_ context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i], _ = s.GetAccount(slot, pk)
	}
	return out, nil
}

// groupExecutionEnv is a block execution over a MemAccounts parent snapshot
// and overlay, mirroring what ProcessBlock builds through the account loader,
// with the process-wide sysvar cache set the way newCommitTestSlotCtx sets it.
type groupExecutionEnv struct {
	exec    *blockExecution
	parent  accounts.MemAccounts
	source  *memBlockSource
	cleanup func()
}

func newGroupExecutionEnv(t *testing.T, txParallelism int, payerLamports uint64) *groupExecutionEnv {
	t.Helper()
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)

	durable := accounts.NewMemAccounts()
	_ = durable.SetAccountWithoutLock(addresses.SystemProgramAddr, &accounts.Account{
		Key: addresses.SystemProgramAddr, Lamports: 1, Owner: addresses.NativeLoaderAddr, Executable: true, RentEpoch: math.MaxUint64,
	})
	_ = durable.SetAccountWithoutLock(txfixture.PayerPubkey(), &accounts.Account{
		Key: txfixture.PayerPubkey(), Lamports: payerLamports, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64,
	})
	_ = durable.SetAccountWithoutLock(txfixture.DestPubkey(), &accounts.Account{
		Key: txfixture.DestPubkey(), Lamports: 10_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64,
	})

	prevRBH := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	rbh := sealevel.SysvarRecentBlockhashes{{Blockhash: txfixture.TestBlockhash(), FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &rbh
	prevRent := sealevel.SysvarCache.Rent.Sysvar
	rentSysvar := sealevel.NewDefaultRentSysvar()
	sealevel.SysvarCache.Rent.Sysvar = &rentSysvar

	parent := accounts.NewMemAccounts()
	overlay := accounts.NewOverlayAccounts(parent)
	block := &b.Block{Slot: 42, Features: feats}
	slotCtx := &sealevel.SlotCtx{
		Accounts:        overlay,
		ParentAccts:     parent,
		Slot:            block.Slot,
		Features:        feats,
		FeeRateGovernor: &sealevel.FeeRateGovernor{PrevLamportsPerSignature: 5000},
		LastBlockhash:   txfixture.TestBlockhash(),
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
		VoteTimestampMu: &sync.Mutex{},
		VoteTimestamps:  make(map[solana.PublicKey]sealevel.BlockTimestamp),
		Replay:          true,
	}
	source := &memBlockSource{mem: durable}
	exec := &blockExecution{
		block:          block,
		txParallelism:  txParallelism,
		blockSrc:       source,
		slotCtx:        slotCtx,
		parentAccts:    parent,
		accts:          overlay,
		ctx:            context.Background(),
		setReplayStage: func(string) {},
		seenMessages:   make(map[[32]byte]int),
	}
	return &groupExecutionEnv{
		exec:   exec,
		parent: parent,
		source: source,
		cleanup: func() {
			sealevel.SysvarCache.RecentBlockHashes.Sysvar = prevRBH
			sealevel.SysvarCache.Rent.Sysvar = prevRent
		},
	}
}

func (env *groupExecutionEnv) lamports(t *testing.T, key solana.PublicKey) uint64 {
	t.Helper()
	acct, err := env.exec.slotCtx.GetAccountShared(key)
	require.NoError(t, err)
	return acct.Lamports
}

func (env *groupExecutionEnv) modifiedKeys() []string {
	keys := make([]string, 0, len(env.exec.slotCtx.ModifiedAccts))
	for key := range env.exec.slotCtx.ModifiedAccts {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	return keys
}

func transferTransactions(t *testing.T, n int, firstSeq uint64) []*solana.Transaction {
	t.Helper()
	txs := make([]*solana.Transaction, n)
	for i := range txs {
		tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(firstSeq + uint64(i)))
		require.NoError(t, err)
		txs[i] = tx
	}
	return txs
}

type groupExecutionOutcome struct {
	payer, dest         uint64
	fees                uint64
	cu                  uint64
	processed, sigs     uint64
	executeMask         []bool
	modified            []string
	parentPayerLamports uint64
}

func runGroups(t *testing.T, txParallelism int, payerLamports uint64, txs []*solana.Transaction, splits []int) groupExecutionOutcome {
	t.Helper()
	env := newGroupExecutionEnv(t, txParallelism, payerLamports)
	defer env.cleanup()
	start := 0
	for _, end := range append(append([]int(nil), splits...), len(txs)) {
		if end < start {
			end = start
		}
		require.NoError(t, env.exec.executeTransactionGroup(txs[start:end], nil, false))
		start = end
	}
	parentPayer, err := env.parent.GetAccountWithoutLock(txfixture.PayerPubkey())
	require.NoError(t, err)
	return groupExecutionOutcome{
		payer:               env.lamports(t, txfixture.PayerPubkey()),
		dest:                env.lamports(t, txfixture.DestPubkey()),
		fees:                env.exec.txFeeAccumulator.TotalFees,
		cu:                  env.exec.totalCU,
		processed:           env.exec.processedTxCount,
		sigs:                env.exec.processedSignatures,
		executeMask:         append([]bool(nil), env.exec.execute...),
		modified:            env.modifiedKeys(),
		parentPayerLamports: parentPayer.Lamports,
	}
}

// The reference is the pre-existing sequential path: ProcessTransaction over
// the block order on a slot context whose accounts were preloaded whole.
func runSequentialReference(t *testing.T, payerLamports uint64, txs []*solana.Transaction) groupExecutionOutcome {
	t.Helper()
	env := newGroupExecutionEnv(t, 0, payerLamports)
	defer env.cleanup()
	keys := []solana.PublicKey{addresses.SystemProgramAddr, txfixture.PayerPubkey(), txfixture.DestPubkey()}
	for _, key := range keys {
		acct, _ := env.source.GetAccount(42, key)
		pk := [32]byte(key)
		require.NoError(t, env.parent.SetAccount(&pk, acct))
	}
	var sigverify sync.WaitGroup
	var out groupExecutionOutcome
	for _, tx := range txs {
		feeInfo, cu, _ := ProcessTransaction(env.exec.slotCtx, &sigverify, tx, nil, nil, nil, false)
		require.NotNil(t, feeInfo)
		out.fees += feeInfo.TotalFee
		out.cu += cu
		out.processed++
		out.sigs += uint64(tx.Message.Header.NumRequiredSignatures)
		out.executeMask = append(out.executeMask, true)
	}
	sigverify.Wait()
	out.payer = env.lamports(t, txfixture.PayerPubkey())
	out.dest = env.lamports(t, txfixture.DestPubkey())
	out.modified = env.modifiedKeys()
	out.parentPayerLamports = payerLamports
	return out
}

// TestExecuteTransactionGroupMatchesWholeBlock runs the same block-ordered
// transfers as one group, as random groups, and through the sequential
// reference. Every transfer shares the payer and destination, so every group
// boundary is a cross-group write dependency, and the payer balance is small
// enough that later transfers fail for insufficient funds, which exercises
// fee charging on failed transactions and makes outcomes order-dependent.
func TestExecuteTransactionGroupMatchesWholeBlock(t *testing.T) {
	// Amounts are 999,001+ lamports each (seq%1e6+1), so with a 5 SOL-ish
	// payer of 5,000,000 lamports the first few transfers succeed and the
	// rest fail inside the System program while still paying their fee.
	const payerLamports = 5_000_000
	txs := transferTransactions(t, 48, 999_000)
	reference := runSequentialReference(t, payerLamports, txs)
	require.Less(t, reference.payer, uint64(payerLamports))

	for _, txParallelism := range []int{0, 1, 4} {
		single := runGroups(t, txParallelism, payerLamports, txs, nil)
		require.Equal(t, reference.payer, single.payer, "txpar %d single group payer", txParallelism)
		require.Equal(t, reference.dest, single.dest, "txpar %d single group dest", txParallelism)
		require.Equal(t, reference.fees, single.fees)
		require.Equal(t, reference.cu, single.cu)
		require.Equal(t, reference.processed, single.processed)
		require.Equal(t, reference.sigs, single.sigs)
		require.Equal(t, reference.executeMask, single.executeMask)
		require.Equal(t, reference.modified, single.modified)
		require.Equal(t, uint64(payerLamports), single.parentPayerLamports, "parent image must stay pristine")

		rng := rand.New(rand.NewSource(int64(7 + txParallelism)))
		for iter := 0; iter < 8; iter++ {
			splitCount := 1 + rng.Intn(6)
			splits := make([]int, splitCount)
			for i := range splits {
				splits[i] = rng.Intn(len(txs) + 1)
			}
			sort.Ints(splits)
			grouped := runGroups(t, txParallelism, payerLamports, txs, splits)
			require.Equal(t, single, grouped, "txpar %d splits %v", txParallelism, splits)
		}
	}
}

func TestExecuteTransactionGroupRejectsDuplicatesAcrossGroups(t *testing.T) {
	env := newGroupExecutionEnv(t, 2, 10_000_000_000)
	defer env.cleanup()
	txs := transferTransactions(t, 3, 100)
	require.NoError(t, env.exec.executeTransactionGroup(txs[:2], nil, false))
	err := env.exec.executeTransactionGroup([]*solana.Transaction{txs[2], txs[0]}, nil, false)
	var duplicateErr *DuplicateTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &duplicateErr))
	require.Equal(t, uint64(42), duplicateErr.Slot)
	require.Equal(t, uint64(1), duplicateErr.DuplicateCount)
	require.Equal(t, []DuplicateTransactionOccurrence{{Index: 3, FirstIndex: 0}}, duplicateErr.Occurrences)
	// The rejected group must not have been recorded or executed.
	require.Len(t, env.exec.transactions, 2)
	require.Equal(t, uint64(2), env.exec.processedTxCount)
}

func TestExecuteTransactionGroupRejectsV1BeforeActivation(t *testing.T) {
	env := newGroupExecutionEnv(t, 2, 10_000_000_000)
	defer env.cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedV1Wire(9, 8))
	require.NoError(t, err)
	err = env.exec.executeTransactionGroup([]*solana.Transaction{tx}, nil, false)
	require.ErrorIs(t, err, TxErrUnsupportedVersion)
	require.Empty(t, env.exec.transactions)
}

func TestExecuteTransactionGroupKeepsFirstParentImage(t *testing.T) {
	env := newGroupExecutionEnv(t, 2, 10_000_000_000)
	defer env.cleanup()
	txs := transferTransactions(t, 4, 200)
	require.NoError(t, env.exec.executeTransactionGroup(txs[:2], nil, false))
	afterFirst := env.lamports(t, txfixture.PayerPubkey())
	require.Less(t, afterFirst, uint64(10_000_000_000))
	// A later group touching the same accounts must not reload the payer's
	// parent image over the pristine one, nor see stale overlay state.
	require.NoError(t, env.exec.executeTransactionGroup(txs[2:], nil, false))
	parentPayer, err := env.parent.GetAccountWithoutLock(txfixture.PayerPubkey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000_000), parentPayer.Lamports)
	require.Less(t, env.lamports(t, txfixture.PayerPubkey()), afterFirst)
	require.Equal(t, 2, env.exec.groups)
	require.Len(t, env.exec.transactions, 4)
}

func TestExecuteTransactionGroupRefusesClosedExecution(t *testing.T) {
	env := newGroupExecutionEnv(t, 2, 10_000_000_000)
	defer env.cleanup()
	env.exec.closed = true
	err := env.exec.executeTransactionGroup(transferTransactions(t, 1, 300), nil, false)
	require.ErrorIs(t, err, errBlockExecutionClosed)
}
