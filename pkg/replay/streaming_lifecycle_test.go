package replay

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

// The lifecycle equivalence gate: the same block executed whole by
// ProcessBlock and executed as a stream (bank opened on a transaction-less
// shell, groups fed through the executor, finalize against the complete
// block) must produce the same bank hash, the same committed account delta,
// the same signature/fee/compute totals and the same status publication.
// The bank is self-contained: an in-memory tail stands in for the unrooted
// working set, the parent bank sysvars are an explicit snapshot, rent
// rewrites are skipped (the rent scan needs a real AccountsDB), and the
// AccountsDB is only referenced for its directory.

// lifecycleTail is an unrooted working set over a fixed durable memory: reads
// resolve to clones (or a zero-lamport placeholder), commits are recorded.
type lifecycleTail struct {
	unrootedState
	durable accounts.MemAccounts
	added   []lifecycleCommit
}

type lifecycleCommit struct {
	slot     uint64
	delta    []*accounts.Account
	bankhash []byte
}

func (t *lifecycleTail) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, err := t.durable.GetAccountWithoutLock(pubkey); err == nil {
		return acct.Clone(), nil
	}
	return &accounts.Account{Key: pubkey}, nil
}

func (t *lifecycleTail) GetAccountsBatch(_ context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i], _ = t.GetAccount(slot, pk)
	}
	return out, nil
}

func (t *lifecycleTail) Add(slot uint64, delta []*accounts.Account, bankhash []byte) {
	t.added = append(t.added, lifecycleCommit{slot: slot, delta: delta, bankhash: append([]byte(nil), bankhash...)})
}

func (t *lifecycleTail) OverCap() bool { return false }

type lifecycleEnv struct {
	leader        solana.PublicKey
	feats         *features.Features
	durable       accounts.MemAccounts
	acctsDb       *accountsdb.AccountsDb
	epochSchedule *sealevel.SysvarEpochSchedule
	parent        *sealevel.BankSysvars
}

const (
	lifecycleParentSlot = uint64(7)
	lifecycleSlot       = uint64(8)
)

var lifecycleParentBlockID = solana.Hash{0xAA, 0xBB}

// ensureBorrowedAccountArenas gives parallelTxLoop the per-worker arena slots
// the node installs at startup (nil arenas are accepted by ProcessTransaction).
func ensureBorrowedAccountArenas(t *testing.T, n int) {
	t.Helper()
	if len(sealevel.BorrowedAccountArenas) >= n {
		return
	}
	prev := sealevel.BorrowedAccountArenas
	sealevel.BorrowedAccountArenas = make([]*arena.Arena[sealevel.BorrowedAccount], n)
	t.Cleanup(func() { sealevel.BorrowedAccountArenas = prev })
}

func newLifecycleEnv(t *testing.T) *lifecycleEnv {
	t.Helper()
	ensureBorrowedAccountArenas(t, 4)
	// Bank open publishes derived sysvars to the legacy process-global cache;
	// leave it as we found it for the rest of the package.
	sysvarCacheBefore := sealevel.SysvarCache
	t.Cleanup(func() { sealevel.SysvarCache = sysvarCacheBefore })
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	feats.EnableFeature(features.SkipRentRewrites, 0)

	durable := accounts.NewMemAccounts()
	_ = durable.SetAccountWithoutLock(addresses.SystemProgramAddr, &accounts.Account{
		Key: addresses.SystemProgramAddr, Lamports: 1, Owner: addresses.NativeLoaderAddr, Executable: true, RentEpoch: ^uint64(0),
	})
	_ = durable.SetAccountWithoutLock(txfixture.PayerPubkey(), &accounts.Account{
		Key: txfixture.PayerPubkey(), Lamports: 3_200_000, Owner: addresses.SystemProgramAddr, RentEpoch: ^uint64(0),
	})
	_ = durable.SetAccountWithoutLock(txfixture.DestPubkey(), &accounts.Account{
		Key: txfixture.DestPubkey(), Lamports: 10_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: ^uint64(0),
	})

	// The parent bank's sysvar snapshot, as the retained parent context would
	// hold it. RecentBlockhashes carries the fixture blockhash so the transfers
	// are age-valid.
	clock := sealevel.SysvarClock{Slot: lifecycleParentSlot, EpochStartTimestamp: 111, UnixTimestamp: 222}
	slotHashes := sealevel.SysvarSlotHashes{{Slot: lifecycleParentSlot - 1, Hash: [32]byte{0x61}}}
	recent := sealevel.SysvarRecentBlockhashes{{
		Blockhash:     txfixture.TestBlockhash(),
		FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5_000},
	}}
	slotHistory := sealevel.SysvarSlotHistory{
		Bits: sealevel.SlotHistoryBitvec{
			Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{0x81}},
			Len:  64,
		},
		NextSlot: lifecycleSlot,
	}
	stakeHistory := sealevel.SysvarStakeHistory{{Epoch: 0, Entry: sealevel.StakeHistoryEntry{Effective: 91}}}
	lastRestart := sealevel.SysvarLastRestartSlot{LastRestartSlot: 3}
	epochSchedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}
	rent := sealevel.NewDefaultRentSysvar()
	parent, err := sealevel.NewBankSysvars(lifecycleParentSlot,
		&accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: slotHashes.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: slotHistory.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: marshalStakeHistoryForParentLoader(t, &stakeHistory)},
		&accounts.Account{Key: sealevel.SysvarLastRestartSlotAddr, Lamports: 1, Data: marshalLastRestartSlotForParentLoader(t, lastRestart)},
		&accounts.Account{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1, Data: marshalEpochScheduleForParentLoader(t, epochSchedule)},
		&accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()},
	)
	require.NoError(t, err)
	require.NoError(t, parent.ValidateForExecution())

	return &lifecycleEnv{
		feats:         feats,
		durable:       durable,
		acctsDb:       &accountsdb.AccountsDb{AcctsDir: t.TempDir()},
		epochSchedule: &epochSchedule,
		parent:        parent,
	}
}

// block builds the slot's block as the loop would have configured it on the
// executed parent; txs nil is the streaming shell.
func (env *lifecycleEnv) block(txs []*solana.Transaction) *b.Block {
	return &b.Block{
		Leader:                    env.leader,
		Slot:                      lifecycleSlot,
		VoteTimestamps:            make(map[solana.PublicKey]sealevel.BlockTimestamp),
		Epoch:                     0,
		ParentSlot:                lifecycleParentSlot,
		ParentBankhash:            [32]byte{0x88},
		Blockhash:                 [32]byte{0x99},
		LastBlockhash:             [32]byte{0x77},
		Features:                  env.feats,
		Transactions:              txs,
		PrevFeeRateGovernor:       &sealevel.FeeRateGovernor{TargetLamportsPerSignature: 5_000, LamportsPerSignature: 5_000},
		FromLiveStream:            true,
		SourceParentSlot:          lifecycleParentSlot,
		AlpenglowParentBlockID:    lifecycleParentBlockID,
		HasAlpenglowParentBlockID: true,
	}
}

type lifecycleOutcome struct {
	rewards       []rpc.BlockReward
	bankhash      []byte
	numSignatures uint64
	computeUnits  uint64
	lamportsBurnt uint64
	delta         map[solana.PublicKey]*accounts.Account
}

func lifecycleOutcomeOf(t *testing.T, slotCtx *sealevel.SlotCtx, tail *lifecycleTail) lifecycleOutcome {
	t.Helper()
	require.NotNil(t, slotCtx)
	require.Len(t, tail.added, 1, "the bank commits exactly once")
	require.Equal(t, slotCtx.Slot, tail.added[0].slot)
	require.Equal(t, slotCtx.FinalBankhash, tail.added[0].bankhash)
	delta := make(map[solana.PublicKey]*accounts.Account, len(tail.added[0].delta))
	for _, acct := range tail.added[0].delta {
		delta[acct.Key] = acct
	}
	require.Contains(t, delta, txfixture.PayerPubkey())
	return lifecycleOutcome{
		bankhash:      append([]byte(nil), slotCtx.FinalBankhash...),
		numSignatures: slotCtx.NumSignatures,
		computeUnits:  slotCtx.TotalComputeUnitsConsumed,
		lamportsBurnt: slotCtx.LamportsBurnt,
		delta:         delta,
	}
}

func requireSameLifecycleOutcome(t *testing.T, want, got lifecycleOutcome) {
	t.Helper()
	require.NotEmpty(t, want.bankhash)
	require.Equal(t, want.bankhash, got.bankhash, "bank hash")
	require.Equal(t, want.numSignatures, got.numSignatures)
	require.Equal(t, want.computeUnits, got.computeUnits)
	require.Equal(t, want.lamportsBurnt, got.lamportsBurnt)
	wantKeys := make([]string, 0, len(want.delta))
	for key := range want.delta {
		wantKeys = append(wantKeys, key.String())
	}
	gotKeys := make([]string, 0, len(got.delta))
	for key := range got.delta {
		gotKeys = append(gotKeys, key.String())
	}
	sort.Strings(wantKeys)
	sort.Strings(gotKeys)
	require.Equal(t, wantKeys, gotKeys, "committed account set")
	for key, acct := range want.delta {
		other := got.delta[key]
		require.Equal(t, acct.Lamports, other.Lamports, "%s lamports", key)
		require.Equal(t, acct.Owner, other.Owner, "%s owner", key)
		require.Equal(t, acct.Data, other.Data, "%s data", key)
		require.Equal(t, acct.Executable, other.Executable, "%s executable", key)
	}
}

func lifecycleWholeBlock(t *testing.T, env *lifecycleEnv, txs []*solana.Transaction, txParallelism int) lifecycleOutcome {
	t.Helper()
	tail := &lifecycleTail{durable: env.durable}
	statuses := NewTransactionStatusCache()
	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	slotCtx, err := ProcessBlock(env.acctsDb, block, env.epochSchedule, txParallelism, nil, &persistedTracker{}, tail, statuses, false, env.parent)
	require.NoError(t, err)
	outcome := lifecycleOutcomeOf(t, slotCtx, tail)
	outcome.rewards = block.Rewards
	return outcome
}

// lifecycleStream opens the bank on a transaction-less shell, feeds txs in
// the given batch splits through the executor, and finalizes against the
// complete block; suffixTxs of the transactions are never streamed and
// execute at finalize.
func lifecycleStream(t *testing.T, env *lifecycleEnv, txs []*solana.Transaction, splits []int, suffixTxs int, workers int) (lifecycleOutcome, *streamingExecutor) {
	t.Helper()
	tail := &lifecycleTail{durable: env.durable}
	statuses := NewTransactionStatusCache()
	shell := env.block(nil)
	exec := newBlockExecution(env.acctsDb, shell, env.epochSchedule, workers, nil, &persistedTracker{}, tail, statuses, false, env.parent)
	require.NoError(t, exec.open())
	exec.slotCtx.DeferVoteCachePublication = true
	exec.slotCtx.TrackProgramCacheAdds = true

	feed := newFakeStreamFeed()
	gen := turbine.NewDetachedStreamGeneration(lifecycleSlot)
	feed.status[gen] = turbine.StreamActive
	s := newStreamingExecutor(streamingDeps{
		feed:                feed,
		epochSchedule:       env.epochSchedule,
		tail:                tail,
		transactionStatuses: statuses,
		alpenglowMode:       true,
		unrootedTailUsed:    true,
		frontier:            func() uint64 { return lifecycleParentSlot },
		lastSlotCtx:         func() *sealevel.SlotCtx { return nil },
		currentFeatures:     func() *features.Features { return env.feats },
		currentEpoch:        func() uint64 { return 0 },
		rewardsInFlight:     func() bool { return false },
		switchPending:       func() bool { return false },
		executedBlockID:     func(uint64) (solana.Hash, bool) { return lifecycleParentBlockID, true },
	})
	s.current = &streamingSlot{
		slot: lifecycleSlot, generation: gen, parentSlot: lifecycleParentSlot, parentID: lifecycleParentBlockID,
		exec: exec, pending: make(map[uint32]*turbine.StreamBatch), openedAt: time.Now(), headerAt: time.Now(),
	}
	header := turbine.NewDetachedStreamMarker(gen, 0, 0, turbine.StreamMarkerHeader, lifecycleParentSlot, lifecycleParentBlockID)
	s.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen, Batch: header})

	streamed := txs[:len(txs)-suffixTxs]
	start, next := 0, uint32(1)
	for _, end := range append(append([]int(nil), splits...), len(streamed)) {
		if end <= start {
			continue
		}
		group := streamed[start:end]
		batch := turbine.NewDetachedStreamBatch(gen, next, next+uint32(len(group))-1, group, verifiedIdentities(t, group))
		s.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen, Batch: batch})
		next += uint32(len(group))
		start = end
	}
	require.NotNil(t, s.current, "no group may have discarded the stream (%s)", metrics.GlobalBlockReplay.StreamingExecution.DiscardReason)
	sameTransactions(t, streamed, s.current.origin)
	sameCopies(t, streamed, exec.transactions)

	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	slotCtx, ok, err := s.finalize(block, env.parent)
	require.NoError(t, err)
	require.True(t, ok, "the stream must accept its own block")
	require.Nil(t, s.current)
	require.True(t, exec.closed)
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.StreamingExecution.Opened)
	require.Equal(t, uint64(len(txs)), metrics.GlobalBlockReplay.StreamingExecution.Transactions)
	outcome := lifecycleOutcomeOf(t, slotCtx, tail)
	outcome.rewards = block.Rewards
	return outcome, s
}

func TestStreamingLifecycleRecordsExecutedFeeRewards(t *testing.T) {
	previousStreaming, previousSchedule := StreamingExecutionCfg, global.ManageLeaderSchedule()
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	global.SetManageLeaderSchedule(true)
	t.Cleanup(func() {
		StreamingExecutionCfg = previousStreaming
		global.SetManageLeaderSchedule(previousSchedule)
	})
	txs := transferTransactions(t, 4, 1_200)
	leader := txfixture.DestPubkey()
	wholeEnv := newLifecycleEnv(t)
	wholeEnv.leader = leader
	whole := lifecycleWholeBlock(t, wholeEnv, txs, 2)
	require.Len(t, whole.rewards, 1)
	require.Equal(t, rpc.RewardTypeFee, whole.rewards[0].RewardType)
	require.Equal(t, leader, whole.rewards[0].Pubkey)
	require.Equal(t, int64(10_000), whole.rewards[0].Lamports)
	require.Equal(t, whole.delta[leader].Lamports, whole.rewards[0].PostBalance)

	streamedEnv := newLifecycleEnv(t)
	streamedEnv.leader = leader
	streamed, _ := lifecycleStream(t, streamedEnv, txs, []int{2}, 1, 2)
	requireSameLifecycleOutcome(t, whole, streamed)
	require.Equal(t, whole.rewards, streamed.rewards)
}

func TestStreamingLifecycleMatchesWholeBlock(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	// Transfers of 999,001+ lamports at a 5,000-lamport fee from a
	// 3,200,000-lamport payer: the first two succeed, the rest fail for rent
	// and still pay, so every group boundary is a write dependency on the
	// payer and the outcome is order-dependent.
	txs := transferTransactions(t, 12, 999_000)
	txCountBefore := global.TransactionCount()

	whole := lifecycleWholeBlock(t, newLifecycleEnv(t), txs, 0)
	require.Equal(t, txCountBefore+uint64(len(txs)), global.TransactionCount())
	wholeParallel := lifecycleWholeBlock(t, newLifecycleEnv(t), txs, 4)
	requireSameLifecycleOutcome(t, whole, wholeParallel)

	cases := []struct {
		name      string
		splits    []int
		suffixTxs int
		workers   int
	}{
		{"one group, no suffix", nil, 0, 1},
		{"three groups, no suffix", []int{3, 7}, 0, 4},
		{"per-transaction groups", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 0, 2},
		{"two groups and a suffix", []int{4}, 3, 4},
		{"everything in the suffix", nil, 12, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := global.TransactionCount()
			streamed, _ := lifecycleStream(t, newLifecycleEnv(t), txs, tc.splits, tc.suffixTxs, tc.workers)
			requireSameLifecycleOutcome(t, whole, streamed)
			require.Equal(t, before+uint64(len(txs)), global.TransactionCount())
		})
	}
}

// A stream discarded after executing groups leaves the durable view and the
// tail untouched, and the same block then executes whole to the same result.
func TestStreamingLifecycleDiscardThenWholeBlock(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	for _, reason := range []string{"update_parent", "sigverify_timeout"} {
		t.Run(reason, func(t *testing.T) {
			txs := transferTransactions(t, 8, 999_000)
			env := newLifecycleEnv(t)
			whole := lifecycleWholeBlock(t, env, txs, 2)

			tail := &lifecycleTail{durable: env.durable}
			statuses := NewTransactionStatusCache()
			shell := env.block(nil)
			exec := newBlockExecution(env.acctsDb, shell, env.epochSchedule, 2, nil, &persistedTracker{}, tail, statuses, false, env.parent)
			require.NoError(t, exec.open())
			feed := newFakeStreamFeed()
			gen := turbine.NewDetachedStreamGeneration(lifecycleSlot)
			feed.status[gen] = turbine.StreamActive
			s := newStreamingExecutor(streamingDeps{feed: feed, epochSchedule: env.epochSchedule, tail: tail, transactionStatuses: statuses})
			s.current = &streamingSlot{slot: lifecycleSlot, generation: gen, parentSlot: lifecycleParentSlot, parentID: lifecycleParentBlockID,
				exec: exec, pending: make(map[uint32]*turbine.StreamBatch), openedAt: time.Now(), headerAt: time.Now(), nextStart: 1}
			s.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen,
				Batch: turbine.NewDetachedStreamBatch(gen, 1, 5, txs[:5], verifiedIdentities(t, txs[:5]))})
			sameTransactions(t, txs[:5], s.current.origin)
			sameCopies(t, txs[:5], exec.transactions)
			payerNow, err := exec.slotCtx.GetAccountShared(txfixture.PayerPubkey())
			require.NoError(t, err)
			require.Less(t, payerNow.Lamports, uint64(3_200_000), "the overlay saw the executed prefix")

			if reason == "sigverify_timeout" {
				s.waitVerificationFn = func(context.Context, *turbine.StreamBatch) ([]txverify.VerifiedMessageIdentity, bool, error) {
					return nil, false, context.DeadlineExceeded
				}
				s.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen, Batch: turbine.NewDetachedStreamBatch(gen, 6, 8, txs[5:], verifiedIdentities(t, txs[5:]))})
				require.Nil(t, s.current)
			} else {
				s.discard(reason)
			}
			require.Empty(t, tail.added, "a discarded stream commits nothing")
			durablePayer, err := env.durable.GetAccountWithoutLock(txfixture.PayerPubkey())
			require.NoError(t, err)
			require.Equal(t, uint64(3_200_000), durablePayer.Lamports, "the durable view is untouched")

			again := lifecycleWholeBlock(t, env, txs, 2)
			requireSameLifecycleOutcome(t, whole, again)

		})
	}
}

// V0 / address-lookup-table coverage. Resolving lookups mutates the message
// object (SetAddressTables refuses a second call; ResolveLookups appends to
// AccountKeys), so a stream must execute its own copies and leave the block's
// objects for the whole-block path — which may run them against a different
// parent, with a different table.

var lifecycleTableKey = solana.PublicKey{0x7A, 0xB1, 0xE0}

// lookupTableAccount is an active address lookup table whose only entry is
// dest, encoded the way the ALT program stores it.
func lookupTableAccount(t *testing.T, dest solana.PublicKey) *accounts.Account {
	t.Helper()
	var buf bytes.Buffer
	enc := bin.NewBinEncoder(&buf)
	require.NoError(t, enc.WriteUint32(sealevel.AddressLookupTableProgramStateLookupTable, bin.LE))
	authority := txfixture.PayerPubkey()
	meta := sealevel.LookupTableMeta{DeactivationSlot: ^uint64(0), LastExtendedSlot: 1, Authority: &authority}
	require.NoError(t, meta.MarshalWithEncoder(enc))
	require.NoError(t, enc.WriteBytes(dest[:], false))
	require.Equal(t, sealevel.AddressLookupTableMetaSize+32, buf.Len())
	return &accounts.Account{Key: lifecycleTableKey, Lamports: 1_000_000, Owner: addresses.AddressLookupTableAddr, Data: buf.Bytes(), RentEpoch: ^uint64(0)}
}

func systemTransferData(lamports uint64) []byte {
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data, 2) // SystemInstruction::Transfer
	binary.LittleEndian.PutUint64(data[4:], lamports)
	return data
}

// signedV0TransferViaTableWire is a signed v0 transfer from the fixture payer
// to entry 0 of lifecycleTableKey (a writable lookup): static keys are the
// payer and the System program, so the destination is account index 2.
func signedV0TransferViaTableWire(t *testing.T, seq uint64) []byte {
	t.Helper()
	msg := solana.Message{
		Header:          solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys:     solana.PublicKeySlice{txfixture.PayerPubkey(), solana.SystemProgramID},
		RecentBlockhash: txfixture.TestBlockhash(),
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
			Accounts:       []uint16{0, 2},
			Data:           systemTransferData(1_000 + seq),
		}},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{{AccountKey: lifecycleTableKey, WritableIndexes: []uint8{0}}},
	}
	_, err := msg.SetVersion(solana.MessageVersionV0)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg}
	payerKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(txfixture.PayerPubkey()) {
			return &payerKey
		}
		return nil
	})
	require.NoError(t, err)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	return wire
}

// decodeTransactions decodes wires the way turbine does, so each call yields
// fresh, unresolved objects.
func decodeTransactions(t *testing.T, wires [][]byte) []*solana.Transaction {
	t.Helper()
	txs := make([]*solana.Transaction, len(wires))
	for i, wire := range wires {
		tx, err := solana.TransactionFromBytes(wire)
		require.NoError(t, err)
		require.Equal(t, solana.MessageVersionV0, tx.Message.GetVersion())
		require.False(t, tx.Message.IsResolved())
		txs[i] = tx
	}
	return txs
}

// newLifecycleEnvWithTable is newLifecycleEnv with a well-funded payer, the
// lookup table pointing at dest, and dest as an existing rent-exempt account,
// so every v0 transfer succeeds and the credited destination shows which
// table resolved it.
func newLifecycleEnvWithTable(t *testing.T, dest solana.PublicKey) *lifecycleEnv {
	t.Helper()
	env := newLifecycleEnv(t)
	_ = env.durable.SetAccountWithoutLock(txfixture.PayerPubkey(), &accounts.Account{
		Key: txfixture.PayerPubkey(), Lamports: 10_000_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: ^uint64(0),
	})
	_ = env.durable.SetAccountWithoutLock(dest, &accounts.Account{
		Key: dest, Lamports: 10_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: ^uint64(0),
	})
	_ = env.durable.SetAccountWithoutLock(lifecycleTableKey, lookupTableAccount(t, dest))
	return env
}

// A stream refuses a batch whose transactions already carry resolution:
// their account keys were derived against a parent the stream cannot vouch
// for. The assembler never produces one; this pins the refusal.
func TestStreamingRefusesResolvedInput(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	txs := decodeTransactions(t, [][]byte{signedV0TransferViaTableWire(t, 1), signedV0TransferViaTableWire(t, 2)})
	identities := verifiedIdentities(t, txs)
	require.NoError(t, txs[1].Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{lifecycleTableKey: {{0xD1}}}))
	require.NoError(t, txs[1].Message.ResolveLookups())

	h := newStreamingTestHarness(t)
	h.exec.handleEvent(h.event(turbine.NewDetachedStreamBatch(h.gen, 1, 2, txs, identities)))
	require.Nil(t, h.exec.current)
	require.Equal(t, "resolved_input", h.discardReason())
	require.False(t, txs[0].Message.IsResolved(), "the unresolved sibling is untouched")
}

func TestStreamingLifecycleV0LookupsMatchWholeBlock(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	dest := solana.PublicKey{0xDA}
	wires := make([][]byte, 6)
	for i := range wires {
		wires[i] = signedV0TransferViaTableWire(t, uint64(i))
	}
	whole := lifecycleWholeBlock(t, newLifecycleEnvWithTable(t, dest), decodeTransactions(t, wires), 2)
	require.Contains(t, whole.delta, dest)
	require.Equal(t, uint64(10_000_000+6*1_000+0+1+2+3+4+5), whole.delta[dest].Lamports, "every transfer reached the table's entry")

	orig := decodeTransactions(t, wires)
	streamed, _ := lifecycleStream(t, newLifecycleEnvWithTable(t, dest), orig, []int{2}, 2, 2)
	requireSameLifecycleOutcome(t, whole, streamed)
	for i := 0; i < 4; i++ {
		require.False(t, orig[i].Message.IsResolved(), "streamed transaction %d ran as a copy", i)
	}
	for i := 4; i < 6; i++ {
		require.True(t, orig[i].Message.IsResolved(), "suffix transaction %d ran as the block's own object, like whole-block execution", i)
	}
}

// A v0 prefix executed on a stream against parent A is discarded; the same
// authoritative objects then execute whole against parent B, whose table
// names a different destination. The block's objects must still resolve
// (they were never touched), B's destination must be the one credited, and
// the result must equal a fresh-decoded whole-block reference on B.
func TestStreamingLifecycleDiscardedV0PrefixResolvesAgainstTheNewParent(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	destA, destB := solana.PublicKey{0xA1}, solana.PublicKey{0xB2}
	wires := make([][]byte, 6)
	for i := range wires {
		wires[i] = signedV0TransferViaTableWire(t, uint64(10+i))
	}
	orig := decodeTransactions(t, wires)

	envA := newLifecycleEnvWithTable(t, destA)
	tail := &lifecycleTail{durable: envA.durable}
	statuses := NewTransactionStatusCache()
	exec := newBlockExecution(envA.acctsDb, envA.block(nil), envA.epochSchedule, 2, nil, &persistedTracker{}, tail, statuses, false, envA.parent)
	require.NoError(t, exec.open())
	feed := newFakeStreamFeed()
	gen := turbine.NewDetachedStreamGeneration(lifecycleSlot)
	feed.status[gen] = turbine.StreamActive
	s := newStreamingExecutor(streamingDeps{feed: feed, epochSchedule: envA.epochSchedule, tail: tail, transactionStatuses: statuses})
	s.current = &streamingSlot{slot: lifecycleSlot, generation: gen, parentSlot: lifecycleParentSlot, parentID: lifecycleParentBlockID,
		exec: exec, pending: make(map[uint32]*turbine.StreamBatch), openedAt: time.Now(), headerAt: time.Now(), nextStart: 1}
	s.handleEvent(turbine.StreamEvent{Kind: turbine.StreamBatchReady, Slot: lifecycleSlot, Generation: gen,
		Batch: turbine.NewDetachedStreamBatch(gen, 1, 4, orig[:4], verifiedIdentities(t, orig[:4]))})
	require.NotNil(t, s.current, "stream discarded: %s", metrics.GlobalBlockReplay.StreamingExecution.DiscardReason)
	sameTransactions(t, orig[:4], s.current.origin)
	sameCopies(t, orig[:4], exec.transactions)
	creditedA, err := exec.slotCtx.GetAccountShared(destA)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000+4*1_000+10+11+12+13), creditedA.Lamports, "the stream resolved through A's table")
	for i, tx := range orig {
		require.False(t, tx.Message.IsResolved(), "block object %d must be untouched by the stream", i)
	}

	s.discard("fork_switch")
	require.Empty(t, tail.added)

	envB := newLifecycleEnvWithTable(t, destB)
	whole := lifecycleWholeBlock(t, envB, orig, 2)
	require.Contains(t, whole.delta, destB, "the block's objects resolved through B's table")
	require.NotContains(t, whole.delta, destA, "nothing of A's resolution survived")
	require.Equal(t, uint64(10_000_000+6*1_000+10+11+12+13+14+15), whole.delta[destB].Lamports)
	for i, tx := range orig {
		require.True(t, tx.Message.IsResolved(), "block object %d was resolved by the whole-block path", i)
	}

	reference := lifecycleWholeBlock(t, newLifecycleEnvWithTable(t, destB), decodeTransactions(t, wires), 2)
	requireSameLifecycleOutcome(t, reference, whole)
}
