package replay

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prefetchRecordingSource serves fixed accounts and records every batch.
type prefetchRecordingSource struct {
	accts map[solana.PublicKey]*accounts.Account

	mu      sync.Mutex
	batches [][]solana.PublicKey
}

func (s *prefetchRecordingSource) GetAccount(slot uint64, pk solana.PublicKey) (*accounts.Account, error) {
	return s.accts[pk], nil
}

func (s *prefetchRecordingSource) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	s.mu.Lock()
	s.batches = append(s.batches, append([]solana.PublicKey(nil), pks...))
	s.mu.Unlock()
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i] = s.accts[pk]
	}
	return out, nil
}

func (s *prefetchRecordingSource) recorded() [][]solana.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]solana.PublicKey(nil), s.batches...)
}

func TestAccountPrefetchSnapshotCopiesReferences(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	lookup := solana.MessageAddressTableLookup{AccountKey: table, WritableIndexes: []uint8{0, 2}, ReadonlyIndexes: []uint8{7}}
	tx1 := lookupTestTx(t, lookup)
	tx2 := lookupTestTx(t, lookup)
	job := snapshotAccountPrefetchJob(&b.Block{Slot: 9, Transactions: []*solana.Transaction{tx1, tx2}})

	assert.Equal(t, uint64(9), job.slot)
	assert.ElementsMatch(t, []solana.PublicKey{txfixture.PayerPubkey(), solana.SystemProgramID}, job.keys, "static keys are deduplicated across transactions")
	assert.Equal(t, []solana.PublicKey{table}, job.tables, "a table used twice is read once")
	require.Len(t, job.lookups, 2)
	assert.Equal(t, []uint8{0, 2, 7}, job.lookups[0].indexes)

	// Replay may later mutate the block; the job must not alias it.
	tx1.Message.AddressTableLookups[0].WritableIndexes[0] = 5
	tx1.Message.AccountKeys[0] = solana.NewWallet().PublicKey()
	assert.Equal(t, []uint8{0, 2, 7}, job.lookups[0].indexes)
	assert.Contains(t, job.keys, txfixture.PayerPubkey())
}

func TestAccountPrefetchWarmsStaticAndLookupKeys(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	addrs := lookupTestAddresses(3)
	// The table also lists the payer, which is already a static key.
	tableAcct := encodeLookupTable(t, table, activeTableMeta(), addrs[0], addrs[1], txfixture.PayerPubkey(), addrs[2])
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{table: tableAcct}}
	p := &accountPrefetcher{src: src, ctx: context.Background()}
	tx := lookupTestTx(t, solana.MessageAddressTableLookup{
		AccountKey: table, WritableIndexes: []uint8{1, 2}, ReadonlyIndexes: []uint8{3, 40},
	})
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 5, Transactions: []*solana.Transaction{tx}}), true)

	batches := src.recorded()
	require.Len(t, batches, 2, "one batch for the tables, one for the block's accounts")
	assert.Equal(t, []solana.PublicKey{table}, batches[0])
	// Index 40 is past the table and ignored; the payer is not requested twice.
	assert.ElementsMatch(t, []solana.PublicKey{txfixture.PayerPubkey(), solana.SystemProgramID, addrs[1], addrs[2]}, batches[1])
	assert.Equal(t, uint64(1), p.warmed.Load())
}

func TestAccountPrefetchMissingTableStillWarmsStaticKeys(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := &accountPrefetcher{src: src, ctx: context.Background()}
	tx := lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, WritableIndexes: []uint8{0}})
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 5, Transactions: []*solana.Transaction{tx}}), true)
	batches := src.recorded()
	require.Len(t, batches, 2)
	assert.ElementsMatch(t, []solana.PublicKey{txfixture.PayerPubkey(), solana.SystemProgramID}, batches[1])
}

func TestAccountPrefetchWindow(t *testing.T) {
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := newAccountPrefetcher(src, 2, 0)
	defer p.Close()
	tx := lookupTestTx(t)
	offer := func(slot uint64) { p.Offer(&b.Block{Slot: slot, Transactions: []*solana.Transaction{tx}}) }

	p.Started(10)
	offer(10) // replay already began it: late, not warmed
	offer(12) // inside the window
	require.Eventually(t, func() bool { return p.warmed.Load() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, uint64(1), p.late.Load())

	offer(15) // beyond 10+2: waits for replay
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, uint64(1), p.warmed.Load(), "a block past the window must wait")
	p.Started(13)
	require.Eventually(t, func() bool { return p.warmed.Load() == 2 }, time.Second, time.Millisecond)

	offer(20)
	p.Started(21) // replay overtook it while it waited
	require.Eventually(t, func() bool { return p.late.Load() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, uint64(2), p.warmed.Load())
}

// Close cancels a read in flight and waits for the worker, so AccountsDB is
// never closed under a prefetch read; later offers neither block nor panic.
func TestAccountPrefetchCloseWaitsForReads(t *testing.T) {
	src := &blockingPrefetchSource{entered: make(chan struct{}, 1)}
	p := newAccountPrefetcher(src, 1, 0)
	p.Started(99)
	p.Offer(&b.Block{Slot: 100, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	<-src.entered
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the read in flight")
	}
	assert.True(t, src.cancelled.Load(), "the read saw the cancellation")
	for range accountPrefetchQueue + 1 {
		p.Offer(&b.Block{Slot: 101, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	}
}

// blockingPrefetchSource blocks every read until its context is cancelled.
type blockingPrefetchSource struct {
	entered   chan struct{}
	cancelled atomic.Bool
}

func (s *blockingPrefetchSource) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	return nil, nil
}

func (s *blockingPrefetchSource) GetAccountsBatch(ctx context.Context, _ uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	s.cancelled.Store(true)
	return nil, ctx.Err()
}

// A block whose batches were summarized at decode time is prefetched from those
// summaries, never from its transactions, which streaming execution may be
// rewriting.
func TestAccountPrefetchUsesDecodeSummaries(t *testing.T) {
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := newAccountPrefetcher(src, 1, 0)
	defer p.Close()
	p.Started(9)
	p.OfferStreamRefs(10, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(1), testPk(2)}})
	p.OfferStreamRefs(10, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(2), testPk(3)}})
	// The block's transactions name other keys: only the summaries count.
	p.Offer(&b.Block{Slot: 10, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	require.Eventually(t, func() bool { return p.warmed.Load() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, [][]solana.PublicKey{{testPk(1), testPk(2), testPk(3)}}, src.recorded())
	assert.Zero(t, p.partialOffered.Load(), "without tip warming batches are only recorded")
}

// A whole-block job waiting for its turn does not hold up tip warming.
func TestAccountPrefetchWaitingJobDoesNotBlockTipWarming(t *testing.T) {
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := newAccountPrefetcher(src, 1, 0)
	p.tipWarm = true
	defer p.Close()
	p.Started(10)
	p.Offer(&b.Block{Slot: 20, Transactions: []*solana.Transaction{lookupTestTx(t)}}) // far ahead
	time.Sleep(10 * time.Millisecond)
	p.OfferStreamRefs(11, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(7)}})
	require.Eventually(t, func() bool { return p.partialWarmed.Load() == 1 }, time.Second, time.Millisecond)
	assert.Zero(t, p.warmed.Load())
}

func TestAccountPrefetchSnapshotCollectsTopLevelPrograms(t *testing.T) {
	tx := lookupTestTx(t)
	job := snapshotAccountPrefetchJob(&b.Block{Slot: 3, Transactions: []*solana.Transaction{tx, lookupTestTx(t)}})
	assert.Equal(t, []solana.PublicKey{solana.SystemProgramID}, job.programs, "program ids come from instructions, deduplicated")
}

// Tip warming: decoded batches of the executing slot and the next one are
// warmed, each key once per slot; others are stale. They go ahead of queued
// whole-block jobs.
func TestAccountPrefetchTipWarming(t *testing.T) {
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := newAccountPrefetcher(src, 1, 0)
	p.tipWarm = true
	defer p.Close()
	k := func(b byte) solana.PublicKey { return testPk(b) }
	refs := func(keys ...solana.PublicKey) *turbine.StreamAccountRefs {
		return &turbine.StreamAccountRefs{Keys: keys}
	}

	p.Started(10)
	p.OfferStreamRefs(10, refs(k(1), k(2))) // later batch of the executing slot
	p.OfferStreamRefs(11, refs(k(2), k(3))) // next slot: k(2) is new for slot 11
	p.OfferStreamRefs(11, refs(k(3), k(4))) // k(3) already warmed for slot 11
	p.OfferStreamRefs(9, refs(k(5)))        // behind replay
	p.OfferStreamRefs(12, refs(k(6)))       // beyond the lookahead
	require.Eventually(t, func() bool { return p.partialWarmed.Load()+p.partialStale.Load() == 5 }, time.Second, time.Millisecond)
	assert.Equal(t, uint64(3), p.partialWarmed.Load())
	assert.Equal(t, uint64(2), p.partialStale.Load())
	assert.Equal(t, uint64(5), p.partialKeys.Load(), "k1,k2 for slot 10; k2,k3 then k4 for slot 11")
	var warmed []solana.PublicKey
	for _, batch := range src.recorded() {
		warmed = append(warmed, batch...)
	}
	assert.ElementsMatch(t, []solana.PublicKey{k(1), k(2), k(2), k(3), k(4)}, warmed)

}

// Queued batches of one slot are merged into one read, and a later
// whole-block job for the slot skips what tip warming already warmed.
func TestAccountPrefetchTipWarmingMergesAndFilters(t *testing.T) {
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := &accountPrefetcher{src: src, ahead: 1, ctx: context.Background(), partial: make(chan partialPrefetch, 16),
		partialSeen: map[uint64]map[solana.PublicKey]struct{}{}, partialPrograms: map[uint64]map[solana.PublicKey]struct{}{}}
	p.started.Store(10)
	k := func(b byte) solana.PublicKey { return testPk(b) }
	refs := func(keys ...solana.PublicKey) *turbine.StreamAccountRefs {
		return &turbine.StreamAccountRefs{Keys: keys}
	}

	p.partial <- partialPrefetch{slot: 11, refs: refs(k(2), k(3))}
	p.partial <- partialPrefetch{slot: 11, refs: refs(k(3), k(4))}
	p.warmPartial(partialPrefetch{slot: 11, refs: refs(k(1), k(2))}) // drains the two queued batches
	batches := src.recorded()
	require.Len(t, batches, 1, "three batches of one slot, one read")
	assert.ElementsMatch(t, []solana.PublicKey{k(1), k(2), k(3), k(4)}, batches[0])
	assert.Equal(t, uint64(3), p.partialWarmed.Load())

	job := p.withoutTipWarmed(accountPrefetchJob{slot: 11, keys: []solana.PublicKey{k(1), k(4), k(9)}})
	assert.Equal(t, []solana.PublicKey{k(9)}, job.keys, "the whole-block job keeps only keys tip warming did not warm")
	assert.Equal(t, uint64(2), p.tipFiltered.Load())
	untouched := p.withoutTipWarmed(accountPrefetchJob{slot: 12, keys: []solana.PublicKey{k(1)}})
	assert.Equal(t, []solana.PublicKey{k(1)}, untouched.keys, "another slot is not filtered")
}
