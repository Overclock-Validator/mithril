package replay

import (
	"bytes"
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preloadAccounts is a block's world for the preload: a top-level v2 program,
// an upgradeable program that the transaction only lists as an account (as a
// program it invokes through CPI must be listed), its programdata, and a v2
// program the transaction reaches through an address lookup table.
type preloadAccounts struct {
	top, invoked, programData, viaTable, table solana.PublicKey
	accts                                      map[solana.PublicKey]*accounts.Account
}

func newPreloadAccounts(t *testing.T, elf []byte) preloadAccounts {
	t.Helper()
	w := preloadAccounts{top: solana.PublicKey{0xA1}, invoked: solana.PublicKey{0xA2}, programData: solana.PublicKey{0xA3},
		viaTable: solana.PublicKey{0xA4}, table: solana.PublicKey{0xA5}, accts: map[solana.PublicKey]*accounts.Account{}}
	encode := func(state sealevel.UpgradeableLoaderState, size int) []byte {
		var buf bytes.Buffer
		require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
		out := make([]byte, size)
		copy(out, buf.Bytes())
		return out
	}
	put := func(key, owner solana.PublicKey, data []byte, executable bool) {
		w.accts[key] = &accounts.Account{Key: key, Owner: owner, Lamports: 1, Data: data, Executable: executable}
	}
	put(w.top, addresses.BpfLoader2Addr, elf, true)
	put(w.invoked, addresses.BpfLoaderUpgradeableAddr, encode(sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeProgram,
		Program: sealevel.UpgradeableLoaderStateProgram{ProgramDataAddress: w.programData}}, 36), true)
	pd := encode(sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeProgramData,
		ProgramData: sealevel.UpgradeableLoaderStateProgramData{Slot: 1}}, 45+len(elf))
	copy(pd[45:], elf)
	put(w.programData, addresses.BpfLoaderUpgradeableAddr, pd, false)
	put(w.viaTable, addresses.BpfLoader2Addr, elf, true)
	w.accts[w.table] = encodeLookupTable(t, w.table, activeTableMeta(), w.viaTable)
	return w
}

// tx calls the top-level program, lists the invoked program as an account and
// loads viaTable through the lookup table.
func (w preloadAccounts) tx(t *testing.T) *solana.Transaction {
	t.Helper()
	msg := solana.Message{
		Header:              solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 2},
		AccountKeys:         solana.PublicKeySlice{txfixture.PayerPubkey(), w.invoked, w.top},
		RecentBlockhash:     txfixture.TestBlockhash(),
		Instructions:        []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{0, 1, 3}}},
		AddressTableLookups: []solana.MessageAddressTableLookup{{AccountKey: w.table, ReadonlyIndexes: []uint8{0}}},
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
	return tx
}

func preloadPrefetcher(src blockAccountSource) (*accountPrefetcher, *accountsdb.AccountsDb) {
	cache := accountsdb.NewWeightedCache[solana.PublicKey, *accountsdb.ProgramCacheEntry](64,
		func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 })
	db := &accountsdb.AccountsDb{ProgramCache: cache}
	p := &accountPrefetcher{src: src, ctx: context.Background(), db: db}
	p.features.Store(features.NewFeaturesDefault())
	return p, db
}

// Warming a block compiles, into the program cache and under the keys
// execution looks up, its top-level program, a program it lists only as an
// account (the CPI case; upgradeable, so cached under its programdata) and a
// program it reaches through an address lookup table. Execution's own source
// check accepts each entry.
func TestAccountPrefetchPreloadsInvokedProgramsEndToEnd(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	w := newPreloadAccounts(t, elf)
	p, db := preloadPrefetcher(&prefetchRecordingSource{accts: w.accts})
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 50, Transactions: []*solana.Transaction{w.tx(t)}}), true)

	f := features.NewFeaturesDefault()
	for name, key := range map[string]solana.PublicKey{"top-level": w.top, "invoked (programdata)": w.programData, "through a lookup table": w.viaTable} {
		entry, ok := db.MaybeGetProgramFromCache(key)
		require.True(t, ok, "%s program not preloaded", name)
		assert.True(t, entry.MatchesSource(elf, f), "%s entry does not match its source", name)
	}
	_, ok := db.MaybeGetProgramFromCache(w.invoked)
	assert.False(t, ok, "an upgradeable program is cached under its programdata, not its own key")
	assert.Equal(t, uint64(2), p.invoked.Load(), "two programs found among the loaded accounts")
	assert.Equal(t, uint64(3), p.preloaded.Load())
}

// partialSource returns one account fewer than asked for the block's batch, as
// a source that failed part of a read would.
type partialSource struct {
	prefetchRecordingSource
	short int // the batch length that is answered short
}

func (s *partialSource) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out, err := s.prefetchRecordingSource.GetAccountsBatch(ctx, slot, pks)
	if len(pks) == s.short {
		out = out[:len(out)-1]
	}
	return out, err
}

// A partial read of the block's accounts preloads no CPI candidates (their
// keys cannot be matched to accounts), while the top-level program, read
// separately, is still preloaded.
func TestAccountPrefetchPreloadSkipsPartialRead(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	w := newPreloadAccounts(t, elf)
	src := &partialSource{prefetchRecordingSource: prefetchRecordingSource{accts: w.accts}, short: 4} // payer, invoked, top, viaTable
	p, db := preloadPrefetcher(src)
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 50, Transactions: []*solana.Transaction{w.tx(t)}}), true)

	_, ok := db.MaybeGetProgramFromCache(w.top)
	assert.True(t, ok, "the top-level program is read and preloaded on its own")
	for _, key := range []solana.PublicKey{w.programData, w.viaTable} {
		_, ok := db.MaybeGetProgramFromCache(key)
		assert.False(t, ok, "a partial read must not preload candidates matched by position")
	}
	assert.Equal(t, uint64(0), p.invoked.Load())
}

// referencedPrograms returns n v2 programs with the given source, as the
// accounts a block lists without invoking them.
func referencedPrograms(first byte, n int, source []byte) ([]solana.PublicKey, []*accounts.Account) {
	keys := make([]solana.PublicKey, n)
	accts := make([]*accounts.Account, n)
	for i := range n {
		keys[i] = solana.PublicKey{first, byte(i)}
		accts[i] = &accounts.Account{Key: keys[i], Owner: addresses.BpfLoader2Addr, Lamports: 1, Executable: true, Data: source}
	}
	return keys, accts
}

// Preloads of programs a block only lists stop after preloadReferencedAttempts
// compile attempts, failed ones included, or once their source would exceed
// preloadReferencedBytes. Top-level programs are not bounded.
func TestAccountPrefetchPreloadBudget(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	cachedCount := func(db *accountsdb.AccountsDb, keys []solana.PublicKey) int {
		n := 0
		for _, key := range keys {
			if _, ok := db.MaybeGetProgramFromCache(key); ok {
				n++
			}
		}
		return n
	}

	failing, failingAccts := referencedPrograms(0xB1, preloadReferencedAttempts, []byte("not an elf"))
	valid, validAccts := referencedPrograms(0xB2, 4, elf)
	p, db := preloadPrefetcher(&prefetchRecordingSource{})
	p.preloadPrograms(context.Background(), accountPrefetchJob{slot: 50}, append(failing, valid...), append(failingAccts, validAccts...))
	assert.Zero(t, cachedCount(db, valid), "failed attempts count against the bound")
	assert.Equal(t, uint64(1), p.overBudget.Load())

	big, bigAccts := referencedPrograms(0xB3, 2, make([]byte, preloadReferencedBytes/2+1))
	p, db = preloadPrefetcher(&prefetchRecordingSource{})
	p.preloadPrograms(context.Background(), accountPrefetchJob{slot: 50}, append(big, valid...), append(bigAccts, validAccts...))
	assert.Zero(t, cachedCount(db, valid), "source bytes count against the bound")
	assert.Equal(t, uint64(1), p.overBudget.Load())

	top, topAccts := referencedPrograms(0xB4, preloadReferencedAttempts+4, elf)
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	for i, key := range top {
		src.accts[key] = topAccts[i]
	}
	p, db = preloadPrefetcher(src)
	p.preloadPrograms(context.Background(), accountPrefetchJob{slot: 50, programs: top}, nil, nil)
	assert.Equal(t, len(top), cachedCount(db, top), "top-level programs are not bounded")
	assert.Zero(t, p.overBudget.Load())
}

// cancellingSource cancels the preload's context when it serves the read of
// one key, as Close would during that read.
type cancellingSource struct {
	prefetchRecordingSource
	key    solana.PublicKey
	cancel context.CancelFunc
}

func (s *cancellingSource) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	if len(pks) == 1 && pks[0] == s.key {
		s.cancel()
	}
	return s.prefetchRecordingSource.GetAccountsBatch(ctx, slot, pks)
}

// Cancellation during a programdata read prevents that program's compile and
// every later one.
func TestAccountPrefetchPreloadStopsWhenCancelled(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	w := newPreloadAccounts(t, elf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, db := preloadPrefetcher(&cancellingSource{prefetchRecordingSource: prefetchRecordingSource{accts: w.accts}, key: w.programData, cancel: cancel})
	p.ctx = ctx
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 50, Transactions: []*solana.Transaction{w.tx(t)}}), true)

	_, ok := db.MaybeGetProgramFromCache(w.top)
	assert.True(t, ok, "the top-level program, preloaded before the cancellation, stays cached")
	for name, key := range map[string]solana.PublicKey{"whose programdata read was cancelled": w.programData, "after the cancellation": w.viaTable} {
		_, ok := db.MaybeGetProgramFromCache(key)
		assert.False(t, ok, "program %s was compiled", name)
	}
}
