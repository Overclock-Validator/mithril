package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRecentBlockhashesFromState(t *testing.T) {
	entries := make([]state.BlockhashEntry, 150)
	hashes := make([]solana.Hash, len(entries))
	for i := range entries {
		hashes[i] = solana.Hash{byte(i + 1), byte((i + 1) >> 8)}
		entries[i] = state.BlockhashEntry{
			Blockhash:            hashes[i].String(),
			LamportsPerSignature: uint64(5000 + i),
		}
	}

	recent, err := RecentBlockhashesFromState(entries)
	require.NoError(t, err)
	require.Len(t, recent, 150)
	require.Equal(t, [32]byte(hashes[0]), recent[0].Blockhash)
	require.Equal(t, uint64(5000), recent[0].FeeCalculator.LamportsPerSignature)
	require.True(t, recent.IsBlockhashAgeValid(hashes[141]))

	_, err = RecentBlockhashesFromState(nil)
	require.Error(t, err)
	_, err = RecentBlockhashesFromState([]state.BlockhashEntry{{Blockhash: "invalid"}})
	require.Error(t, err)
}

func TestSeedRecentBlockhashesCache(t *testing.T) {
	prev := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = prev }()

	rbh := sealevel.SysvarRecentBlockhashes{{Blockhash: solana.Hash{1}}}
	SeedRecentBlockhashesCache(rbh)
	require.NotNil(t, sealevel.SysvarCache.RecentBlockHashes.Sysvar)
	require.Equal(t, [32]byte(solana.Hash{1}), (*sealevel.SysvarCache.RecentBlockHashes.Sysvar)[0].Blockhash)

	other := sealevel.SysvarRecentBlockhashes{{Blockhash: solana.Hash{2}}}
	SeedRecentBlockhashesCache(other)
	require.Equal(t, [32]byte(solana.Hash{1}), (*sealevel.SysvarCache.RecentBlockHashes.Sysvar)[0].Blockhash)
}

func TestCloneRecentBlockhashesFromCache(t *testing.T) {
	prev := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = prev }()

	sealevel.SysvarCache.RecentBlockHashes.Sysvar = nil
	_, err := cloneRecentBlockhashesFromCache()
	require.Error(t, err)

	rbh := sealevel.SysvarRecentBlockhashes{{Blockhash: solana.Hash{9}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &rbh

	clone, err := cloneRecentBlockhashesFromCache()
	require.NoError(t, err)
	newHash := solana.Hash{2}
	clone.PushLatest(newHash, 5000)
	require.Equal(t, [32]byte(solana.Hash{9}), (*sealevel.SysvarCache.RecentBlockHashes.Sysvar)[0].Blockhash)
	require.Equal(t, [32]byte(newHash), clone[0].Blockhash)
}

func TestPushLatestPrependsFrozenEntryHash(t *testing.T) {
	parentHash := solana.Hash{1}
	newHash := solana.Hash{2}
	recent := sealevel.SysvarRecentBlockhashes{{
		Blockhash:     parentHash,
		FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
	}}

	evicted := recent.PushLatest(newHash, 5000)
	require.Equal(t, [32]byte{}, evicted)
	require.Len(t, recent, 2)
	require.Equal(t, [32]byte(newHash), recent[0].Blockhash)
	require.Equal(t, [32]byte(parentHash), recent[1].Blockhash)
}

// A full window must marshal to the sysvar account's fixed size: a shorter list
// would resize the account that gets hashed into the bank hash.
func TestRecentBlockhashesFromStateIsNewestFirstAndFullWindow(t *testing.T) {
	const entryCount = 150
	entries := make([]state.BlockhashEntry, entryCount)
	want := make([]solana.Hash, entryCount)
	for i := range entries {
		want[i] = solana.Hash{byte(i + 1), byte((i + 1) >> 8), 0xAB}
		entries[i] = state.BlockhashEntry{
			Blockhash:            want[i].String(),
			LamportsPerSignature: uint64(5000 + i),
		}
	}

	recent, err := RecentBlockhashesFromState(entries)
	require.NoError(t, err)
	require.Len(t, recent, entryCount)

	// Newest-first: element 0 is what configureInitialBlock assigns to
	// block.LastBlockhash, and it must be the parent's hash, not the oldest.
	require.Equal(t, [32]byte(want[0]), recent[0].Blockhash,
		"recent[0] must be the newest entry; block.LastBlockhash is taken from it")
	require.Equal(t, [32]byte(want[entryCount-1]), recent[entryCount-1].Blockhash)
	require.Equal(t, uint64(5000), recent[0].FeeCalculator.LamportsPerSignature)

	// A full window marshals to the fixed sysvar account size. Agave keeps the
	// account at this length, so a shorter list here would resize the account
	// that gets hashed into the bank hash.
	require.Len(t, recent.MustMarshal(), 8+entryCount*40,
		"seeded RecentBlockhashes must marshal to the sysvar account's fixed size")
}

// The seeded cache and the parent hash must come from the same newest-first
// window: nonce validation reads the cache while age checks compare against
// block.LastBlockhash, so a disagreement admits or rejects the wrong nonces.
func TestSeedInitialBlockhashContextSeedsCacheAndParentHash(t *testing.T) {
	saved := sealevel.SysvarCache.RecentBlockHashes
	t.Cleanup(func() { sealevel.SysvarCache.RecentBlockHashes = saved })
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = nil

	const entryCount = 150
	entries := make([]state.BlockhashEntry, entryCount)
	want := make([]solana.Hash, entryCount)
	for i := range entries {
		want[i] = solana.Hash{byte(i + 1), byte((i + 1) >> 8), 0xCD}
		entries[i] = state.BlockhashEntry{
			Blockhash:            want[i].String(),
			LamportsPerSignature: uint64(7000 + i),
		}
	}

	lastBlockhash, err := seedInitialBlockhashContext(entries)
	require.NoError(t, err)
	require.Equal(t, [32]byte(want[0]), lastBlockhash,
		"parent hash must be the newest entry, not the oldest")

	require.NotNil(t, sealevel.SysvarCache.RecentBlockHashes.Sysvar, "cache was not seeded")
	cached := *sealevel.SysvarCache.RecentBlockHashes.Sysvar
	require.Len(t, cached, entryCount, "cache must hold the full window")
	require.Equal(t, [32]byte(want[0]), cached[0].Blockhash,
		"cache and parent hash must agree on the newest entry")
	require.True(t, cached.IsBlockhashAgeValid(want[entryCount-1]),
		"the oldest seeded hash must still be admissible")
}

// An empty manifest window must fail rather than seed nothing and hand back a
// zero parent hash, which would silently reject every durable-nonce transaction.
func TestSeedInitialBlockhashContextRejectsEmptyWindow(t *testing.T) {
	saved := sealevel.SysvarCache.RecentBlockHashes
	t.Cleanup(func() { sealevel.SysvarCache.RecentBlockHashes = saved })

	_, err := seedInitialBlockhashContext(nil)
	require.Error(t, err, "an empty manifest window must not produce a zero parent hash")
}
