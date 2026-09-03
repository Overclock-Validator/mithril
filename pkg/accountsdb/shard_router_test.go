package accountsdb

import (
	"encoding/binary"
	"testing"

	"github.com/dgryski/go-sip13"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
)

func TestPersistentIndexShardCountValidation(t *testing.T) {
	t.Parallel()

	for count := 1; count <= MaxPersistentIndexShards; count <<= 1 {
		require.NoError(t, ValidatePersistentIndexShardCount(count), "count %d", count)
	}
	for _, count := range []int{-1, 0, 3, 6, 1000, MaxPersistentIndexShards + 1} {
		require.ErrorIs(t, ValidatePersistentIndexShardCount(count), ErrInvalidPersistentIndexShardCount)
	}
	require.Equal(t, 1024, DefaultPersistentIndexShards)
}

func testPersistentIndexRoutingKey() PersistentIndexRoutingKey {
	return PersistentIndexRoutingKey{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}
}

func TestPersistentIndexShardRouterIsDeterministic(t *testing.T) {
	t.Parallel()

	routingKey := testPersistentIndexRoutingKey()
	router, err := NewPersistentIndexShardRouter(DefaultPersistentIndexShards, routingKey)
	require.NoError(t, err)
	require.Equal(t, DefaultPersistentIndexShards, router.Count())
	require.Equal(t, uint8(10), router.PrefixBits())
	require.Equal(t, routingKey, router.RoutingKey())

	for ordinal := uint64(0); ordinal < DefaultPersistentIndexShards; ordinal++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[24:], ordinal)
		first := router.Shard(key)
		require.Less(t, first, uint32(router.Count()))
		require.Equal(t, first, router.Shard(key))
	}
}

func TestPersistentIndexShardRouterResistsRepeatedRawPrefix(t *testing.T) {
	t.Parallel()

	router, err := NewPersistentIndexShardRouter(DefaultPersistentIndexShards, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	counts := make([]int, router.Count())
	for ordinal := uint64(0); ordinal < 1<<16; ordinal++ {
		var key solana.PublicKey
		key[0], key[1] = 0xab, 0xcd
		binary.LittleEndian.PutUint64(key[24:], ordinal)
		counts[router.Shard(key)]++
	}
	for shard, count := range counts {
		require.Greater(t, count, 20, "shard %d", shard)
		require.Less(t, count, 120, "shard %d", shard)
	}
}

func TestPersistentIndexShardRouterKeyChangesRouting(t *testing.T) {
	t.Parallel()

	keyA := testPersistentIndexRoutingKey()
	keyB := keyA
	keyB[0] ^= 0xff
	routerA, err := NewPersistentIndexShardRouter(1024, keyA)
	require.NoError(t, err)
	routerB, err := NewPersistentIndexShardRouter(1024, keyB)
	require.NoError(t, err)
	changed := 0
	for ordinal := uint64(0); ordinal < 1024; ordinal++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[24:], ordinal)
		if routerA.Shard(key) != routerB.Shard(key) {
			changed++
		}
	}
	require.Greater(t, changed, 1000)
}

func TestPersistentIndexShardRouterRejectsZeroValueUse(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		var router PersistentIndexShardRouter
		router.Shard(solana.PublicKey{})
	})
	_, err := NewPersistentIndexShardRouter(1024, PersistentIndexRoutingKey{})
	require.ErrorContains(t, err, "zero persistent shard routing key")
	key, err := NewPersistentIndexRoutingKey()
	require.NoError(t, err)
	require.NotEqual(t, PersistentIndexRoutingKey{}, key)
}

var persistentRouterBenchmarkSink uint64

func BenchmarkPersistentRouterHashCandidates(b *testing.B) {
	var key solana.PublicKey
	for i := range key {
		key[i] = byte(i*17 + 3)
	}
	var keyedBlake3 [32]byte
	for i := range keyedBlake3 {
		keyedBlake3[i] = byte(i*29 + 7)
	}

	b.Run("siphash-1-3", func(b *testing.B) {
		var result uint64
		for b.Loop() {
			result = sip13.Sum64(0x0706050403020100, 0x0f0e0d0c0b0a0908, key[:])
		}
		persistentRouterBenchmarkSink = result
	})
	b.Run("production-router", func(b *testing.B) {
		router, err := NewPersistentIndexShardRouter(1024, testPersistentIndexRoutingKey())
		if err != nil {
			b.Fatal(err)
		}
		var result uint32
		for b.Loop() {
			result = router.Shard(key)
		}
		persistentRouterBenchmarkSink = uint64(result)
	})
	b.Run("xxh3-seeded", func(b *testing.B) {
		var result uint64
		for b.Loop() {
			result = xxh3.HashSeed(key[:], 0x0706050403020100)
		}
		persistentRouterBenchmarkSink = result
	})
	b.Run("blake3-keyed", func(b *testing.B) {
		var result uint64
		for b.Loop() {
			hasher, err := blake3.NewKeyed(keyedBlake3[:])
			if err != nil {
				b.Fatal(err)
			}
			_, _ = hasher.Write(key[:])
			var digest [8]byte
			_, _ = hasher.Digest().Read(digest[:])
			result = binary.LittleEndian.Uint64(digest[:])
		}
		persistentRouterBenchmarkSink = result
	})
}
