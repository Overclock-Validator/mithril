package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The memo must agree with the direct check for every input, on first and
// repeated queries, and for non-32-byte inputs. (Shards stay below their limit
// here; TestIsOnCurveMemoEvictsFullShard covers eviction.)
func TestIsOnCurveMemoMatchesDirectCheck(t *testing.T) {
	var on, off int
	for round := 0; round < 3; round++ {
		for i := 0; i < 3*onCurveMemoShards*onCurveMemoShardLimit/16; i++ {
			var seed [8]byte
			binary.LittleEndian.PutUint64(seed[:], uint64(i))
			h := sha256.Sum256(seed[:])
			want := isOnCurve(h[:])
			require.Equal(t, want, IsOnCurve(h[:]), "input %d round %d", i, round)
			if round == 0 {
				if want {
					on++
				} else {
					off++
				}
			}
		}
	}
	require.Greater(t, on, 1000)
	require.Greater(t, off, 1000)
	for _, b := range [][]byte{nil, {1, 2, 3}, make([]byte, 31), make([]byte, 33)} {
		require.Equal(t, isOnCurve(b), IsOnCurve(b))
	}
}

func BenchmarkIsOnCurveRepeated(b *testing.B) {
	keys := make([][32]byte, 1024)
	for i := range keys {
		keys[i] = sha256.Sum256([]byte{byte(i), byte(i >> 8)})
	}
	b.Run("direct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			isOnCurve(keys[i%len(keys)][:])
		}
	})
	b.Run("memo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsOnCurve(keys[i%len(keys)][:])
		}
	})
}

// shardInputs returns n distinct 32-byte inputs that all land in one memo shard.
func shardInputs(shard, n int) [][32]byte {
	out := make([][32]byte, 0, n)
	for i := uint64(0); len(out) < n; i++ {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], i)
		h := sha256.Sum256(seed[:])
		if int(h[0])%onCurveMemoShards == shard {
			out = append(out, h)
		}
	}
	return out
}

func memoShardLen(shard int) int {
	s := &onCurveMemo[shard]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Overfilling one shard evicts it: the shard never exceeds its limit, and
// answers stay correct for inputs seen before and after the refill, including
// under concurrent refills.
func TestIsOnCurveMemoEvictsFullShard(t *testing.T) {
	const shard = 5
	inputs := shardInputs(shard, 2*onCurveMemoShardLimit+100)
	refills := 0
	prev := memoShardLen(shard)
	for i, in := range inputs {
		require.Equal(t, isOnCurve(in[:]), IsOnCurve(in[:]), "input %d", i)
		n := memoShardLen(shard)
		require.LessOrEqual(t, n, onCurveMemoShardLimit)
		if n < prev {
			refills++
		}
		prev = n
	}
	require.GreaterOrEqual(t, refills, 2, "the shard must have been evicted and refilled")

	// Inputs from before the last eviction are recomputed, not misremembered.
	for i, in := range inputs[:200] {
		require.Equal(t, isOnCurve(in[:]), IsOnCurve(in[:]), "evicted input %d", i)
	}

	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < len(inputs); i += 3 {
				in := inputs[i]
				if isOnCurve(in[:]) != IsOnCurve(in[:]) {
					errs <- "concurrent mismatch"
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	require.LessOrEqual(t, memoShardLen(shard), onCurveMemoShardLimit)
}
