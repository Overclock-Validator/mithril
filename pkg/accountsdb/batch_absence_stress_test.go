package accountsdb

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func stressKey(i int) solana.PublicKey {
	var k solana.PublicKey
	k[0] = 0xA5
	binary.LittleEndian.PutUint32(k[1:], uint32(i))
	return k
}

// Absence must be linearizable with folds and rewinds: once CommitBatch or
// RewindToBatchBoundary has returned, every batch read that starts afterwards
// reflects it, even while concurrent readers keep caching those keys as absent.
func TestBatchAbsenceConcurrentFoldsAndRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const n = 60
	keys := make([]solana.PublicKey, n)
	for i := range keys {
		keys[i] = stressKey(i)
	}
	// Per key, for reads starting now: 1 must be present, 2 must be absent, 0 either.
	visible := make([]atomic.Int32, n)
	// A read overlapping a state transition may legally see either version.
	generation := make([]atomic.Uint64, n)
	var negativeHits atomic.Uint64
	var readSlot atomic.Uint64
	readSlot.Store(100)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				batch := make([]solana.PublicKey, 16)
				idx := make([]int, 16)
				want := make([]int32, 16)
				versions := make([]uint64, 16)
				for j := range batch {
					idx[j] = rng.Intn(n)
					batch[j] = keys[idx[j]]
					versions[j] = generation[idx[j]].Load()
					want[j] = visible[idx[j]].Load()
				}
				out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), readSlot.Load(), batch)
				negativeHits.Add(stats.NegativeCacheHits)
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				for j, i := range idx {
					if versions[j]&1 != 0 || versions[j] != generation[i].Load() {
						continue
					}
					switch want[j] {
					case 1:
						if out[j].Lamports == 0 {
							t.Errorf("key %d was committed before the read started but read as absent", i)
						}
					case 2:
						if out[j].Lamports != 0 {
							t.Errorf("key %d was rewound away before the read started but read as present", i)
						}
					}
				}
			}
		}(int64(w + 1))
	}

	defer func() { close(stop); wg.Wait() }()
	slot := uint64(100)
	commit := func(i int) {
		generation[i].Add(1)
		slot += 10
		commitTestBatch(t, db, slot, &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{1}, Data: []byte{byte(i)}})
		readSlot.Store(slot)
		visible[i].Store(1)
		generation[i].Add(1)
	}
	boundary := make([]uint64, n)
	for i := 0; i < n; i++ {
		commit(i)
		boundary[i] = slot
	}

	// Rewind to the boundary after key 29: keys 30.. become absent again.
	for i := 30; i < n; i++ {
		generation[i].Add(1)
		visible[i].Store(0)
	}
	_, err := db.RewindToBatchBoundary(boundary[29])
	require.NoError(t, err)
	readSlot.Store(boundary[29])
	for i := 30; i < n; i++ {
		visible[i].Store(2)
		generation[i].Add(1)
	}

	// Recreate them at new slots; each must be visible as soon as its fold returns.
	slot = boundary[29] + 5
	for i := 30; i < n; i++ {
		visible[i].Store(0)
		commit(i)
	}
	// Guard against a vacuous run: the absence cache must actually have served.
	require.NotZero(t, negativeHits.Load(), "stress run never hit the absence cache")
	t.Logf("negative-cache hits during the run: %d", negativeHits.Load())

	// Final state, read fresh: everything present with its committed value.
	out, err := db.GetAccountsBatch(context.Background(), slot, keys)
	require.NoError(t, err)
	for i := range keys {
		require.EqualValues(t, i+1, out[i].Lamports, "key %d", i)
	}
}
