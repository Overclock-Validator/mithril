package snapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Unpacked appendvecs spread over both directories, each file under the ID
// that maps back to its directory, and the index entries use that ID.
func TestSnapshotWorkersSpreadAppendVecsAcrossDirectories(t *testing.T) {
	oldCopyingWorkers := SnapshotAppendVecCopyingWorkers
	SnapshotAppendVecCopyingWorkers = 1 // sequential writes: placement alternates by bytes written
	t.Cleanup(func() { SnapshotAppendVecCopyingWorkers = oldCopyingWorkers })

	paths := []string{t.TempDir(), t.TempDir()}
	dirs, err := makeAppendVecDirs(paths)
	require.NoError(t, err)
	t.Cleanup(dirs.close)

	var encoded bytes.Buffer
	key := solana.PublicKey{7}
	require.NoError(t, (&accountsdb.AppendVecAccount{
		Pubkey: key, Lamports: 1, Owner: solana.PublicKeyFromBytes(addresses.StakeProgramAddr[:]),
		DataLen: 4, Data: []byte{1, 2, 3, 4},
	}).Marshal(&encoded))
	data := encoded.Bytes()
	manifest := &SnapshotManifest{AccountsDb: &AccountsDbFields{Storages: map[uint64]SlotAcctVecs{}}}

	shardLogger := NewShardLogger(1, t.TempDir())
	closeShardLogger := sync.OnceFunc(func() { require.NoError(t, shardLogger.Close(context.Background())) })
	t.Cleanup(closeShardLogger)
	wg := &sync.WaitGroup{}
	stakes := &stakeIndexCollector{}
	pools, err := initWorkerPools(wg, shardLogger, manifest, nil, dirs, stakes)
	require.NoError(t, err)
	t.Cleanup(pools.Release)
	for i := uint64(0); i < 6; i++ {
		require.NoError(t, invokeSnapshotTask(wg, pools.appendVecCopying, appendVecCopyingTask{
			Filename: filepath.Join("accounts", accountsdb.SegmentDataName(100+i, 10+i)), Buf: append([]byte(nil), data...),
		}))
	}
	require.NoError(t, waitForSnapshotWorkers(wg, pools))
	closeShardLogger()

	perDir := [2]int{}
	var maxId uint64
	require.Len(t, stakes.entries, 6)
	for _, e := range stakes.entries {
		maxId = max(maxId, e.FileId)
		dir := e.FileId % 2
		perDir[dir]++
		orig := e.FileId / 2
		require.FileExists(t, filepath.Join(paths[dir], "accounts", accountsdb.SegmentDataName(orig+90, e.FileId)))
	}
	require.Equal(t, [2]int{3, 3}, perDir, "sequential equal-size writes alternate between directories")
	require.Equal(t, maxId, dirs.largest.Load(), "largest file ID tracks the remapped IDs")
}

// One directory gets 3 MiB of room above the reserve and the other plenty:
// files stop landing on the small one once its room is used, and placement
// still writes (rather than failing early) when every disk is at its reserve.
func TestAppendVecDirsRespectFreeSpace(t *testing.T) {
	small, large := t.TempDir(), t.TempDir()
	const reserve = uint64(64 << 20)
	written := map[string]uint64{}
	capacity := map[string]uint64{small: reserve + 3<<20, large: 1 << 40}
	var mu sync.Mutex
	oldAvail, oldReserve, oldInterval := dirAvailableBytes, SnapshotAccountsDirMinFreeBytes, dirFreeRefreshInterval
	t.Cleanup(func() {
		dirAvailableBytes, SnapshotAccountsDirMinFreeBytes, dirFreeRefreshInterval = oldAvail, oldReserve, oldInterval
	})
	// Like statfs: bytes already written are no longer free.
	dirAvailableBytes = func(dir string) (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if written[dir] > capacity[dir] {
			return 0, nil
		}
		return capacity[dir] - written[dir], nil
	}
	SnapshotAccountsDirMinFreeBytes = reserve
	dirFreeRefreshInterval = time.Millisecond

	d, err := openAppendVecDirs([]string{small, large})
	require.NoError(t, err)
	const size = 256 << 10
	perDir := map[int]int{}
	for i := 0; i < 200; i++ {
		dir, fileId := d.place(uint64(i), size)
		require.Equal(t, uint64(dir), fileId%2)
		mu.Lock()
		written[d.dirs[dir]] += size
		mu.Unlock()
		d.finish(dir, size)
		perDir[dir]++
		if i%10 == 0 {
			time.Sleep(2 * time.Millisecond) // let refreshes interleave with writes
		}
	}
	d.close()
	// 3 MiB of room is 12 files; one more may be placed as it runs out.
	require.LessOrEqual(t, perDir[0], 13)
	require.Positive(t, perDir[1])

	// Both directories below the reserve (unknown to capacity, so 0 bytes
	// free): placement falls back instead of failing.
	d, err = openAppendVecDirs([]string{t.TempDir(), t.TempDir()})
	require.NoError(t, err)
	dir, _ := d.place(1, size)
	require.Contains(t, []int{0, 1}, dir)
	d.finish(dir, size)
	d.close()
}

func TestCleanAccountsDbDirsRemovesEveryAccountsDir(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir()}
	for _, p := range paths {
		require.NoError(t, os.MkdirAll(filepath.Join(p, "accounts"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(p, "accounts", "1.2"), []byte("x"), 0o644))
	}
	require.NoError(t, accountsdb.WriteAccountsDirCount(paths[0], 2))
	CleanAccountsDbDirs(paths)
	for _, p := range paths {
		require.NoDirExists(t, filepath.Join(p, "accounts"))
	}
	require.NoFileExists(t, filepath.Join(paths[0], accountsdb.AccountsDirCountFile))

	// Aliased paths are refused and nothing is removed.
	keep := filepath.Join(paths[1], "keep")
	require.NoError(t, os.MkdirAll(filepath.Join(keep, "accounts"), 0o755))
	CleanAccountsDbDirs([]string{keep, keep})
	require.DirExists(t, filepath.Join(keep, "accounts"))
}
