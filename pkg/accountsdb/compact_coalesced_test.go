package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func newCoalescedTestDb(t *testing.T, shards int) (*AccountsDb, []string, []*accounts.Account) {
	t.Helper()
	paths := make([]string, shards)
	for i := range paths {
		paths[i] = t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(paths[i], "accounts"), 0755))
	}
	var b [8]byte
	for name, n := range map[string]uint64{"num_shards": uint64(shards), "largest_file_id": uint64(shards - 1), "bootstrap_high_file_id": uint64(shards - 1)} {
		binary.LittleEndian.PutUint64(b[:], n)
		require.NoError(t, os.WriteFile(filepath.Join(paths[0], name), b[:], 0644))
	}
	db, err := OpenDbPaths(paths)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	var live []*accounts.Account
	for shard := range paths {
		var data bytes.Buffer
		for j := 0; j < 3; j++ {
			// Nonzero dead bytes, O_DIRECT gaps, unaligned starts, and no final
			// padding: sequential appendvec parsing cannot recover these boundaries.
			data.Write(bytes.Repeat([]byte{0xab}, 4096+j))
			a := foldAcct(byte(1+shard*3+j), uint64(100+j), []byte{byte(j), 1, 2, 3, 4})
			a.Slot = uint64(70 + j)
			off := uint64(data.Len())
			var record bytes.Buffer
			require.NoError(t, (&AppendVecAccount{Pubkey: a.Key, Lamports: a.Lamports, Owner: a.Owner, RentEpoch: a.RentEpoch, DataLen: uint64(len(a.Data)), Data: a.Data}).Marshal(&record))
			data.Write(record.Bytes()[:hdrLen+len(a.Data)])
			var idx [24]byte
			(&AccountIndexEntry{Slot: a.Slot, FileId: uint64(shard), Offset: off}).Marshal(&idx)
			require.NoError(t, db.Index.Set(a.Key[:], idx[:], pebble.NoSync))
			live = append(live, a)
		}
		require.NoError(t, os.WriteFile(filepath.Join(paths[shard], "accounts", "data"), data.Bytes(), 0644))
	}
	require.NoError(t, db.Index.Flush())
	return db, paths, live
}

func reopenCoalescedTestDb(t *testing.T, db *AccountsDb, paths []string) *AccountsDb {
	t.Helper()
	db.CloseDb()
	re, err := OpenDbPaths(paths)
	require.NoError(t, err)
	re.RootedDurable = true
	re.InitCaches()
	_, err = re.RecoverFoldState()
	require.NoError(t, err)
	return re
}

func checkCoalescedAccounts(t *testing.T, db *AccountsDb, live []*accounts.Account) {
	t.Helper()
	keys := make([]solana.PublicKey, len(live))
	for i, a := range live {
		keys[i] = a.Key
		require.Equal(t, a, mustColdRead(t, db, 100, a.Key))
	}
	// Force actual batch reads too, after the individual cold reads populated caches.
	for _, k := range keys {
		db.CommonAcctsCache.Delete(k)
		db.VoteAcctCache.Delete(k)
	}
	got, err := db.GetAccountsBatch(context.Background(), 100, keys)
	require.NoError(t, err)
	require.Equal(t, live, got)
}

func coalescedSourcesExist(paths []string) bool {
	for _, p := range paths {
		if _, err := os.Stat(filepath.Join(p, "accounts", "data")); err == nil {
			return true
		}
	}
	return false
}

func TestCoalescedCompactionBoundedReopen(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		t.Run(fmt.Sprintf("wal-off=%v", walOff), func(t *testing.T) {
			old := DisableIndexWAL
			DisableIndexWAL = walOff
			defer func() { DisableIndexWAL = old }()
			db, paths, live := newCoalescedTestDb(t, 2)
			defer func() { db.CloseDb() }()
			checkCoalescedAccounts(t, db, live)
			cfg := CompactionConfig{MinDeadFraction: 0.5, MaxMoveBytesPerCycle: 160, MaxScanBytesPerCycle: 300}
			var reclaimed int64
			cycles := 0
			reopened := false
			for coalescedSourcesExist(paths) && cycles < 200 {
				stats, err := db.CompactOnce(cfg)
				require.NoError(t, err)
				require.LessOrEqual(t, stats.LiveBytesMoved, int64(144), "one indivisible record per move budget")
				reclaimed += stats.BytesReclaimed
				cycles++
				checkCoalescedAccounts(t, db, live)
				if stats.LiveBytesMoved > 0 && !reopened {
					require.True(t, coalescedSourcesExist(paths), "partial evacuation retains the source")
					db = reopenCoalescedTestDb(t, db, paths)
					reopened = true
				}
			}
			require.Less(t, cycles, 200, "bounded scans must make progress")
			require.True(t, reopened)
			require.Positive(t, reclaimed)
			require.Greater(t, cycles, 3)
			db = reopenCoalescedTestDb(t, db, paths)
			checkCoalescedAccounts(t, db, live)
			// Compacted files are themselves candidates after their records die.
			for _, a := range live {
				require.NoError(t, db.Index.Delete(a.Key[:], pebble.NoSync))
			}
			require.NoError(t, db.Index.Flush())
			for i := 0; i < 100; i++ {
				_, err := db.CompactOnce(cfg)
				require.NoError(t, err)
			}
			count := 0
			db.coalescedFiles.Range(func(_, _ any) bool { count++; return true })
			require.Zero(t, count)
		})
	}
}

func TestCoalescedCompactionThresholdAndFullyDead(t *testing.T) {
	db, paths, live := newCoalescedTestDb(t, 2)
	defer db.CloseDb()
	for i := 0; i < 3; i++ {
		stats, err := db.CompactOnce(CompactionConfig{MinDeadFraction: 0.999})
		require.NoError(t, err)
		require.Zero(t, stats.LiveBytesMoved)
		require.Zero(t, stats.FilesDeleted)
	}
	checkCoalescedAccounts(t, db, live)
	for _, a := range live {
		require.NoError(t, db.Index.Delete(a.Key[:], pebble.NoSync))
	}
	stats, err := db.CompactOnce(CompactionConfig{})
	require.NoError(t, err)
	require.Equal(t, 2, stats.FilesDeleted)
	require.False(t, coalescedSourcesExist(paths))
}

func TestCoalescedCompactionFailureRecovery(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, stage := range []string{"after-data", "after-manifest", "after-index", "before-unlink"} {
			t.Run(fmt.Sprintf("wal-off=%v/%s", walOff, stage), func(t *testing.T) {
				old := DisableIndexWAL
				DisableIndexWAL = walOff
				defer func() { DisableIndexWAL = old }()
				db, paths, live := newCoalescedTestDb(t, 1)
				defer func() { db.CloseDb() }()
				boom := errors.New("injected compaction failure")
				db.coalescedCompactHook = func(s string) error {
					if s == stage {
						return boom
					}
					return nil
				}
				cfg := CompactionConfig{MinDeadFraction: 0.5}
				_, err := db.CompactOnce(cfg)
				require.NoError(t, err) // assessment
				_, err = db.CompactOnce(cfg)
				require.ErrorIs(t, err, boom)
				require.True(t, coalescedSourcesExist(paths))
				checkCoalescedAccounts(t, db, live)
				db = reopenCoalescedTestDb(t, db, paths)
				checkCoalescedAccounts(t, db, live)
				for i := 0; i < 10 && coalescedSourcesExist(paths); i++ {
					_, err = db.CompactOnce(cfg)
					require.NoError(t, err)
				}
				require.False(t, coalescedSourcesExist(paths))
				checkCoalescedAccounts(t, db, live)
			})
		}
	}
}

func TestCoalescedCompactionPinsInvalidateProgress(t *testing.T) {
	db, paths, live := newCoalescedTestDb(t, 1)
	defer func() { db.CloseDb() }()
	cfg := CompactionConfig{MinDeadFraction: 0.5, RewindHorizonBatches: 1}
	commitTestBatch(t, db, 101, foldAcct(9, 9, []byte("boundary")))
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.NotNil(t, db.coalescedScans[0])
	commitTestBatch(t, db, 102, foldAcct(1, 999, []byte("changed")))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Nil(t, db.coalescedScans[0], "new undo pin invalidates an earlier scan")
	require.True(t, coalescedSourcesExist(paths))
	_, err = db.RewindToBatchBoundary(101)
	require.NoError(t, err)
	checkCoalescedAccounts(t, db, live)
	// Re-establish a head that does not pin the source and complete evacuation.
	commitTestBatch(t, db, 103, foldAcct(9, 10, []byte("next")))
	for i := 0; i < 10 && coalescedSourcesExist(paths); i++ {
		_, err = db.CompactOnce(cfg)
		require.NoError(t, err)
	}
	require.False(t, coalescedSourcesExist(paths))
	db = reopenCoalescedTestDb(t, db, paths)
	checkCoalescedAccounts(t, db, live)
}

func TestCoalescedCompactionRejectsInvalidIndex(t *testing.T) {
	for _, kind := range []string{"outside", "wrong-key", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			db, paths, live := newCoalescedTestDb(t, 1)
			defer db.CloseDb()
			if kind == "truncated" {
				require.NoError(t, os.Truncate(filepath.Join(paths[0], "accounts", "data"), 4096+hdrLen+1))
			} else {
				offset := uint64(0)
				if kind == "outside" {
					offset = 1 << 40
				}
				var b [24]byte
				(&AccountIndexEntry{Slot: 70, FileId: 0, Offset: offset}).Marshal(&b)
				require.NoError(t, db.Index.Set(live[0].Key[:], b[:], pebble.NoSync))
			}
			_, err := db.CompactOnce(CompactionConfig{})
			require.Error(t, err)
			require.True(t, coalescedSourcesExist(paths))
		})
	}
}

func TestCoalescedCompactionConcurrentReaders(t *testing.T) {
	db, paths, live := newCoalescedTestDb(t, 2)
	defer db.CloseDb()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 1)
	keys := []solana.PublicKey{live[0].Key, live[3].Key}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, k := range keys {
				db.CommonAcctsCache.Delete(k)
			}
			got, err := db.GetAccountsBatch(context.Background(), 100, keys)
			if err == nil && (len(got) != 2 || !bytes.Equal(got[0].Data, live[0].Data) || !bytes.Equal(got[1].Data, live[3].Data)) {
				err = errors.New("changed account data")
			}
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	for i := 0; i < 30 && coalescedSourcesExist(paths); i++ {
		_, err := db.CompactOnce(CompactionConfig{MinDeadFraction: 0.5, MaxMoveBytesPerCycle: 150})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.False(t, coalescedSourcesExist(paths))
}

// Exit without CloseDb to test the actual on-disk commit boundaries. In
// particular, a graceful test teardown must not flush a WAL-disabled index
// and accidentally make an unsafe compaction appear durable.
func TestCoalescedCompactionProcessExit(t *testing.T) {
	if root := os.Getenv("MITHRIL_TEST_COMPACT_ROOT"); root != "" {
		DisableIndexWAL = os.Getenv("MITHRIL_TEST_COMPACT_WAL_OFF") == "true"
		db, err := OpenDbPaths([]string{root})
		if err != nil {
			t.Fatal(err)
		}
		db.RootedDurable = true
		db.coalescedCompactHook = func(stage string) error {
			if stage == os.Getenv("MITHRIL_TEST_COMPACT_STAGE") {
				os.Exit(93)
			}
			return nil
		}
		for i := 0; i < 3; i++ {
			if _, err = db.CompactOnce(CompactionConfig{MinDeadFraction: 0.5}); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("crash point not reached")
	}
	for _, walOff := range []bool{false, true} {
		for _, stage := range []string{"after-data", "after-manifest", "after-index", "before-unlink"} {
			t.Run(fmt.Sprintf("wal-off=%v/%s", walOff, stage), func(t *testing.T) {
				old := DisableIndexWAL
				DisableIndexWAL = walOff
				defer func() { DisableIndexWAL = old }()
				db, paths, live := newCoalescedTestDb(t, 1)
				db.CloseDb()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCoalescedCompactionProcessExit$")
				cmd.Env = append(os.Environ(), "MITHRIL_TEST_COMPACT_ROOT="+paths[0], "MITHRIL_TEST_COMPACT_STAGE="+stage, fmt.Sprintf("MITHRIL_TEST_COMPACT_WAL_OFF=%v", walOff))
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				require.Equal(t, 93, exit.ExitCode(), string(output))
				db, err = OpenDbPaths(paths)
				require.NoError(t, err)
				defer db.CloseDb()
				db.RootedDurable = true
				db.InitCaches()
				_, err = db.RecoverFoldState()
				require.NoError(t, err)
				checkCoalescedAccounts(t, db, live)
				for i := 0; i < 10 && coalescedSourcesExist(paths); i++ {
					_, err = db.CompactOnce(CompactionConfig{MinDeadFraction: 0.5})
					require.NoError(t, err)
				}
				require.False(t, coalescedSourcesExist(paths))
				checkCoalescedAccounts(t, db, live)
			})
		}
	}
}
