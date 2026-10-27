package snapshot

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// appendVecDirs places unpacked appendvec files across the accounts
// directories, one per disk. Each file goes to the directory with the fewest
// bytes currently being written, so the disk that drains fastest takes the
// most data; a directory whose filesystem is down to its free-space reserve
// stops receiving files, so disks of different sizes each fill to capacity.
// A file in directory d out of n gets fileId origId*n+d, which is how the
// AccountsDB finds it again (see accountsdb/accounts_dirs.go). With one
// directory, files keep their archive IDs and the layout is unchanged.
type appendVecDirs struct {
	dirs     []string
	inflight []atomic.Int64
	written  []atomic.Int64
	largest  atomic.Uint64 // highest file ID placed; the build's largest_file_id

	reserve  int64
	headroom []atomic.Int64 // free minus in-flight minus reserve at last refresh, less writes since
	stop     chan struct{}
	stopped  chan struct{}
	warnOnce sync.Once
	closed   sync.Once
}

var (
	// Test hooks; production reads statfs every refresh interval.
	dirAvailableBytes      = accountsdb.AvailableDiskBytes
	dirFreeRefreshInterval = 250 * time.Millisecond
	dirSyncFilesystem      = syncFilesystem
)

func openAppendVecDirs(dirs []string) (*appendVecDirs, error) {
	if err := accountsdb.ValidateStorageDirectories(dirs); err != nil {
		return nil, err
	}
	d := &appendVecDirs{
		dirs:     dirs,
		inflight: make([]atomic.Int64, len(dirs)),
		written:  make([]atomic.Int64, len(dirs)),
		reserve:  int64(min(SnapshotAccountsDirMinFreeBytes, math.MaxInt64)),
		headroom: make([]atomic.Int64, len(dirs)),
	}
	if len(dirs) > 1 {
		d.refreshHeadroom()
		for i, dir := range dirs {
			mlog.Log.Infof("snapshot accounts dir %d: %s, %d MiB usable above the %d MiB reserve", i, dir, max(d.headroom[i].Load(), 0)>>20, d.reserve>>20)
		}
		d.stop, d.stopped = make(chan struct{}), make(chan struct{})
		go d.refreshLoop()
	}
	return d, nil
}

func (d *appendVecDirs) n() uint64 { return uint64(len(d.dirs)) }

func (d *appendVecDirs) refreshLoop() {
	defer close(d.stopped)
	ticker := time.NewTicker(dirFreeRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			d.refreshHeadroom()
		}
	}
}

// refreshHeadroom re-reads free space. Bytes being written are not yet visible
// to statfs, so they are subtracted explicitly. Other writers on the same
// filesystem (index logs and SSTs on the primary) are picked up here.
func (d *appendVecDirs) refreshHeadroom() {
	for i, dir := range d.dirs {
		free, err := dirAvailableBytes(dir)
		if err != nil {
			// Unknown capacity must not stop the unpack; ENOSPC still reports a
			// genuinely full disk.
			d.headroom[i].Store(math.MaxInt64)
			continue
		}
		d.headroom[i].Store(int64(min(free, math.MaxInt64)) - d.inflight[i].Load() - d.reserve)
	}
}

func (d *appendVecDirs) choose() int {
	if len(d.dirs) == 1 {
		return 0
	}
	best, bestQ, bestW := -1, int64(math.MaxInt64), int64(math.MaxInt64)
	for i := range d.dirs {
		if d.headroom[i].Load() <= 0 {
			continue
		}
		q, w := d.inflight[i].Load(), d.written[i].Load()
		if q < bestQ || (q == bestQ && w < bestW) {
			best, bestQ, bestW = i, q, w
		}
	}
	if best >= 0 {
		return best
	}
	// Every disk is at its reserve: keep writing where the most room is left,
	// and let the filesystem decide whether it actually fits.
	d.warnOnce.Do(func() {
		mlog.Log.Warnf("snapshot unpack: every accounts directory is below its %d MiB free-space reserve", d.reserve>>20)
	})
	best, bestH := 0, int64(math.MinInt64)
	for i := range d.dirs {
		if h := d.headroom[i].Load(); h > bestH {
			best, bestH = i, h
		}
	}
	return best
}

// place picks the directory for an appendvec of size bytes and returns it with
// the file ID the AccountsDB will use. The caller must call finish when the
// write ends.
func (d *appendVecDirs) place(origFileId uint64, size int) (dir int, fileId uint64) {
	dir = d.choose()
	fileId = origFileId
	if n := d.n(); n > 1 {
		fileId = origFileId*n + uint64(dir)
	}
	d.inflight[dir].Add(int64(size))
	d.written[dir].Add(int64(size))
	d.headroom[dir].Add(-int64(size))
	for {
		prev := d.largest.Load()
		if fileId <= prev || d.largest.CompareAndSwap(prev, fileId) {
			break
		}
	}
	return dir, fileId
}

func (d *appendVecDirs) finish(dir int, size int) {
	d.inflight[dir].Add(-int64(size))
}

func (d *appendVecDirs) close() {
	d.closed.Do(func() {
		if d.stop != nil {
			close(d.stop)
			<-d.stopped
		}
	})
}

// syncAll makes every unpacked appendvec durable with one filesystem sync per
// accounts directory (instead of an fsync per file), then syncs the
// directories themselves. Run it before the build is reported complete.
func (d *appendVecDirs) syncAll() error {
	for _, dir := range d.dirs {
		if err := dirSyncFilesystem(dir); err != nil {
			return fmt.Errorf("syncing %s: %w", dir, err)
		}
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("syncing directory %s: %w", dir, err)
		}
	}
	return nil
}

// startSync runs syncAll in the background, overlapping the index flush. The
// returned wait reports its result and may be called any number of times. A
// build defers it right away, so no return path, including an error, leaves
// the sync running over directories a retry or cleanup may be touching.
func (d *appendVecDirs) startSync() (wait func() error) {
	done := make(chan struct{})
	var err error
	go func() {
		err = d.syncAll()
		close(done)
	}()
	return func() error {
		<-done
		return err
	}
}

// syncBuildMetadata fsyncs the small files a completed build writes next to the
// index, and their directory, before the build is reported complete.
func syncBuildMetadata(accountsDbDir string) error {
	for _, name := range []string{accountsdb.AccountsDirCountFile, "largest_file_id", "bootstrap_high_file_id", "bank_hash", "stake_pubkeys.idx"} {
		f, err := os.Open(filepath.Join(accountsDbDir, name))
		if os.IsNotExist(err) {
			continue // accounts_dir_count is absent for single-directory builds
		}
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return fmt.Errorf("sync %s: %w", name, err)
		}
	}
	return syncDir(accountsDbDir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
