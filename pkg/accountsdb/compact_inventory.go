package accountsdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// Compaction used to list the accounts directory twice and lstat every data
// file in every cycle. On mainnet that directory holds about 480,000 bootstrap
// appendvecs, so each cycle spent about 1.5 s of CPU and 300 MiB of garbage on
// bookkeeping while holding foldMu. The inventory keeps that view in memory.
//
// It is maintained only where files change often: fold (CommitBatch) and
// compaction itself. Every other directory mutation (recovery, rewind, rewind
// intent) and incomplete file-mutating operations invalidate it; the next cycle
// rebuilds it from the directory. A periodic rebuild compares
// against the tracked view and logs any drift, so an untracked writer shows up
// as a warning instead of silently starving reclamation.
//
// Compaction's correctness does not depend on the inventory being complete: a
// missing data file is merely not compacted until the next rebuild, a vanished
// one is skipped, and rewind pins still fail closed on any missing in-horizon
// fold manifest. FoldAndParkedManifests does depend on the manifest names
// being complete; it says why they are. Guarded by foldMu.
//
// A store that has not compacted yet (or never does: compaction can be
// disabled) may hold a names-only inventory, built for FoldAndParkedManifests
// by listing the primary directory without a stat per file. Its data map is
// nil. The first compaction cycle replaces it with a full one.
type compactionInventory struct {
	data      map[string]inventoryDataFile // nil in a names-only inventory
	manifests map[string]struct{}          // ".manifest" and ".manifest.rewound" names, not tmp files
	cycles    uint64                       // uses served since the last scan
}

func (inv *compactionInventory) namesOnly() bool { return inv.data == nil }

type inventoryDataFile struct {
	slot, fileId    uint64
	size, allocated int64
}

// compactionInventoryVerifyCycles bounds how long drift from an untracked
// writer can last; a full scan costs about one old cycle's bookkeeping.
var compactionInventoryVerifyCycles uint64 = 600

// compactionInventoryStrict turns drift that only a missed tracking call can
// cause (an untracked file, a wrong size) into a panic (test knob). Stale
// entries come from external deletion and fail closed on their own.
var compactionInventoryStrict bool

// scanCompactionInventory lists every accounts directory. Manifests live in
// the primary; a data file counts only in the directory its file ID maps to.
func (db *AccountsDb) scanCompactionInventory() (*compactionInventory, error) {
	inv := &compactionInventory{data: make(map[string]inventoryDataFile), manifests: make(map[string]struct{})}
	for _, dir := range db.AccountsDirs() {
		if err := db.scanCompactionInventoryDir(inv, dir); err != nil {
			return nil, err
		}
	}
	return inv, nil
}

func (db *AccountsDb) scanCompactionInventoryDir(inv *compactionInventory, dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if isTrackedManifestName(name) {
			if dir == db.AcctsDir {
				inv.manifests[name] = struct{}{}
			}
			continue
		}
		slot, fileId, ok := parseDataFileName(name)
		if !ok || db.appendVecDir(fileId) != dir {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue // vanished between listing and stat, as the old scan did
		}
		inv.data[name] = inventoryDataFile{slot: slot, fileId: fileId, size: info.Size(), allocated: allocatedBytesFromInfo(info)}
	}
	return nil
}

// scanManifestNames builds a names-only inventory: one pass over the primary
// directory, where every manifest lives, in batches and without a stat per
// entry. That is the cost of one ListFoldAndParkedManifests listing, against
// roughly eight times that for the full scan's stat of every data file.
func (db *AccountsDb) scanManifestNames() (*compactionInventory, error) {
	dir, err := os.Open(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	inv := &compactionInventory{manifests: make(map[string]struct{})}
	for {
		entries, readErr := dir.ReadDir(1024)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		for _, e := range entries {
			if name := e.Name(); !e.IsDir() && isTrackedManifestName(name) {
				inv.manifests[name] = struct{}{}
			}
		}
		if readErr == io.EOF {
			return inv, nil
		}
	}
}

func isTrackedManifestName(name string) bool {
	if strings.HasSuffix(name, segManifestTmpSuffix) {
		return false
	}
	return strings.HasSuffix(name, segManifestSuffix) || strings.HasSuffix(name, segManifestRewoundSuffix)
}

// compactionInventoryLocked returns the current full inventory, rebuilding it
// after an invalidation, replacing a names-only one, and periodically
// re-verifying a tracked one.
func (db *AccountsDb) compactionInventoryLocked() (*compactionInventory, error) {
	inv := db.compactInventory
	if inv != nil && !inv.namesOnly() && inv.cycles < compactionInventoryVerifyCycles {
		inv.cycles++
		return inv, nil
	}
	fresh, err := db.scanCompactionInventory()
	if err != nil {
		db.compactInventory = nil
		return nil, err
	}
	db.adoptInventoryLocked(inv, fresh)
	return fresh, nil
}

// manifestInventoryLocked returns an inventory whose manifest names are
// current, which is all FoldAndParkedManifests needs. It serves whatever
// inventory exists. With none, it builds a names-only one; a store that never
// compacts therefore never pays for, or retains, the per-data-file view. Uses
// count toward the same periodic rebuild as compaction cycles.
func (db *AccountsDb) manifestInventoryLocked() (*compactionInventory, error) {
	inv := db.compactInventory
	if inv != nil && inv.cycles < compactionInventoryVerifyCycles {
		inv.cycles++
		return inv, nil
	}
	if inv != nil && !inv.namesOnly() {
		return db.compactionInventoryLocked()
	}
	fresh, err := db.scanManifestNames()
	if err != nil {
		db.compactInventory = nil
		return nil, err
	}
	db.adoptInventoryLocked(inv, fresh)
	return fresh, nil
}

// adoptInventoryLocked installs a freshly scanned inventory, reporting how the
// tracked one (if any) had drifted from the directory.
func (db *AccountsDb) adoptInventoryLocked(tracked, fresh *compactionInventory) {
	if tracked != nil {
		if drift := tracked.diff(fresh); len(drift) > 0 {
			db.compactInventoryDrift += uint64(len(drift))
			if compactionInventoryStrict {
				for _, d := range drift {
					if !strings.HasPrefix(d, "stale ") {
						panic(fmt.Sprintf("compaction inventory drift: %v", drift))
					}
				}
			}
			mlog.Log.Warnf("accountsdb: compaction inventory drifted from the directory (%d entries, adopting the directory): %s", len(drift), strings.Join(drift[:min(len(drift), 8)], "; "))
		}
	}
	db.compactInventory = fresh
}

// FoldAndParkedManifests returns what ListFoldAndParkedManifests returns for
// the primary accounts directory: active fold headers ascending by BatchSeq
// and parked manifest paths. The manifest names come from the inventory, so a
// caller that asks after every fold (transaction-status checkpoint retention)
// does not list hundreds of thousands of appendvecs to find a few dozen
// manifests. Headers are still read from the files on every call, because
// sparse compaction rewrites an expired fold manifest in place as compact-kind.
//
// Unlike compaction, this caller depends on the view being complete: a fold or
// parked manifest missing from it would let the checkpoint it references be
// removed. Completeness holds because every writer of those names either
// records the name (CommitBatch, compaction) or drops the inventory (recovery,
// rewind, a failed fold or compaction cycle), and the first call after a drop
// rebuilds the names from the directory (one listing, no stat per file, when
// compaction has not built the full inventory). Like every inventory use, the
// call counts toward the periodic rebuild. A listed manifest that no longer
// exists means something outside the store removed it; the inventory is then
// rebuilt once before answering, which is what a directory listing would have
// shown.
//
// It takes foldMu, so it must not be called from inside a fold, a compaction
// cycle, recovery or rewind.
func (db *AccountsDb) FoldAndParkedManifests() ([]ManifestHeader, []string, error) {
	db.foldMu.LockAs("manifest_listing")
	defer db.foldMu.Unlock()
	for rebuilt := false; ; rebuilt = true {
		inv, err := db.manifestInventoryLocked()
		if err != nil {
			return nil, nil, err
		}
		var listing manifestListing
		stale := false
		for name := range inv.manifests {
			if err := listing.add(db.AcctsDir, name); err != nil && os.IsNotExist(err) {
				stale = true
			}
			if strings.HasSuffix(name, segManifestRewoundSuffix) {
				if _, err := os.Lstat(filepath.Join(db.AcctsDir, name)); err != nil && os.IsNotExist(err) {
					stale = true
				}
			}
		}
		if stale && !rebuilt {
			db.compactInventoryDrift++
			mlog.Log.Warnf("accountsdb: a manifest in the compaction inventory is gone from the directory; rebuilding the inventory")
			db.invalidateCompactionInventoryLocked()
			continue
		}
		folds, parked := listing.sorted()
		return folds, parked, nil
	}
}

// invalidateCompactionInventoryLocked drops the directory view and, with it,
// the cached rewind pins.
func (db *AccountsDb) invalidateCompactionInventoryLocked() {
	db.compactInventory = nil
	db.compactPinCache = nil
}

// verifyManifestCRC checks a manifest's whole body against its CRC trailer, as
// ReadSegmentManifest does, without decoding it, streaming through a buffer
// reused across calls. It returns the verified CRC.
func (db *AccountsDb) verifyManifestCRC(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < 4 {
		return 0, ErrTornManifest
	}
	if db.compactCRCBuf == nil {
		db.compactCRCBuf = make([]byte, 1<<20)
	}
	h := crc32.NewIEEE()
	if _, err := io.CopyBuffer(h, io.LimitReader(f, info.Size()-4), db.compactCRCBuf); err != nil {
		return 0, err
	}
	var tail [4]byte
	if _, err := io.ReadFull(f, tail[:]); err != nil {
		return 0, ErrTornManifest
	}
	if h.Sum32() != binary.LittleEndian.Uint32(tail[:]) {
		return 0, ErrTornManifest
	}
	return h.Sum32(), nil
}

// trackDataFileLocked records a data file this process has just written and
// synced. A failed stat invalidates the inventory instead.
func (db *AccountsDb) trackDataFileLocked(name string) {
	inv := db.compactInventory
	if inv == nil || inv.namesOnly() {
		return
	}
	slot, fileId, ok := parseDataFileName(name)
	info, err := os.Lstat(filepath.Join(db.appendVecDir(fileId), name))
	if !ok || err != nil {
		db.invalidateCompactionInventoryLocked()
		return
	}
	inv.data[name] = inventoryDataFile{slot: slot, fileId: fileId, size: info.Size(), allocated: allocatedBytesFromInfo(info)}
}

func (db *AccountsDb) trackManifestLocked(path string) {
	if inv := db.compactInventory; inv != nil {
		inv.manifests[filepath.Base(path)] = struct{}{}
	}
}

func (db *AccountsDb) trackAllocatedLocked(name string, allocated int64) {
	if inv := db.compactInventory; inv != nil {
		if f, ok := inv.data[name]; ok {
			f.allocated = allocated
			inv.data[name] = f
		}
	}
}

// trackRemovedLocked drops a data file and its manifest after a successful
// removeSourceFiles.
func (db *AccountsDb) trackRemovedLocked(dataPath, manifestPath string) {
	if inv := db.compactInventory; inv != nil {
		delete(inv.data, filepath.Base(dataPath))
		if manifestPath != "" {
			delete(inv.manifests, filepath.Base(manifestPath))
		}
	}
}

// diff reports names whose presence or recorded size/allocation differs. Data
// files are compared only when both sides track them.
func (inv *compactionInventory) diff(fresh *compactionInventory) []string {
	var out []string
	if !inv.namesOnly() && !fresh.namesOnly() {
		for name, f := range inv.data {
			g, ok := fresh.data[name]
			switch {
			case !ok:
				out = append(out, "stale data file "+name)
			case f != g:
				out = append(out, fmt.Sprintf("data file %s size/allocated %d/%d, directory %d/%d", name, f.size, f.allocated, g.size, g.allocated))
			}
		}
		for name := range fresh.data {
			if _, ok := inv.data[name]; !ok {
				out = append(out, "untracked data file "+name)
			}
		}
	}
	for name := range inv.manifests {
		if _, ok := fresh.manifests[name]; !ok {
			out = append(out, "stale manifest "+name)
		}
	}
	for name := range fresh.manifests {
		if _, ok := inv.manifests[name]; !ok {
			out = append(out, "untracked manifest "+name)
		}
	}
	sort.Strings(out)
	return out
}

// Lock diagnostics budgets. foldMu serializes folds against compaction,
// recovery and rewind, so a long hold delays folding; appendVecReadMu and
// readCacheEpochMu sit on the account read path, so short waits matter.
var (
	foldLockStat       = metrics.Lock("accountsdb.fold", 250*time.Millisecond, time.Second)
	appendVecLockStat  = metrics.Lock("accountsdb.append_vec_read", 20*time.Millisecond, 100*time.Millisecond)
	cacheEpochLockStat = metrics.Lock("accountsdb.read_cache_epoch", 10*time.Millisecond, 50*time.Millisecond)
)

func (db *AccountsDb) bindLockDiagnostics() {
	db.foldMu.Bind(foldLockStat)
	db.appendVecReadMu.Bind(appendVecLockStat)
	db.readCacheEpochMu.Bind(cacheEpochLockStat)
}
