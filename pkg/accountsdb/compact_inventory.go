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
// Correctness does not depend on the inventory being complete: a missing data
// file is merely not compacted until the next rebuild, a vanished one is
// skipped, and rewind pins still fail closed on any missing in-horizon fold
// manifest. Guarded by foldMu.
type compactionInventory struct {
	data      map[string]inventoryDataFile
	manifests map[string]struct{} // ".manifest" and ".manifest.rewound" names, not tmp files
	cycles    uint64              // cycles served since the last full scan
}

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

func scanCompactionInventory(dir string) (*compactionInventory, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	inv := &compactionInventory{data: make(map[string]inventoryDataFile), manifests: make(map[string]struct{})}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if isTrackedManifestName(name) {
			inv.manifests[name] = struct{}{}
			continue
		}
		slot, fileId, ok := parseDataFileName(name)
		if !ok {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue // vanished between listing and stat, as the old scan did
		}
		inv.data[name] = inventoryDataFile{slot: slot, fileId: fileId, size: info.Size(), allocated: allocatedBytesFromInfo(info)}
	}
	return inv, nil
}

func isTrackedManifestName(name string) bool {
	if strings.HasSuffix(name, segManifestTmpSuffix) {
		return false
	}
	return strings.HasSuffix(name, segManifestSuffix) || strings.HasSuffix(name, segManifestRewoundSuffix)
}

// compactionInventoryLocked returns the current inventory, rebuilding it after
// an invalidation and periodically re-verifying a tracked one.
func (db *AccountsDb) compactionInventoryLocked() (*compactionInventory, error) {
	inv := db.compactInventory
	if inv != nil && inv.cycles < compactionInventoryVerifyCycles {
		inv.cycles++
		return inv, nil
	}
	fresh, err := scanCompactionInventory(db.AcctsDir)
	if err != nil {
		db.compactInventory = nil
		return nil, err
	}
	if inv != nil {
		if drift := inv.diff(fresh); len(drift) > 0 {
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
	return fresh, nil
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
	if inv == nil {
		return
	}
	slot, fileId, ok := parseDataFileName(name)
	info, err := os.Lstat(filepath.Join(db.AcctsDir, name))
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

// diff reports names whose presence or recorded size/allocation differs.
func (inv *compactionInventory) diff(fresh *compactionInventory) []string {
	var out []string
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
