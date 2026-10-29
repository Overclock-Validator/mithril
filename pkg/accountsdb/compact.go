package accountsdb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/cockroachdb/pebble/v2"
)

// Compaction reclaims dead bytes from the append-only store. Folds never
// overwrite: every re-written account leaves its previous version behind as
// dead bytes in an older segment or bootstrap appendvec. CompactOnce scans
// mostly-dead files and moves the still-live records into a fresh output file
// (1 source -> 1 output), then deletes the source. Large manifest-backed files
// instead release obsolete payload pages in place; see sparsifyFile.
//
// The pin rule (invariant I5) is what keeps rewind sound: a file is untouchable
// while it is inside the rewind horizon, including the target boundary itself.
// An older file named by an in-horizon undo pointer cannot be relocated, but
// sparse reclamation may free other obsolete payloads in it while preserving
// every referenced record at its exact location. Bootstrap/small files retain
// the conservative whole-file pin for undo references.
//
// Liveness is exact, not heuristic: a record is live iff the index still maps
// its pubkey to this exact (fileId, offset). FileIds are never reused (I7), so
// the check cannot alias across files.
//
// Concurrency: runs under foldMu, so it is serialized against CommitBatch,
// recovery, and rewind. It requires rooted-durable mode because the legacy
// direct-store path writes the index without foldMu and would race the
// liveness scan. Concurrent READERS are safe: the source file is unlinked only
// after the moved index entries are durable, and a reader that loses the race
// (fetched the old entry, then the file vanished) retries once with a fresh
// index entry (getStoredAccount).
//
// Crash safety (matrix C5): the output file + compact-kind manifest are written
// and fsynced BEFORE the index moves, and the source is unlinked only after the
// index move is durable (WAL fsync, or explicit Flush when the WAL is off —
// recovery does NOT replay compact manifests, their only job is marking the
// output as non-orphan). A crash anywhere leaves either duplicate bytes (source
// still authoritative) or a fully-dead source; both converge on the next cycle.

// CompactionConfig bounds one CompactOnce cycle.
type CompactionConfig struct {
	// RewindHorizonBatches pins the newest N fold batches, their target boundary
	// and every record their undo pointers name. Must match the operational rewind
	// horizon. Clamped to >= 1 so the committed head fold is always pinned.
	RewindHorizonBatches uint64
	// MinDeadFraction is the dead-byte fraction a file must reach before its
	// live records are moved. Defaults to 0.7.
	MinDeadFraction float64
	// MaxMoveBytesPerCycle caps live bytes rewritten per call (wear/latency
	// bound). Deleting fully-dead files is free and not counted. Default 256MB.
	MaxMoveBytesPerCycle int64
	// MaxScanBytesPerCycle caps bytes read for liveness scans per call (read
	// churn bound; mostly-live files cost a scan but yield no move). Progress
	// across cycles is kept by a directory cursor. Default 1GB.
	MaxScanBytesPerCycle int64
	// MaxFilesPerCycle and MaxDuration bound metadata-only reclamation work.
	// A single candidate can exceed the duration; it is never interrupted in
	// the middle of publishing its durable reclamation marker.
	MaxFilesPerCycle int
	MaxDuration      time.Duration
}

// CompactStats reports one CompactOnce cycle.
type CompactStats struct {
	CandidatesScanned int
	FilesCompacted    int
	FilesDeleted      int // fully-dead fast path (no bytes moved)
	FilesSparsified   int // dead payload pages released without moving live/undo records
	LiveBytesMoved    int64
	BytesReclaimed    int64 // source bytes freed (compacted + deleted)
}

const (
	defaultMinDeadFraction      = 0.7
	defaultMaxMoveBytesPerCycle = int64(256 << 20)
	defaultMaxScanBytesPerCycle = int64(1 << 30)
)

type compactCandidate struct {
	name          string
	path          string
	slot          uint64
	fileId        uint64
	size          int64
	allocated     int64
	manifestPath  string // the source's own manifest; "" for bootstrap appendvecs
	alreadySparse bool
	scanOrder     uint64
}

// CompactOnce runs one bounded compaction cycle and returns what it did.
func (db *AccountsDb) CompactOnce(cfg CompactionConfig) (CompactStats, error) {
	stats := CompactStats{}
	if !db.RootedDurable {
		return stats, fmt.Errorf("accountsdb: compaction requires rooted-durable mode (the direct store path writes the index outside foldMu)")
	}
	if cfg.RewindHorizonBatches == 0 {
		cfg.RewindHorizonBatches = 1
	}
	if cfg.MinDeadFraction <= 0 {
		cfg.MinDeadFraction = defaultMinDeadFraction
	}
	if cfg.MaxMoveBytesPerCycle <= 0 {
		cfg.MaxMoveBytesPerCycle = defaultMaxMoveBytesPerCycle
	}
	if cfg.MaxScanBytesPerCycle <= 0 {
		cfg.MaxScanBytesPerCycle = defaultMaxScanBytesPerCycle
	}
	if cfg.MaxFilesPerCycle <= 0 {
		cfg.MaxFilesPerCycle = 16
	}

	db.foldMu.LockAs("compaction")
	defer db.foldMu.Unlock()
	// A cycle that stops early (error or panic) after it started reclaiming
	// files may have changed some the inventory has not recorded yet; the next
	// cycle rescans the directory. Failures before that point (pin setup, the
	// history floor, which lives in the index) change no files, so a store
	// whose pin setup keeps failing does not rescan on every cycle.
	touchedFiles, completed := false, false
	defer func() {
		if touchedFiles && !completed {
			db.invalidateCompactionInventoryLocked()
		}
	}()

	if db.rewindPending {
		return stats, fmt.Errorf("accountsdb: pending rewind state must be persisted before compaction")
	}

	meta, _, err := db.readFoldMeta()
	if err != nil {
		return stats, err
	}
	if db.compactScanned == nil || db.compactScanSeq != meta.BatchSeq {
		db.compactScanSeq = meta.BatchSeq
		db.compactScanned = make(map[uint64]struct{})
	}
	if db.compactLastScan == nil {
		db.compactLastScan = make(map[uint64]uint64)
	}

	inv, err := db.compactionInventoryLocked()
	if err != nil {
		return stats, err
	}
	pinned, manifestByFileId, err := db.compactionPinSet(meta.BatchSeq, cfg.RewindHorizonBatches, inv)
	if err != nil {
		return stats, err
	}

	floor, err := db.compactionHistoryFloor(meta.BatchSeq, cfg.RewindHorizonBatches)
	if err != nil {
		return stats, err
	}
	if floor > 0 {
		var boundary foldMeta
		for _, h := range manifestByFileId {
			if h.Kind == ManifestKindFold && h.BatchSeq == floor {
				if boundary.BatchSeq != 0 {
					return stats, fmt.Errorf("ambiguous compaction history boundary %d", floor)
				}
				boundary = foldMeta{BatchSeq: h.BatchSeq, ThroughSlot: h.ThroughSlot, FileId: h.FileId}
			}
		}
		if boundary.BatchSeq == 0 {
			return stats, fmt.Errorf("missing compaction history boundary %d", floor)
		}
		if err := db.persistCompactionHistoryFloor(boundary); err != nil {
			return stats, err
		}
	}

	fire(db.compactHooks.afterHistoryFloor)

	// Retiring a fold manifest (even a fully-dead file) requires its index
	// frontier to survive a crash. WAL-off folds otherwise rely on that redo log.
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return stats, err
		}
	}

	bootstrapHigh := db.bootstrapHighFileId()
	candidates := make([]compactCandidate, 0, len(inv.data))
	for name, file := range inv.data {
		slot, fileId := file.slot, file.fileId
		if _, isPinned := pinned.whole[fileId]; isPinned {
			continue
		}
		if _, scanned := db.compactScanned[fileId]; scanned {
			continue // no liveness or rewind-pin change since the successful scan
		}
		manifest, hasManifest := manifestByFileId[fileId]
		if !hasManifest && fileId > bootstrapHigh {
			continue // undecided orphan — recovery's to delete, not ours
		}
		allocated := file.allocated
		if allocated < sparseCompactionMinSize {
			delete(db.compactLastScan, fileId)
		}
		// Small files and bootstrap appendvecs still use relocation compaction.
		// Only manifest-backed large files support reclaiming individual dead
		// payloads while keeping all undo pointers at their original offsets.
		if len(pinned.records[fileId]) > 0 && (!hasManifest || file.size < sparseCompactionMinSize || !sparseCompactionSupported) {
			continue
		}
		candidates = append(candidates, compactCandidate{
			name: name, slot: slot, fileId: fileId, size: file.size, allocated: allocated, manifestPath: manifest.Path,
			alreadySparse: (manifest.Kind == ManifestKindCompact && manifest.DataCRC == 0) || db.compactLastScan[fileId] != 0,
			scanOrder:     db.compactLastScan[fileId],
		})
	}
	db.compactCycles++
	queue := newCompactCandidateQueue(candidates, db.sparseCursor, db.compactCursor, db.resparseNext, db.compactCycles%8 == 0)

	// A source manifest may be the only durable copy of its bankhashes.
	// Flush their WAL before this cycle can remove any such recovery record.
	for _, c := range candidates {
		if c.manifestPath != "" {
			if err := db.BankHashStore.LogData(nil, pebble.Sync); err != nil {
				return stats, fmt.Errorf("accountsdb: sync bankhashes before compaction: %w", err)
			}
			break
		}
	}

	// This cache is private to the foldMu-protected cycle. Sparse reclamation
	// never changes index locations; relocation below invalidates it.
	var indexCache sparseIndexCache
	var scannedBytes int64
	started := time.Now()
	touchedFiles = true
	for {
		if stats.LiveBytesMoved >= cfg.MaxMoveBytesPerCycle || scannedBytes >= cfg.MaxScanBytesPerCycle || stats.CandidatesScanned >= cfg.MaxFilesPerCycle ||
			(cfg.MaxDuration > 0 && stats.CandidatesScanned > 0 && time.Since(started) >= cfg.MaxDuration) {
			break
		}
		c, ok := queue.next()
		if !ok {
			break
		}
		// Paths are not part of candidate ordering. Construct one only for a
		// selected file, rather than for every entry in the inventory.
		c.path = filepath.Join(db.appendVecDir(c.fileId), c.name)
		switch compactCandidateClass(c) {
		case '0':
			db.sparseCursor = compactCandidateKey(c)
			db.resparseNext = true
		case '1':
			db.resparseNext = false
		default:
			db.compactCursor = compactCandidateKey(c)
		}
		stats.CandidatesScanned++
		if c.manifestPath != "" && c.size >= sparseCompactionMinSize && sparseCompactionSupported {
			reclaimed, scanned, deleted, err := db.sparsifyFile(c, pinned.records[c.fileId], &indexCache)
			scannedBytes += scanned
			if err != nil {
				return stats, fmt.Errorf("accountsdb: sparsify %s: %w", c.name, err)
			}
			db.compactScanned[c.fileId] = struct{}{}
			db.compactScanClock++
			db.compactLastScan[c.fileId] = db.compactScanClock
			if deleted {
				delete(db.compactLastScan, c.fileId)
			}
			stats.BytesReclaimed += reclaimed
			if deleted {
				stats.FilesDeleted++
			} else if reclaimed > 0 {
				stats.FilesSparsified++
			}
			continue
		}
		scannedBytes += c.size

		// compactFile may publish new index locations. Never reuse values across it.
		indexCache = sparseIndexCache{}
		acted, moved, err := db.compactFile(c, cfg.MinDeadFraction)
		if err != nil {
			return stats, fmt.Errorf("accountsdb: compact %s: %w", c.name, err)
		}
		switch {
		case !acted:
			// mostly live — leave it to decay further
		case moved == 0:
			stats.FilesDeleted++
			stats.BytesReclaimed += c.size
		default:
			stats.FilesCompacted++
			stats.LiveBytesMoved += moved
			stats.BytesReclaimed += c.size - moved
		}
	}
	completed = true
	if stats.FilesCompacted > 0 || stats.FilesDeleted > 0 || stats.FilesSparsified > 0 {
		mlog.Log.Infof("accountsdb: compaction cycle — %d scanned, %d compacted, %d sparsified, %d deleted, %s live moved, %s reclaimed",
			stats.CandidatesScanned, stats.FilesCompacted, stats.FilesSparsified, stats.FilesDeleted,
			humanBytes(stats.LiveBytesMoved), humanBytes(stats.BytesReclaimed))
	}
	return stats, nil
}

// All keys have the same two-byte prefix shape (class + ':'). Compare the
// class and existing name separately instead of building two strings for every
// sort comparison. Retry scan order still takes precedence within class '1'.
func compactCandidateLess(a, b compactCandidate) bool {
	ac, bc := compactCandidateClass(a), compactCandidateClass(b)
	if ac != bc {
		return ac < bc
	}
	if ac == '1' && a.scanOrder != b.scanOrder {
		return a.scanOrder < b.scanOrder
	}
	return a.name < b.name
}

func compactCandidateClass(c compactCandidate) byte {
	if c.manifestPath != "" && c.allocated >= sparseCompactionMinSize {
		if c.alreadySparse {
			// Hole punching retains record headers and edge pages. Repeatedly
			// walking a multi-GB logical file with only a few percent allocated
			// is expensive and seldom releases much. Keep it in the periodic
			// slow queue; reserve fast retries for substantial payload remains.
			if c.allocated >= c.size/10 {
				return '1'
			}
			return '2'
		}
		return '0'
	}
	return '2'
}

// Cursor encoding is unchanged; only the hot sort/classification paths avoid
// constructing it. Cursors are still compared using the original string key.
func compactCandidateKey(c compactCandidate) string {
	switch compactCandidateClass(c) {
	case '0':
		return "0:" + c.name
	case '1':
		return "1:" + c.name
	default:
		return "2:" + c.name
	}
}

func rotateCompactCandidates(candidates []compactCandidate, cursor string) []compactCandidate {
	i := sort.Search(len(candidates), func(i int) bool { return compactCandidateKey(candidates[i]) > cursor })
	rotated := make([]compactCandidate, 0, len(candidates))
	rotated = append(rotated, candidates[i:]...)
	return append(rotated, candidates[:i]...)
}

type compactionPins struct {
	whole map[uint64]struct{}
	// fileId -> offset -> expected account key. Undo references retain exact
	// records, not every obsolete account that happens to share their file.
	records map[uint64]map[uint64][32]byte
}

// compactionPinSet returns whole-file and exact undo-record pins (I5) plus a
// fileId -> header map for every current (non-parked) manifest.
//
// Whole-file pins: in-horizon segments including the target (BatchSeq >=
// head-horizon), and parked ".rewound" segments and undo targets. Other undo
// targets pin individual records, allowing sparse reclamation without relocation.
//
// Integrity is checked exactly as when every cycle re-read the full manifests:
// every header is read and validated, and every retained manifest's whole body
// is checked against its CRC trailer each cycle (streamed through a reusable
// buffer). Only the decoding is reused: pins are recomputed from the full
// manifests (about 65 x 3 MiB on mainnet) unless every retained manifest's
// name, complete header, size, mtime and verified CRC, and both floors, equal
// those of the last full decode, which changes about once per fold. The
// returned maps are shared with later cycles: read only.
func (db *AccountsDb) compactionPinSet(headSeq, horizon uint64, inv *compactionInventory) (*compactionPins, map[uint64]ManifestHeader, error) {
	horizonFloor, err := db.compactionHistoryFloor(headSeq, horizon)
	if err != nil {
		return nil, nil, err
	}
	retained, err := db.readCompactionHistoryFloor()
	if err != nil {
		return nil, nil, err
	}
	validationFloor := horizonFloor
	if retained.BatchSeq != 0 {
		validationFloor = retained.BatchSeq
	}
	manifestByFileId := make(map[uint64]ManifestHeader)
	retainedSeqs := make(map[uint64]bool)

	names := make([]string, 0, len(inv.manifests))
	for name := range inv.manifests {
		names = append(names, name)
	}
	sort.Strings(names)
	type retainedManifest struct {
		name, path string
		hdr        ManifestHeader
		parked     bool
	}
	var keep []retainedManifest
	var sig strings.Builder
	fmt.Fprintf(&sig, "%d/%d", horizonFloor, validationFloor)
	for _, name := range names {
		parked := strings.HasSuffix(name, segManifestRewoundSuffix)
		path := filepath.Join(db.AcctsDir, name)
		hdr, herr := readManifestHeader(path)
		if herr != nil {
			return nil, nil, fmt.Errorf("cannot establish rewind pins from %s: %w", name, herr)
		}
		dataName := strings.TrimSuffix(strings.TrimSuffix(name, ".rewound"), segManifestSuffix)
		slot, fileID, validName := parseDataFileName(dataName)
		if !validName || hdr.FileId != fileID || hdr.ThroughSlot != slot ||
			(hdr.Kind != ManifestKindFold && hdr.Kind != ManifestKindCompact) ||
			(hdr.Kind != ManifestKindFold && hdr.BatchSeq != 0) ||
			(hdr.Kind == ManifestKindFold && hdr.BatchSeq == 0) {
			return nil, nil, fmt.Errorf("cannot establish rewind pins: invalid manifest header %s", name)
		}
		if !parked {
			manifestByFileId[hdr.FileId] = hdr
		}
		if hdr.Kind != ManifestKindFold {
			continue
		}
		// Keep the target boundary itself as well as all undoable batches.
		if parked || hdr.BatchSeq >= validationFloor {
			info, serr := os.Lstat(path)
			if serr != nil {
				return nil, nil, fmt.Errorf("cannot establish rewind pins from %s: %w", name, serr)
			}
			crc, cerr := db.verifyManifestCRC(path)
			if cerr != nil {
				db.compactPinCache = nil
				return nil, nil, fmt.Errorf("cannot establish rewind pins from %s: %w", name, cerr)
			}
			fmt.Fprintf(&sig, "|%s:%d:%d:%d:%d:%d:%d:%d:%d:%d:%08x", name, hdr.Kind, hdr.BatchSeq, hdr.FromSlot, hdr.ThroughSlot,
				hdr.FileId, hdr.DataLen, hdr.DataCRC, info.Size(), info.ModTime().UnixNano(), crc)
			keep = append(keep, retainedManifest{name: name, path: path, hdr: hdr, parked: parked})
			retainedSeqs[hdr.BatchSeq] = true
		}
	}

	pinned := (*compactionPins)(nil)
	if cache := db.compactPinCache; cache != nil && cache.sig == sig.String() {
		pinned = cache.pinned
	} else {
		pinned = &compactionPins{whole: make(map[uint64]struct{}), records: make(map[uint64]map[uint64][32]byte)}
		for _, k := range keep {
			m, merr := ReadSegmentManifest(k.path)
			if merr != nil {
				db.compactPinCache = nil
				return nil, nil, fmt.Errorf("cannot establish rewind pins from %s: %w", k.name, merr)
			}
			// Validate the previously promised range before expiring more history.
			// A new head must not silently age out a missing/corrupt retained fold.
			if !k.parked && k.hdr.BatchSeq < horizonFloor {
				continue
			}
			pinned.whole[k.hdr.FileId] = struct{}{}
			if !k.parked && k.hdr.BatchSeq == horizonFloor {
				continue // boundary's own undo pointers are outside the horizon
			}
			for i := range m.Records {
				if m.Records[i].PrevValid {
					r := m.Records[i]
					if k.parked {
						pinned.whole[r.Prev.FileId] = struct{}{}
					}
					if pinned.records[r.Prev.FileId] == nil {
						pinned.records[r.Prev.FileId] = make(map[uint64][32]byte)
					}
					if key, exists := pinned.records[r.Prev.FileId][r.Prev.Offset]; exists && key != r.Pubkey {
						db.compactPinCache = nil
						return nil, nil, fmt.Errorf("conflicting undo pointers for file %d offset %d", r.Prev.FileId, r.Prev.Offset)
					}
					pinned.records[r.Prev.FileId][r.Prev.Offset] = r.Pubkey
				}
			}
		}
		db.compactPinCache = &compactionPinCache{sig: sig.String(), pinned: pinned}
	}
	// Never interpret a missing/corrupted in-horizon fold as an empty undo log.
	// In particular a damaged header must not silently age itself out of pins.
	for seq := max(uint64(1), validationFloor); seq <= headSeq && seq != 0; seq++ {
		if !retainedSeqs[seq] {
			return nil, nil, fmt.Errorf("cannot establish rewind pins: missing fold manifest seq %d", seq)
		}
	}
	return pinned, manifestByFileId, nil
}

// compactionPinCache holds the last computed pins and the retained-manifest
// signature they were computed from. Guarded by foldMu.
type compactionPinCache struct {
	sig    string
	pinned *compactionPins
}

// compactFile scans one candidate and, if dead enough, moves its live records
// to a fresh output file and unlinks the source. Returns (acted, liveBytesMoved).
// acted=false means the file was left alone (too live). liveBytesMoved==0 with
// acted=true means the fully-dead fast path (source deleted, nothing written).
func (db *AccountsDb) compactFile(c compactCandidate, minDeadFraction float64) (bool, int64, error) {
	srcPath := c.path
	data, err := os.ReadFile(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			db.invalidateCompactionInventoryLocked()
			return false, 0, nil // raced an external cleanup; nothing to do
		}
		return false, 0, err
	}

	pubkeys, idxEntries, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(data)), c.slot, c.fileId)
	if err != nil {
		return false, 0, err
	}

	// Exact liveness: the index still names this (fileId, offset). The entry's
	// slot must also equal the filename slot — the read path builds the file
	// name from the entry's slot, so a mismatched entry could not be served
	// from the output file and is safest left untouched.
	liveIdx, err := db.compactionLiveRecords(c, pubkeys, idxEntries)
	if err != nil {
		return false, 0, err
	}
	var liveBytes int64
	for _, i := range liveIdx {
		liveBytes += recordLenAt(data, idxEntries[i].Offset)
	}

	if len(data) == 0 || len(liveIdx) == 0 {
		// Fully dead (or empty bankhash-only segment past the horizon): no
		// index state references it; drop source + its manifest.
		db.appendVecReadMu.LockAs("compaction")
		err := removeSourceFiles(srcPath, c.manifestPath)
		db.appendVecReadMu.Unlock()
		if err != nil {
			return false, 0, err
		}
		db.trackRemovedLocked(srcPath, c.manifestPath)
		return true, 0, nil
	}

	deadFraction := 1.0 - float64(liveBytes)/float64(len(data))
	if deadFraction < minDeadFraction {
		return false, 0, nil
	}

	// Allocate the output fileId, persisting the high-water mark before the
	// first data byte (I7 — a crash must never lead to fileId reuse).
	// The output stays on the source's disk; its manifest lives in the primary.
	newFileId := db.nextFileId(int(c.fileId % db.dirCount()))
	if err := db.persistLargestFileId(); err != nil {
		return false, 0, err
	}

	// Raw-copy each live record span (header + data + alignment padding) so the
	// output is byte-identical per record; offsets are freshly assigned.
	var buf bytes.Buffer
	buf.Grow(int(liveBytes))
	records := make([]ManifestRecord, 0, len(liveIdx))
	for _, i := range liveIdx {
		off := idxEntries[i].Offset
		recLen := recordLenAt(data, off)
		records = append(records, ManifestRecord{
			Pubkey:    pubkeys[i],
			Offset:    uint64(buf.Len()),
			OwnerSlot: c.slot,
		})
		buf.Write(data[off : int64(off)+recLen])
	}

	outName := SegmentDataName(c.slot, newFileId)
	outPath := db.appendVecPath(c.slot, newFileId)
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return false, 0, err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return false, 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return false, 0, err
	}
	if err := f.Close(); err != nil {
		return false, 0, err
	}
	if err := fsyncDir(filepath.Dir(outPath)); err != nil {
		return false, 0, err
	}
	db.trackDataFileLocked(outName)

	// The compact manifest's only durable job is marking the output non-orphan;
	// recovery never replays it (a crash before the index move leaves the
	// source authoritative and the output as harmless duplicates).
	manifest := &SegmentManifest{
		Version:     segManifestVersion,
		Kind:        ManifestKindCompact,
		BatchSeq:    0,
		FromSlot:    c.slot,
		ThroughSlot: c.slot,
		FileId:      newFileId,
		DataLen:     uint64(buf.Len()),
		DataCRC:     crc32.ChecksumIEEE(buf.Bytes()),
		Records:     records,
	}
	if err := WriteSegmentManifest(db.AcctsDir, manifest); err != nil {
		return false, 0, err
	}
	db.trackManifestLocked(segmentManifestPath(db.AcctsDir, manifest.ThroughSlot, manifest.FileId))

	// Move the index entries in one batch, then make the move durable BEFORE
	// unlinking the source — with the WAL off that means an explicit Flush,
	// because compact manifests are not replayed at recovery. Exclude snapshot
	// readers from this short move-to-unlink window, so each sees either the old
	// path or the new path and never needs a cross-epoch per-key fallback.
	db.appendVecReadMu.LockAs("compaction")
	defer db.appendVecReadMu.Unlock()
	batch := db.Index.NewBatch()
	defer batch.Close()
	var idxBuf [24]byte
	for _, rec := range records {
		entry := AccountIndexEntry{Slot: c.slot, FileId: newFileId, Offset: rec.Offset}
		entry.Marshal(&idxBuf)
		if err := batch.Set(rec.Pubkey[:], idxBuf[:], nil); err != nil {
			return false, 0, err
		}
	}
	opts := pebble.Sync
	if db.IndexWALDisabled {
		opts = pebble.NoSync
	}
	if err := batch.Commit(opts); err != nil {
		return false, 0, err
	}
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return false, 0, err
		}
	}

	if err := removeSourceFiles(srcPath, c.manifestPath); err != nil {
		return false, 0, err
	}
	db.trackRemovedLocked(srcPath, c.manifestPath)
	return true, int64(buf.Len()), nil
}

// recordLenAt returns the byte length of the appendvec record starting at off
// (header + data + 8-byte alignment padding), clamped to the file end.
func recordLenAt(data []byte, off uint64) int64 {
	if int64(off)+hdrLen > int64(len(data)) {
		return int64(len(data)) - int64(off)
	}
	dataLen := uint64(0)
	for i := 0; i < 8; i++ {
		dataLen |= uint64(data[off+dataLenOffset+uint64(i)]) << (8 * i)
	}
	recLen := int64(hdrLen) + int64(util.AlignUp(dataLen, 8))
	if int64(off)+recLen > int64(len(data)) {
		return int64(len(data)) - int64(off)
	}
	return recLen
}

func removeSourceFiles(srcPath, manifestPath string) error {
	if err := os.Remove(srcPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if manifestPath != "" {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		// A secondary-directory data file's manifest lives in the primary.
		if filepath.Dir(manifestPath) != filepath.Dir(srcPath) {
			if err := fsyncDir(filepath.Dir(manifestPath)); err != nil {
				return err
			}
		}
	}
	return fsyncDir(filepath.Dir(srcPath))
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
