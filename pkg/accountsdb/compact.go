package accountsdb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/cockroachdb/pebble"
)

// Compaction reclaims dead bytes from the append-only store. Folds never
// overwrite: every re-written account leaves its previous version behind as
// dead bytes in an older segment or bootstrap appendvec. CompactOnce scans
// mostly-dead files and moves the still-live records into a fresh output file
// (1 source -> 1 output), then deletes the source.
//
// The pin rule (invariant I5) is what keeps rewind sound: a file is untouchable
// while it is inside the rewind horizon — either it IS an in-horizon fold
// segment, or an in-horizon fold's undo pointer (ManifestRecord.Prev) names it.
// Once the horizon ages past a batch, its undo targets stop being needed and
// the files become ordinary candidates. Bootstrap appendvecs are never pinned
// by seq (they predate all folds) and compact like any other file once their
// live fraction decays — that is where the bulk of long-run reclaim comes from.
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
	// RewindHorizonBatches pins the newest N fold batches and every file their
	// undo pointers name. Must match (or exceed) the operational rewind
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
}

// CompactStats reports one CompactOnce cycle.
type CompactStats struct {
	CandidatesScanned int
	FilesCompacted    int
	FilesDeleted      int // fully-dead fast path (no bytes moved)
	LiveBytesMoved    int64
	BytesReclaimed    int64 // source bytes freed (compacted + deleted)
}

const (
	defaultMinDeadFraction      = 0.7
	defaultMaxMoveBytesPerCycle = int64(256 << 20)
	defaultMaxScanBytesPerCycle = int64(1 << 30)
)

type compactCandidate struct {
	name         string
	slot         uint64
	fileId       uint64
	size         int64
	manifestPath string // the source's own manifest; "" for bootstrap appendvecs
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

	db.foldMu.Lock()
	defer db.foldMu.Unlock()

	meta, _, err := db.readFoldMeta()
	if err != nil {
		return stats, err
	}

	pinned, manifestByFileId, err := db.compactionPinSet(meta.BatchSeq, cfg.RewindHorizonBatches)
	if err != nil {
		return stats, err
	}

	bootstrapHigh := db.bootstrapHighFileId()
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return stats, err
	}
	candidates := make([]compactCandidate, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		slot, fileId, ok := parseDataFileName(name)
		if !ok {
			continue
		}
		if _, isPinned := pinned[fileId]; isPinned {
			continue
		}
		mpath, hasManifest := manifestByFileId[fileId]
		if !hasManifest && fileId > bootstrapHigh {
			continue // undecided orphan — recovery's to delete, not ours
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		candidates = append(candidates, compactCandidate{
			name: name, slot: slot, fileId: fileId, size: info.Size(), manifestPath: mpath,
		})
	}
	// Deterministic order + resume after the previous cycle's cursor so large
	// directories make steady progress instead of rescanning the same prefix.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].name < candidates[j].name })
	if db.compactCursor != "" {
		rotated := make([]compactCandidate, 0, len(candidates))
		var before []compactCandidate
		for _, c := range candidates {
			if c.name > db.compactCursor {
				rotated = append(rotated, c)
			} else {
				before = append(before, c)
			}
		}
		candidates = append(rotated, before...)
	}

	var scannedBytes int64
	for _, c := range candidates {
		if stats.LiveBytesMoved >= cfg.MaxMoveBytesPerCycle || scannedBytes >= cfg.MaxScanBytesPerCycle {
			break
		}
		db.compactCursor = c.name
		scannedBytes += c.size
		stats.CandidatesScanned++

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
	if stats.FilesCompacted > 0 || stats.FilesDeleted > 0 {
		mlog.Log.Infof("accountsdb: compaction cycle — %d scanned, %d compacted, %d deleted, %s live moved, %s reclaimed",
			stats.CandidatesScanned, stats.FilesCompacted, stats.FilesDeleted,
			humanBytes(stats.LiveBytesMoved), humanBytes(stats.BytesReclaimed))
	}
	return stats, nil
}

// compactionPinSet returns the fileIds compaction must not touch (I5) plus a
// fileId -> manifest-path map for every current (non-parked) manifest.
//
// Pinned: in-horizon fold segments (BatchSeq > head-horizon), every file an
// in-horizon undo pointer names, and — unconditionally — parked ".rewound"
// manifests' segments and undo targets (an interrupted rewind resumes through
// them at the next startup; compaction must not pull files out from under it).
func (db *AccountsDb) compactionPinSet(headSeq, horizon uint64) (map[uint64]struct{}, map[uint64]string, error) {
	horizonFloor := uint64(0)
	if headSeq > horizon {
		horizonFloor = headSeq - horizon
	}
	pinned := make(map[uint64]struct{})
	manifestByFileId := make(map[uint64]string)

	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		parked := strings.HasSuffix(name, segManifestRewoundSuffix)
		if !parked && !strings.HasSuffix(name, segManifestSuffix) {
			continue
		}
		path := filepath.Join(db.AcctsDir, name)
		hdr, herr := readManifestHeader(path)
		if herr != nil {
			continue // torn — recovery quarantines; its data file stays untouched (no manifest entry -> orphan rule)
		}
		if !parked {
			manifestByFileId[hdr.FileId] = path
		}
		if hdr.Kind != ManifestKindFold {
			continue
		}
		if parked || hdr.BatchSeq > horizonFloor {
			pinned[hdr.FileId] = struct{}{}
			m, merr := ReadSegmentManifest(path)
			if merr != nil {
				continue // segment itself stays pinned; undo targets unknown but recovery will quarantine this manifest
			}
			for i := range m.Records {
				if m.Records[i].PrevValid {
					pinned[m.Records[i].Prev.FileId] = struct{}{}
				}
			}
		}
	}
	return pinned, manifestByFileId, nil
}

// compactFile scans one candidate and, if dead enough, moves its live records
// to a fresh output file and unlinks the source. Returns (acted, liveBytesMoved).
// acted=false means the file was left alone (too live). liveBytesMoved==0 with
// acted=true means the fully-dead fast path (source deleted, nothing written).
func (db *AccountsDb) compactFile(c compactCandidate, minDeadFraction float64) (bool, int64, error) {
	srcPath := filepath.Join(db.AcctsDir, c.name)
	data, err := os.ReadFile(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
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
	liveIdx := make([]int, 0, len(idxEntries))
	var liveBytes int64
	for i := range idxEntries {
		cur, closer, gerr := db.Index.Get(pubkeys[i][:])
		if gerr != nil {
			if gerr == pebble.ErrNotFound {
				continue
			}
			return false, 0, gerr
		}
		curEntry, derr := UnmarshalAcctIdxEntry(cur)
		closer.Close()
		if derr != nil {
			return false, 0, derr
		}
		if curEntry.FileId == c.fileId && curEntry.Offset == idxEntries[i].Offset && curEntry.Slot == c.slot {
			liveIdx = append(liveIdx, i)
			liveBytes += recordLenAt(data, idxEntries[i].Offset)
		}
	}

	if len(data) == 0 || len(liveIdx) == 0 {
		// Fully dead (or empty bankhash-only segment past the horizon): no
		// index state references it; drop source + its manifest.
		if err := removeSourceFiles(db.AcctsDir, srcPath, c.manifestPath); err != nil {
			return false, 0, err
		}
		return true, 0, nil
	}

	deadFraction := 1.0 - float64(liveBytes)/float64(len(data))
	if deadFraction < minDeadFraction {
		return false, 0, nil
	}

	// Allocate the output fileId, persisting the high-water mark before the
	// first data byte (I7 — a crash must never lead to fileId reuse).
	newFileId := db.LargestFileId.Add(1)
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
	outPath := filepath.Join(db.AcctsDir, outName)
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
	if err := fsyncDir(db.AcctsDir); err != nil {
		return false, 0, err
	}

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

	// Move the index entries in one batch, then make the move durable BEFORE
	// unlinking the source — with the WAL off that means an explicit Flush,
	// because compact manifests are not replayed at recovery.
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

	if err := removeSourceFiles(db.AcctsDir, srcPath, c.manifestPath); err != nil {
		return false, 0, err
	}
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

func removeSourceFiles(acctsDir, srcPath, manifestPath string) error {
	if err := os.Remove(srcPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if manifestPath != "" {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return fsyncDir(acctsDir)
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
