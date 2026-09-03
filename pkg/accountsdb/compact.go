package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
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

// CompactionConfig controls one CompactOnce cycle. The byte settings are soft
// targets checked between source files, not hard caps: a cycle may exceed each
// target by at most the work attributable to one selected source file.
type CompactionConfig struct {
	// RewindHorizonBatches preserves the newest N fold transitions plus the
	// boundary manifest/file immediately before them, and pins every file the N
	// transitions' undo pointers name. Must match (or exceed) the operational
	// rewind horizon. Clamped to >= 1 so the committed head fold is always pinned.
	RewindHorizonBatches uint64
	// MinDeadFraction is the dead-byte fraction a file must reach before its
	// live records are moved. Defaults to 0.7.
	MinDeadFraction float64
	// MaxMoveBytesPerCycle is the soft target for live bytes rewritten per call.
	// The target is checked between source files, so one source's live bytes may
	// overshoot it. Deleting fully-dead files is free and not counted. Default
	// 256MB.
	MaxMoveBytesPerCycle int64
	// MaxScanBytesPerCycle is the soft target for bytes read by liveness scans.
	// The target is checked between source files, so one source's full size may
	// overshoot it. Mostly-live files cost a scan but yield no move. Progress
	// across cycles is kept by a directory cursor. Default 1GB.
	MaxScanBytesPerCycle int64
	// MaxSourceBytes is a hard admission cap checked before opening or scanning a
	// source. It bounds one routine cycle's uninterrupted per-file work. Zero is
	// unlimited and is reserved for explicit callers and emergency disk-pressure
	// compaction, where bounded replay backpressure is safer than ENOSPC.
	MaxSourceBytes int64
	// NonBlocking makes the cycle return ErrCompactionBusy instead of waiting
	// for an active fold/recovery/rewind. Background maintenance uses this so it
	// never queues ahead of replay's durable fold. Emergency disk-pressure
	// admission deliberately leaves it false and applies bounded backpressure.
	NonBlocking bool
	// MinOutputFreeBytes is filesystem headroom that must remain in addition to
	// a selected source's exact live-byte output. The check happens after the
	// liveness pass but before file-ID allocation or output creation.
	MinOutputFreeBytes uint64
	// ContinueOnOutputSpacePressure lets an emergency pass defer a candidate
	// whose live output will not fit and continue looking for fully-dead (or
	// smaller) files that can still reclaim space. If the pass makes no
	// reclamation progress, the strongest deferred disk-pressure error is
	// returned. Callers must opt in explicitly so unexpected pressure is never
	// silently hidden by a direct CompactOnce invocation.
	ContinueOnOutputSpacePressure bool
}

// CompactStats reports one CompactOnce cycle.
type CompactStats struct {
	EligibleCandidates int
	CandidatesScanned  int
	FilesCompacted     int
	FilesDeleted       int // fully-dead fast path (no bytes moved)
	// OutputSpaceDeferrals counts otherwise-eligible source files skipped because
	// their live output plus the configured scratch reserve did not fit.
	OutputSpaceDeferrals int
	// SourceSizeDeferrals counts files rejected before their liveness scan by
	// MaxSourceBytes. MaxDeferredSourceBytes makes the largest such file visible.
	SourceSizeDeferrals    int
	MaxDeferredSourceBytes int64
	ScannedBytes           int64
	// MaxSourceBytes is the largest source admitted during this cycle. It makes
	// the possible one-source overshoot of the soft byte targets observable.
	MaxSourceBytes int64
	LiveBytesMoved int64
	BytesReclaimed int64 // source bytes freed (compacted + deleted)
	// PassComplete is true when this invocation examined every candidate that
	// was eligible at its start. False means a soft byte target stopped it and
	// the persistent cursor will resume the pass on the next call.
	PassComplete bool
}

var (
	ErrCompactionBusy        = errors.New("accountsdb: appendvec compaction busy")
	ErrAppendVecDiskPressure = errors.New("accountsdb: appendvec disk pressure")
)

// AppendVecDiskPressureError reports a fail-closed admission or compaction
// refusal before the next durable allocation. It is safe for errors.Is with
// ErrAppendVecDiskPressure.
type AppendVecDiskPressureError struct {
	Operation      string
	AvailableBytes uint64
	RequiredBytes  uint64
}

func (err *AppendVecDiskPressureError) Error() string {
	if err == nil {
		return ErrAppendVecDiskPressure.Error()
	}
	return fmt.Sprintf(
		"%s: %s has %d available bytes, requires %d bytes",
		ErrAppendVecDiskPressure,
		err.Operation,
		err.AvailableBytes,
		err.RequiredBytes,
	)
}

func (*AppendVecDiskPressureError) Unwrap() error { return ErrAppendVecDiskPressure }

const (
	defaultMinDeadFraction      = 0.7
	defaultMaxMoveBytesPerCycle = int64(256 << 20)
	defaultMaxScanBytesPerCycle = int64(1 << 30)
)

type compactCandidate struct {
	name               string
	slot               uint64
	fileId             uint64
	size               int64
	manifestPath       string // the source's own manifest; "" for bootstrap appendvecs
	retirementRequired bool   // an immutable generation may still return this path
}

// CompactOnce runs one bounded compaction cycle and returns what it did.
func (db *AccountsDb) CompactOnce(cfg CompactionConfig) (CompactStats, error) {
	return db.CompactOnceContext(context.Background(), cfg)
}

// CompactOnceContext is CompactOnce with cancellation between records and
// bounded copy chunks. Cancellation never weakens crash safety: a published
// output is retained, while the source is unlinked only after every relocation
// and its retirement marker are durable.
func (db *AccountsDb) CompactOnceContext(ctx context.Context, cfg CompactionConfig) (CompactStats, error) {
	stats := CompactStats{}
	if ctx == nil {
		return stats, errors.New("accountsdb: nil compaction context")
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if !db.RootedDurable {
		return stats, fmt.Errorf("accountsdb: compaction requires rooted-durable mode (the direct store path writes the index outside foldMu)")
	}
	if db.ProductionIndex == nil && db.Index == nil {
		return stats, errors.New("accountsdb: compaction requires a durable mutable account index")
	}
	if cfg.RewindHorizonBatches == 0 {
		cfg.RewindHorizonBatches = 1
	}
	if cfg.MinDeadFraction <= 0 {
		cfg.MinDeadFraction = defaultMinDeadFraction
	}
	if math.IsNaN(cfg.MinDeadFraction) || cfg.MinDeadFraction > 1 {
		return stats, fmt.Errorf("accountsdb: invalid compaction minimum dead fraction %v", cfg.MinDeadFraction)
	}
	if cfg.MaxMoveBytesPerCycle <= 0 {
		cfg.MaxMoveBytesPerCycle = defaultMaxMoveBytesPerCycle
	}
	if cfg.MaxScanBytesPerCycle <= 0 {
		cfg.MaxScanBytesPerCycle = defaultMaxScanBytesPerCycle
	}

	if cfg.NonBlocking {
		if !db.foldMu.TryLock() {
			return stats, ErrCompactionBusy
		}
	} else {
		db.foldMu.Lock()
	}
	defer db.foldMu.Unlock()
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	meta, _, err := db.readFoldMeta()
	if err != nil {
		return stats, err
	}

	pinned, manifestByFileId, err := db.compactionPinSet(ctx, meta.BatchSeq, cfg.RewindHorizonBatches)
	if err != nil {
		return stats, err
	}

	bootstrapHigh, err := db.bootstrapHighFileId()
	if err != nil {
		return stats, err
	}
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return stats, err
	}
	candidates := make([]compactCandidate, 0, len(entries))
	for entryOrdinal, e := range entries {
		if entryOrdinal&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
		}
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
			return stats, fmt.Errorf("accountsdb: inspect compaction candidate %s: %w", name, ierr)
		}
		if !info.Mode().IsRegular() {
			return stats, fmt.Errorf("accountsdb: compaction candidate %s is not a regular file", name)
		}
		candidates = append(candidates, compactCandidate{
			name: name, slot: slot, fileId: fileId, size: info.Size(), manifestPath: mpath,
			// FileId order only distinguishes bootstrap data from undecided
			// orphans; it does not prove which files a rolling immutable index
			// generation references. Retirement is cheap and idempotent, so record
			// it for every removed path. Generation GC may discard the marker once
			// no published or pinned base can name the path.
			retirementRequired: db.ProductionIndex != nil || db.Index != nil,
		})
	}
	stats.EligibleCandidates = len(candidates)
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

	var deferredOutputPressure error
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if stats.LiveBytesMoved >= cfg.MaxMoveBytesPerCycle || stats.ScannedBytes >= cfg.MaxScanBytesPerCycle {
			break
		}
		db.compactCursor = c.name
		if cfg.MaxSourceBytes > 0 && c.size > cfg.MaxSourceBytes {
			stats.SourceSizeDeferrals++
			stats.MaxDeferredSourceBytes = max(stats.MaxDeferredSourceBytes, c.size)
			continue
		}
		stats.ScannedBytes += c.size
		stats.MaxSourceBytes = max(stats.MaxSourceBytes, c.size)
		stats.CandidatesScanned++

		acted, moved, err := db.compactFile(ctx, c, cfg.MinDeadFraction, cfg.MinOutputFreeBytes)
		if err != nil {
			if cfg.ContinueOnOutputSpacePressure && errors.Is(err, ErrAppendVecDiskPressure) {
				stats.OutputSpaceDeferrals++
				if deferredOutputPressure == nil {
					deferredOutputPressure = fmt.Errorf("accountsdb: compact %s: %w", c.name, err)
				}
				continue
			}
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
	stats.PassComplete = stats.CandidatesScanned+stats.SourceSizeDeferrals == stats.EligibleCandidates
	if deferredOutputPressure != nil && stats.BytesReclaimed == 0 {
		return stats, deferredOutputPressure
	}
	if stats.FilesCompacted > 0 || stats.FilesDeleted > 0 {
		mlog.Log.Infof("accountsdb: compaction cycle — %d files/%s scanned (largest source %s), %d compacted, %d deleted, %s live moved, %s reclaimed",
			stats.CandidatesScanned, humanBytes(stats.ScannedBytes), humanBytes(stats.MaxSourceBytes),
			stats.FilesCompacted, stats.FilesDeleted,
			humanBytes(stats.LiveBytesMoved), humanBytes(stats.BytesReclaimed))
	}
	return stats, nil
}

// compactionPinSet returns the fileIds compaction must not touch (I5) plus a
// fileId -> manifest-path map for every current (non-parked) manifest.
//
// Pinned: the newest horizon fold segments and the boundary segment immediately
// before them (BatchSeq >= head-horizon), every file an actually undoable suffix
// manifest (BatchSeq > head-horizon) names, and — unconditionally — parked
// ".rewound" manifests' segments and undo targets (an interrupted rewind resumes
// through them at the next startup; compaction must not pull files out from
// under it). The extra boundary segment is required because rewinding N batches
// targets that manifest; its own undo pointers belong to the N+1 transition and
// need not remain pinned.
func (db *AccountsDb) compactionPinSet(ctx context.Context, headSeq, horizon uint64) (map[uint64]struct{}, map[uint64]string, error) {
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
	for entryOrdinal, e := range entries {
		if entryOrdinal&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
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
			// A published manifest is part of the rewind/compaction safety
			// metadata.  If its header cannot be decoded, we cannot know its
			// output file or (for a fold) which older appendvecs its undo
			// pointers pin.  Continuing could therefore reclaim data needed by
			// recovery or rewind.  Only .tmp files are undecided and were
			// filtered above; malformed final manifests fail closed.
			return nil, nil, fmt.Errorf(
				"accountsdb: compaction: published manifest %s is not readable: %w",
				path,
				herr,
			)
		}
		if !parked {
			if previous, duplicate := manifestByFileId[hdr.FileId]; duplicate {
				return nil, nil, fmt.Errorf(
					"accountsdb: compaction: duplicate published manifest file ID %d: %s and %s",
					hdr.FileId,
					previous,
					path,
				)
			}
			manifestByFileId[hdr.FileId] = path
		}
		if hdr.Kind != ManifestKindFold {
			continue
		}
		pinOutput := parked || hdr.BatchSeq >= horizonFloor
		pinUndoTargets := parked || hdr.BatchSeq > horizonFloor
		if pinOutput {
			pinned[hdr.FileId] = struct{}{}
			m, merr := ReadSegmentManifest(path)
			if merr != nil {
				return nil, nil, fmt.Errorf(
					"accountsdb: compaction: in-horizon manifest %s is not fully CRC-valid: %w",
					path,
					merr,
				)
			}
			if err := validateManifestHeaderIdentity(hdr, m); err != nil {
				return nil, nil, fmt.Errorf(
					"accountsdb: compaction: in-horizon manifest %s changed while reading: %w",
					path,
					err,
				)
			}
			if err := validateFoldManifestIdentity(m, path, parked); err != nil {
				return nil, nil, fmt.Errorf(
					"accountsdb: compaction: invalid in-horizon fold manifest %s: %w",
					path,
					err,
				)
			}
			if pinUndoTargets {
				for i := range m.Records {
					if m.Records[i].PrevValid {
						pinned[m.Records[i].Prev.FileId] = struct{}{}
					}
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
func (db *AccountsDb) compactFile(
	ctx context.Context,
	c compactCandidate,
	minDeadFraction float64,
	minOutputFreeBytes uint64,
) (bool, int64, error) {
	srcPath := filepath.Join(db.AcctsDir, c.name)
	src, openedInfo, err := openStableRegularFile(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil // raced an external cleanup; nothing to do
		}
		return false, 0, err
	}
	defer src.Close()
	if openedInfo.Size() < 0 {
		return false, 0, fmt.Errorf("source is not a regular appendvec")
	}
	sourceSize := openedInfo.Size()

	// A previously durable retirement can be left behind by a crash before
	// unlink. Such a path is no longer authoritative for immutable-base hits;
	// exact mutable mappings (if any) still count as live defensively.
	alreadyRetired := false
	if c.retirementRequired {
		alreadyRetired, err = db.isAppendVecRetired(c.slot, c.fileId)
		if err != nil {
			return false, 0, err
		}
	}

	// Exact liveness: the index still names this (fileId, offset). The entry's
	// slot must also equal the filename slot — the read path builds the file
	// name from the entry's slot, so a mismatched entry could not be served
	// from the output file and is safest left untouched.
	var liveBytes uint64
	var liveRecords uint64
	isLive := func(rec appendVecScanRecord) (bool, error) {
		// The source scan supplies the full key, and the complete tuple check is
		// sufficient to reject a StreamHash false candidate without reading data.
		curEntry, source, found, lookupErr := db.lookupAccountIndexCandidate(rec.Pubkey)
		if lookupErr != nil {
			return false, lookupErr
		}
		if !found || curEntry.FileId != c.fileId || curEntry.Offset != rec.Offset || curEntry.Slot != c.slot {
			return false, nil
		}
		if alreadyRetired && source == accountIndexSourceBase {
			return false, nil
		}
		return true, nil
	}
	if err := scanAppendVecRecords(ctx, src, sourceSize, func(rec appendVecScanRecord) error {
		live, lookupErr := isLive(rec)
		if lookupErr != nil {
			return lookupErr
		}
		if !live {
			return nil
		}
		if liveBytes > math.MaxUint64-rec.Span {
			return errors.New("live appendvec byte count overflows uint64")
		}
		liveBytes += rec.Span
		if liveRecords == math.MaxUint64 {
			return errors.New("live appendvec record count overflows uint64")
		}
		liveRecords++
		return nil
	}); err != nil {
		return false, 0, err
	}
	// The accounts directory is process-private in normal operation, but fail
	// closed if it was accidentally or maliciously rewritten while this long
	// liveness scan was in flight. In particular, never retire or unlink a path
	// that has become a symlink or now names a different inode.
	fire(db.foldHooks.afterCompactionSourceScan)
	if err := validateStableRegularFile(src, srcPath, openedInfo); err != nil {
		return false, 0, fmt.Errorf("validate compaction source after liveness scan: %w", err)
	}

	if sourceSize == 0 || liveRecords == 0 {
		// Fully dead (or empty bankhash-only segment past the horizon): no
		// index state references it. Durably retire the path before unlinking so
		// immutable-base false positives remain distinguishable from data loss.
		if err := ctx.Err(); err != nil {
			return false, 0, err
		}
		if c.retirementRequired && !alreadyRetired {
			err = db.markAppendVecRetired(c.slot, c.fileId)
		}
		if err != nil {
			return false, 0, err
		}
		if err := ctx.Err(); err != nil {
			return false, 0, err
		}
		db.appendVecReadMu.Lock()
		err = validateStableRegularFile(src, srcPath, openedInfo)
		if err == nil {
			err = removeSourceFiles(db.AcctsDir, srcPath, c.manifestPath)
		}
		db.appendVecReadMu.Unlock()
		if err != nil {
			return false, 0, err
		}
		return true, 0, nil
	}

	if liveBytes > uint64(sourceSize) {
		return false, 0, fmt.Errorf("live byte count %d exceeds source size %d", liveBytes, sourceSize)
	}
	deadFraction := 1.0 - float64(liveBytes)/float64(sourceSize)
	if deadFraction < minDeadFraction {
		return false, 0, nil
	}
	if minOutputFreeBytes > 0 {
		space, spaceErr := db.appendVecFilesystemSpace()
		if spaceErr != nil {
			return false, 0, spaceErr
		}
		required := saturatingAddUint64(liveBytes, compactionRelocationWALBytes(liveRecords))
		required = saturatingAddUint64(required, db.compactionRewriteAndMetadataHeadroomBytes())
		required = saturatingAddUint64(required, minOutputFreeBytes)
		if space.AvailableBytes < required {
			return false, 0, &AppendVecDiskPressureError{
				Operation:      "compaction output preflight",
				AvailableBytes: space.AvailableBytes,
				RequiredBytes:  required,
			}
		}
	}

	// Allocate the output fileId, persisting the high-water mark before the
	// first data byte (I7 — a crash must never lead to fileId reuse).
	newFileId, err := db.allocateFileID()
	if err != nil {
		return false, 0, err
	}

	outName := SegmentDataName(c.slot, newFileId)
	outPath := filepath.Join(db.AcctsDir, outName)
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return false, 0, err
	}
	outputPublished := false
	defer func() {
		if outputPublished {
			return
		}
		_ = f.Close()
		if removeErr := os.Remove(outPath); removeErr == nil {
			_ = fsyncDir(db.AcctsDir)
		}
	}()

	// Pass two raw-copies only live spans. Account data is never materialized;
	// the fixed copy buffer bounds memory independently of appendvec size.
	copyBuf := make([]byte, compactCopyBufferBytes)
	dataCRC := crc32.NewIEEE()
	var outputBytes uint64
	var outputRecords uint64
	if err := scanAppendVecRecords(ctx, src, sourceSize, func(rec appendVecScanRecord) error {
		live, lookupErr := isLive(rec)
		if lookupErr != nil {
			return lookupErr
		}
		if !live {
			return nil
		}
		if outputBytes > math.MaxUint64-rec.Span {
			return errors.New("compaction output length overflows uint64")
		}
		if err := copyAppendVecSpan(ctx, f, dataCRC, src, rec.Offset, rec.Span, copyBuf); err != nil {
			return err
		}
		outputBytes += rec.Span
		outputRecords++
		return nil
	}); err != nil {
		return false, 0, err
	}
	if outputBytes != liveBytes || outputRecords != liveRecords {
		return false, 0, fmt.Errorf(
			"liveness changed while compacting: first pass=%d records/%d bytes, copy pass=%d records/%d bytes",
			liveRecords, liveBytes, outputRecords, outputBytes,
		)
	}
	if err := validateStableRegularFile(src, srcPath, openedInfo); err != nil {
		return false, 0, fmt.Errorf("validate compaction source after copy: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	if err := f.Sync(); err != nil {
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
		DataLen:     outputBytes,
		DataCRC:     dataCRC.Sum32(),
		// Compact manifests are non-orphan certificates, not redo logs. Keeping
		// relocation records out of them avoids retaining O(file records) RAM;
		// the durable mutable-index journal below remains the source of truth.
		Records: nil,
	}
	// WriteSegmentManifest may report a directory-fsync failure after its
	// rename has already made the manifest visible. From this point onward keep
	// the data file on every error; an unmanifested copy is an ordinary orphan,
	// while a published manifest must never name a file we cleaned up.
	outputPublished = true
	if err := WriteSegmentManifest(db.AcctsDir, manifest); err != nil {
		return false, 0, err
	}
	fire(db.foldHooks.afterManifestRename)

	// Relocation frames are deliberately bounded. Between frames both files
	// exist, so readers can safely observe either mapping; an interrupted run is
	// resumed as ordinary dead-space cleanup. The source is retired and removed
	// only after all relocation frames have reached durable storage.
	if err := db.publishCompactionRelocations(ctx, outPath, int64(outputBytes), c.slot, newFileId); err != nil {
		return false, 0, err
	}
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}

	// Durably retire the old path without holding up readers on the journal
	// fsync. Both copies still exist at this point and every true member has an
	// exact relocation above the immutable base.
	if c.retirementRequired && !alreadyRetired {
		if err := db.markAppendVecRetired(c.slot, c.fileId); err != nil {
			return false, 0, err
		}
	}
	fire(db.foldHooks.afterIndexCommit)
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}

	// Wait only for readers that captured an old mapping, then unlink. New
	// readers already resolve every live key to the output.
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()
	if err := validateStableRegularFile(src, srcPath, openedInfo); err != nil {
		return false, 0, fmt.Errorf("validate compaction source before unlink: %w", err)
	}
	if err := removeSourceFiles(db.AcctsDir, srcPath, c.manifestPath); err != nil {
		return false, 0, err
	}
	if outputBytes > math.MaxInt64 {
		return false, 0, fmt.Errorf("compaction output length %d overflows int64", outputBytes)
	}
	return true, int64(outputBytes), nil
}

func (db *AccountsDb) appendVecFilesystemSpace() (AppendVecFilesystemSpace, error) {
	probe := db.diskSpaceProbe
	if probe == nil {
		probe = ReadAppendVecFilesystemSpace
	}
	return probe(db.AcctsDir)
}

const (
	compactCopyBufferBytes               = 256 << 10
	compactRelocationChunkKeys           = 64 << 10
	compactionFixedMetadataHeadroomBytes = uint64(64 << 20)
	compactionRetirementWALMutations     = uint64(1)
)

// compactionRelocationWALBytes is the exact journal growth of the bounded
// relocation frames plus one conservative retirement-marker frame. Every live
// record produces one fixed-size mutation; each chunk has its own frame header.
// Saturation is intentional: an unrepresentable requirement must fail closed.
func compactionRelocationWALBytes(liveRecords uint64) uint64 {
	frames := liveRecords / compactRelocationChunkKeys
	if liveRecords%compactRelocationChunkKeys != 0 {
		frames++
	}
	payload := saturatingMultiplyUint64(liveRecords, deltaMutationSize)
	headers := saturatingMultiplyUint64(frames, deltaFrameHeaderSize)
	retirement := saturatingAddUint64(
		deltaFrameHeaderSize,
		saturatingMultiplyUint64(compactionRetirementWALMutations, deltaMutationSize),
	)
	return saturatingAddUint64(saturatingAddUint64(payload, headers), retirement)
}

// compactionRewriteAndMetadataHeadroomBytes covers the selector/manifest and
// filesystem metadata plus the largest temporary exact-state WAL rewrite the
// configured production mutable index can create while relocation frames are
// being published. The old journal already occupies disk; this is the bounded
// additional replacement file. Saturation again forces a safe refusal.
func (db *AccountsDb) compactionRewriteAndMetadataHeadroomBytes() uint64 {
	headroom := compactionFixedMetadataHeadroomBytes
	if db == nil || db.ProductionIndex == nil || db.ProductionIndex.mutable == nil {
		return headroom
	}
	mutable := db.ProductionIndex.mutable
	minimumCharge := min(mutable.config.BytesPerKey, mutable.config.BytesPerRetired)
	if minimumCharge == 0 {
		return math.MaxUint64
	}
	maxMutations := mutable.config.MaxHotBytes / minimumCharge
	maxFrameMutations := mutable.maxFrameMutations()
	if maxMutations != 0 && maxFrameMutations == 0 {
		return math.MaxUint64
	}
	frames := uint64(0)
	if maxMutations != 0 {
		frames = maxMutations / maxFrameMutations
		if maxMutations%maxFrameMutations != 0 {
			frames++
		}
	}
	rewriteBytes := saturatingAddUint64(
		deltaJournalHeaderSize,
		saturatingMultiplyUint64(maxMutations, deltaMutationSize),
	)
	rewriteBytes = saturatingAddUint64(
		rewriteBytes,
		saturatingMultiplyUint64(frames, deltaFrameHeaderSize),
	)
	return saturatingAddUint64(headroom, rewriteBytes)
}

type appendVecScanRecord struct {
	Pubkey solana.PublicKey
	Offset uint64
	Span   uint64
}

// scanAppendVecRecords performs the hardened appendvec structural scan using
// one fixed header. It intentionally skips account bodies: liveness needs only
// the key and record span. A final record may omit up to seven padding bytes,
// matching snapshot appendvec parsing.
func scanAppendVecRecords(
	ctx context.Context,
	r io.ReaderAt,
	fileSize int64,
	visit func(appendVecScanRecord) error,
) error {
	if ctx == nil {
		return errors.New("nil appendvec scan context")
	}
	if r == nil {
		return errors.New("nil appendvec reader")
	}
	if visit == nil {
		return errors.New("nil appendvec scan visitor")
	}
	if fileSize < 0 {
		return fmt.Errorf("negative appendvec size %d", fileSize)
	}
	limit := uint64(fileSize)
	var offset uint64
	var header [hdrLen]byte
	for offset < limit {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := limit - offset
		if remaining < hdrLen {
			tail := header[:int(remaining)]
			clear(tail)
			if err := readAppendVecAt(r, tail, offset); err != nil {
				return fmt.Errorf("read appendvec tail at %d: %w", offset, err)
			}
			for _, b := range tail {
				if b != 0 {
					return fmt.Errorf("truncated appendvec header at offset %d: have %d bytes, need %d", offset, remaining, hdrLen)
				}
			}
			return nil
		}
		if err := readAppendVecAt(r, header[:], offset); err != nil {
			return fmt.Errorf("read appendvec header at %d: %w", offset, err)
		}
		dataLen := binary.LittleEndian.Uint64(header[dataLenOffset : dataLenOffset+8])
		pubkey := solana.PublicKeyFromBytes(header[pubkeyOffset : pubkeyOffset+32])
		lamports := binary.LittleEndian.Uint64(header[lamportsOffset : lamportsOffset+8])
		if pubkey == (solana.PublicKey{}) && lamports == 0 {
			return nil
		}
		if dataLen > maxAppendVecAccountDataLen {
			return fmt.Errorf(
				"appendvec account data length %d exceeds maximum %d at offset %d",
				dataLen, maxAppendVecAccountDataLen, offset,
			)
		}
		dataOffset := offset + hdrLen
		if dataLen > limit-dataOffset {
			return fmt.Errorf(
				"truncated appendvec account data at offset %d: data length %d exceeds %d available bytes",
				offset, dataLen, limit-dataOffset,
			)
		}
		if dataLen > math.MaxUint64-7 {
			return fmt.Errorf("appendvec account data length overflows alignment at offset %d: %d", offset, dataLen)
		}
		alignedDataLen := (dataLen + 7) &^ uint64(7)
		if dataOffset > math.MaxUint64-alignedDataLen {
			return fmt.Errorf("appendvec record end overflows at offset %d", offset)
		}
		recordEnd := dataOffset + alignedDataLen
		actualEnd := recordEnd
		if actualEnd > limit {
			actualEnd = limit
		}
		if actualEnd <= offset {
			return fmt.Errorf("appendvec record at offset %d has invalid end %d", offset, actualEnd)
		}
		if err := visit(appendVecScanRecord{Pubkey: pubkey, Offset: offset, Span: actualEnd - offset}); err != nil {
			return err
		}
		offset = recordEnd
	}
	return nil
}

func readAppendVecAt(r io.ReaderAt, dst []byte, offset uint64) error {
	if offset > math.MaxInt64 || uint64(len(dst)) > uint64(math.MaxInt64)-offset {
		return fmt.Errorf("appendvec read range %d+%d overflows int64", offset, len(dst))
	}
	n, err := r.ReadAt(dst, int64(offset))
	if n == len(dst) {
		return nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func copyAppendVecSpan(
	ctx context.Context,
	dst io.Writer,
	crc io.Writer,
	src io.ReaderAt,
	offset uint64,
	length uint64,
	buf []byte,
) error {
	if len(buf) == 0 {
		return errors.New("empty compaction copy buffer")
	}
	for copied := uint64(0); copied < length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := min(uint64(len(buf)), length-copied)
		if copied > math.MaxUint64-offset {
			return errors.New("appendvec copy offset overflows uint64")
		}
		if err := readAppendVecAt(src, buf[:int(chunk)], offset+copied); err != nil {
			return fmt.Errorf("read record span at %d: %w", offset+copied, err)
		}
		if err := writeCompactionBytes(dst, buf[:int(chunk)]); err != nil {
			return fmt.Errorf("write compacted record: %w", err)
		}
		if err := writeCompactionBytes(crc, buf[:int(chunk)]); err != nil {
			return fmt.Errorf("checksum compacted record: %w", err)
		}
		copied += chunk
	}
	return nil
}

func writeCompactionBytes(dst io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := dst.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid write count %d for %d bytes", n, len(data))
		}
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (db *AccountsDb) publishCompactionRelocations(
	ctx context.Context,
	outPath string,
	outputSize int64,
	slot uint64,
	fileID uint64,
) error {
	out, err := os.Open(outPath)
	if err != nil {
		return fmt.Errorf("open compaction output for publication: %w", err)
	}
	defer out.Close()
	mutations := make([]deltaIndexMutation, 0, compactRelocationChunkKeys)
	flush := func() error {
		if len(mutations) == 0 {
			return nil
		}
		if err := db.applyAccountIndexMutations(mutations, nil); err != nil {
			return err
		}
		mutations = mutations[:0]
		return nil
	}
	if err := scanAppendVecRecords(ctx, out, outputSize, func(rec appendVecScanRecord) error {
		entry := AccountIndexEntry{Slot: slot, FileId: fileID, Offset: rec.Offset}
		mutations = append(mutations, liveDeltaMutation(rec.Pubkey, entry))
		if len(mutations) == cap(mutations) {
			return flush()
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan compaction output for index publication: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("publish compaction index relocations: %w", err)
	}
	return nil
}

func removeSourceFiles(acctsDir, srcPath, manifestPath string) error {
	// Remove and durably forget the old commit record first. If power is lost
	// here, its still-present data file is merely an orphan; the already-durable
	// index points at the compaction output. Removing data first could leave a
	// durable fold manifest naming a missing segment, causing recovery to mistake
	// old compacted history for a torn canonical batch.
	if manifestPath != "" {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := fsyncDir(acctsDir); err != nil {
			return err
		}
	}
	if err := os.Remove(srcPath); err != nil && !os.IsNotExist(err) {
		return err
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
