package accountsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/cockroachdb/pebble"
)

// RecoveryResult reports the durable fold state derived from the store itself
// (manifests + index meta), independent of the state file — which is only
// written on graceful shutdown and may be stale after a hard crash.
type RecoveryResult struct {
	// DurableThrough is R: the highest slot whose fold is durably committed.
	// Zero with BatchSeq zero means a fresh (never-folded) store.
	DurableThrough uint64
	BatchSeq       uint64
	// RootedBankhash is the bankhash at DurableThrough (from its manifest);
	// zero when no fold manifest covers R (fresh store).
	RootedBankhash [32]byte
	// ResumeCtx is the opaque serialized resume context at DurableThrough
	// carried by its manifest; nil on a fresh store.
	ResumeCtx []byte
	// ReplayedBatches lists BatchSeqs whose index flip was completed during
	// recovery (crash matrix C3/C4': manifest durable, index tail lost).
	ReplayedBatches []uint64
	// OrphansRemoved lists files deleted as undecided leftovers (segments
	// without a durable manifest and pre-rename tmp manifests).
	OrphansRemoved []string
	// RewindInProgress is true when parked ".rewound" manifests remain — a
	// RewindToBatchBoundary was interrupted. The store may already sit at the
	// rewound boundary (index meta rolled back) while the state file still names
	// the pre-rewind slot; the caller adopts the store's boundary rather than
	// treating the intended rewind as data loss.
	RewindInProgress bool
}

// RecoverFoldState brings the index to the durable fold frontier:
//
//  1. read the fold meta (index watermark)
//  2. fully validate every final manifest before any mutation or cleanup
//  3. REPLAY the complete contiguous BatchSeq run above the watermark into the
//     index (verifying segment length + CRC first); a gap, invalid slot chain,
//     or corrupt final manifest is fatal and preserves every artifact
//  4. orphan-GC data files newer than the bootstrap high-water mark that have
//     no manifest (crash before the manifest rename: matrix C1/C2)
//
// It returns the recovered watermark plus the manifest-carried bankhash and
// resume context at R, which the caller reconciles against the state file.
func (db *AccountsDb) RecoverFoldState() (RecoveryResult, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()

	res := RecoveryResult{}

	meta, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return res, err
	}
	// A final manifest is the durable decision record. Build the orphan-GC
	// allow-list only after every final (ordinary and parked) manifest has passed
	// full CRC, structural, and canonical-identity validation. This happens
	// before replay as well as cleanup so corruption cannot cause partial startup
	// progress followed by destructive orphan classification.
	referencedFileIDs, err := listAllManifestFileIds(db.AcctsDir)
	if err != nil {
		return res, err
	}

	headers, err := ListFoldManifests(db.AcctsDir)
	if err != nil {
		return res, err
	}
	bySeq := make(map[uint64]ManifestHeader, len(headers))
	bySlot := make(map[uint64]ManifestHeader, len(headers))
	var maxSeq uint64
	for _, h := range headers {
		if prior, duplicate := bySeq[h.BatchSeq]; duplicate {
			return res, fmt.Errorf(
				"accountsdb: duplicate fold manifest sequence %d: %s and %s",
				h.BatchSeq, prior.Path, h.Path,
			)
		}
		if prior, duplicate := bySlot[h.ThroughSlot]; duplicate {
			return res, fmt.Errorf(
				"accountsdb: duplicate fold manifest through slot %d: %s and %s",
				h.ThroughSlot, prior.Path, h.Path,
			)
		}
		bySeq[h.BatchSeq] = h
		bySlot[h.ThroughSlot] = h
		if h.BatchSeq > maxSeq {
			maxSeq = h.BatchSeq
		}
	}

	// The applied frontier: meta if present, else fresh (seq 0).
	frontier := foldMeta{}
	if haveMeta {
		frontier = meta
	}
	var frontierManifest *SegmentManifest
	var bankhashRepairs []SlotBankhash
	if haveMeta && meta.BatchSeq != 0 {
		metaPath := segmentManifestPath(db.AcctsDir, meta.ThroughSlot, meta.FileId)
		_, statErr := os.Lstat(metaPath)
		switch {
		case statErr == nil:
			manifest, readErr := ReadSegmentManifest(metaPath)
			if readErr != nil {
				return res, fmt.Errorf(
					"accountsdb: committed fold manifest seq %d is not fully CRC-valid: %w",
					meta.BatchSeq, readErr,
				)
			}
			if identityErr := validateFoldManifestIdentity(manifest, metaPath, false); identityErr != nil {
				return res, fmt.Errorf("accountsdb: committed fold manifest identity: %w", identityErr)
			}
			if manifest.BatchSeq != meta.BatchSeq ||
				manifest.ThroughSlot != meta.ThroughSlot || manifest.FileId != meta.FileId {
				return res, fmt.Errorf(
					"accountsdb: committed fold manifest identity seq=%d slot=%d file=%d does not match authoritative meta seq=%d slot=%d file=%d",
					manifest.BatchSeq, manifest.ThroughSlot, manifest.FileId,
					meta.BatchSeq, meta.ThroughSlot, meta.FileId,
				)
			}
			hdr, found := bySeq[meta.BatchSeq]
			if !found || filepath.Clean(hdr.Path) != filepath.Clean(metaPath) {
				return res, fmt.Errorf(
					"accountsdb: committed fold manifest seq %d was not uniquely indexed at %s",
					meta.BatchSeq, metaPath,
				)
			}
			if identityErr := validateManifestHeaderIdentity(hdr, manifest); identityErr != nil {
				return res, fmt.Errorf("accountsdb: committed fold manifest header changed: %w", identityErr)
			}
			frontierManifest = manifest
			bankhashRepairs = append(bankhashRepairs, manifest.Bankhashes...)
		case os.IsNotExist(statErr):
			// The manifest may legitimately have aged beyond the rewind horizon.
			// A same-sequence file at a different identity is not that case.
			if hdr, found := bySeq[meta.BatchSeq]; found {
				return res, fmt.Errorf(
					"accountsdb: fold manifest seq %d at %s does not match authoritative meta slot=%d file=%d",
					meta.BatchSeq, hdr.Path, meta.ThroughSlot, meta.FileId,
				)
			}
		default:
			return res, fmt.Errorf("accountsdb: inspect committed fold manifest: %w", statErr)
		}
	}

	// A missing sequence is only an ordinary end-of-log when no higher final
	// fold exists. If a higher durable decision exists, the gap is corruption;
	// refusing before replay also avoids advancing meta partway into a known-bad
	// chain.
	if frontier.BatchSeq < maxSeq {
		for seq := frontier.BatchSeq + 1; ; seq++ {
			if _, ok := bySeq[seq]; !ok {
				return res, fmt.Errorf(
					"accountsdb: fold manifest sequence gap at %d before durable sequence %d",
					seq, maxSeq,
				)
			}
			if seq == maxSeq {
				break
			}
		}
	}

	// Replay every final fold above the frontier. A final manifest cannot be an
	// undecided crash tail: rename is the decision point, so any verification or
	// slot-chain failure is corruption and must leave all files in place.
	for seq := frontier.BatchSeq + 1; seq != 0 && seq <= maxSeq; seq++ {
		hdr := bySeq[seq]
		manifest, verr := db.verifyAndReadManifest(hdr)
		if verr != nil {
			return res, fmt.Errorf("accountsdb: verify durable fold manifest seq %d: %w", seq, verr)
		}
		if chainErr := validateFoldManifestSuccessor(frontier, manifest); chainErr != nil {
			return res, fmt.Errorf("accountsdb: durable fold manifest seq %d breaks replay slot chain: %w", seq, chainErr)
		}
		if err := db.applyManifestToIndex(manifest); err != nil {
			return res, fmt.Errorf("accountsdb: replay fold manifest seq %d: %w", seq, err)
		}
		bankhashRepairs = append(bankhashRepairs, manifest.Bankhashes...)
		res.ReplayedBatches = append(res.ReplayedBatches, seq)
		frontier = foldMeta{BatchSeq: manifest.BatchSeq, ThroughSlot: manifest.ThroughSlot, FileId: manifest.FileId}
		frontierManifest = manifest
	}
	// The manifest is authoritative and the Pebble rows are only a derived
	// lookup accelerator. Repair both the already-applied frontier and every
	// newly replayed manifest in one synced batch before startup succeeds. If
	// this fails after index meta advanced, the retained frontier manifest makes
	// the next startup retry exact and idempotent.
	if err := db.storeManifestBankhashes(bankhashRepairs); err != nil {
		return res, fmt.Errorf("accountsdb: repair manifest bankhash rows: %w", err)
	}

	// Orphan data files: newer than bootstrap, no manifest of any kind.
	orphans, err := db.removeUnreferencedDataFiles(referencedFileIDs)
	if err != nil {
		return res, err
	}
	res.OrphansRemoved = append(res.OrphansRemoved, orphans...)

	// Stale ".manifest.tmp" files: a crash between the manifest tmp-write and
	// its rename. The rename is the commit point, so a leftover tmp is never
	// valid — unlink it (runs under foldMu, before any fold can start).
	tmps, err := db.removeStaleTmpManifests()
	if err != nil {
		return res, err
	}
	res.OrphansRemoved = append(res.OrphansRemoved, tmps...)

	// Fold meta is the authoritative frontier. A retained, fully validated
	// manifest contributes only advisory bankhash/resume context.
	res.DurableThrough = frontier.ThroughSlot
	res.BatchSeq = frontier.BatchSeq
	if frontierManifest != nil {
		res.ResumeCtx = frontierManifest.ResumeCtx
		for _, bh := range frontierManifest.Bankhashes {
			if bh.Slot == frontier.ThroughSlot {
				res.RootedBankhash = bh.Bankhash
			}
		}
	}

	// Parked ".rewound" manifests mean a RewindToBatchBoundary was interrupted:
	// the store may already sit at the rewound boundary while the state file
	// still names the pre-rewind slot. The caller uses this to complete the
	// reconciliation instead of condemning the store as data loss.
	res.RewindInProgress = hasParkedRewindManifests(db.AcctsDir)

	db.lastBatchSeq = res.BatchSeq
	db.durableThrough.Store(res.DurableThrough)
	return res, nil
}

// verifyAndReadManifest fully reads a manifest and checks its segment data file
// exists with the recorded length and CRC.
func (db *AccountsDb) verifyAndReadManifest(hdr ManifestHeader) (*SegmentManifest, error) {
	manifest, err := ReadSegmentManifest(hdr.Path)
	if err != nil {
		return nil, err
	}
	if err := validateFoldManifestIdentity(manifest, hdr.Path, false); err != nil {
		return nil, err
	}
	if err := validateManifestHeaderIdentity(hdr, manifest); err != nil {
		return nil, err
	}
	dataPath := filepath.Join(db.AcctsDir, SegmentDataName(manifest.ThroughSlot, manifest.FileId))
	crc, length, err := crcOfFile(dataPath)
	if err != nil {
		return nil, fmt.Errorf("segment data unreadable: %w", err)
	}
	if uint64(length) != manifest.DataLen {
		return nil, fmt.Errorf("segment data length %d != manifest %d", length, manifest.DataLen)
	}
	if crc != manifest.DataCRC {
		return nil, fmt.Errorf("segment data crc mismatch")
	}
	return manifest, nil
}

func validateManifestHeaderIdentity(hdr ManifestHeader, manifest *SegmentManifest) error {
	if manifest == nil {
		return fmt.Errorf("nil fold manifest")
	}
	if hdr.Kind != manifest.Kind || hdr.BatchSeq != manifest.BatchSeq ||
		hdr.FromSlot != manifest.FromSlot || hdr.ThroughSlot != manifest.ThroughSlot ||
		hdr.FileId != manifest.FileId || hdr.DataLen != manifest.DataLen ||
		hdr.DataCRC != manifest.DataCRC {
		return fmt.Errorf("manifest fixed header does not match its fully CRC-validated body")
	}
	return nil
}

func validateFoldManifestIdentity(manifest *SegmentManifest, path string, allowRewound bool) error {
	if manifest == nil || manifest.Kind != ManifestKindFold || manifest.BatchSeq == 0 {
		return fmt.Errorf("invalid fold kind or zero batch sequence")
	}
	if manifest.FromSlot > manifest.ThroughSlot {
		return fmt.Errorf("fold seq %d has from slot %d beyond through slot %d", manifest.BatchSeq, manifest.FromSlot, manifest.ThroughSlot)
	}
	want := segmentManifestPath(filepath.Dir(path), manifest.ThroughSlot, manifest.FileId)
	if allowRewound && strings.HasSuffix(path, segManifestRewoundSuffix) {
		want += ".rewound"
	}
	if filepath.Clean(path) != filepath.Clean(want) {
		return fmt.Errorf("fold seq %d path %s does not match slot=%d file=%d", manifest.BatchSeq, path, manifest.ThroughSlot, manifest.FileId)
	}
	return nil
}

func validateFoldManifestSuccessor(previous foldMeta, manifest *SegmentManifest) error {
	if previous.BatchSeq == ^uint64(0) || manifest.BatchSeq != previous.BatchSeq+1 {
		return fmt.Errorf("batch sequence %d does not follow %d", manifest.BatchSeq, previous.BatchSeq)
	}
	if manifest.ThroughSlot <= previous.ThroughSlot {
		return fmt.Errorf("through slot %d does not advance beyond %d", manifest.ThroughSlot, previous.ThroughSlot)
	}
	if manifest.FromSlot < previous.ThroughSlot {
		return fmt.Errorf("from slot %d overlaps prior frontier %d", manifest.FromSlot, previous.ThroughSlot)
	}
	return nil
}

func (db *AccountsDb) storeManifestBankhashes(bankhashes []SlotBankhash) (retErr error) {
	if len(bankhashes) == 0 {
		return nil
	}
	batch := db.BankHashStore.NewBatch()
	defer func() { retErr = errors.Join(retErr, batch.Close()) }()
	for _, bh := range bankhashes {
		var slotBytes [8]byte
		binary.LittleEndian.PutUint64(slotBytes[:], bh.Slot)
		if err := batch.Set(slotBytes[:], bh.Bankhash[:], nil); err != nil {
			return fmt.Errorf("stage slot %d: %w", bh.Slot, err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("sync bankhash repair batch: %w", err)
	}
	return nil
}

// bootstrapHighFileId reads and fully validates the write-once sidecar
// recorded by snapshot build. Recovery must not guess when it is absent or
// corrupt: a guessed-low value could classify valid snapshot appendvecs as
// orphan crash tails and delete them.
func (db *AccountsDb) bootstrapHighFileId() (uint64, error) {
	high, err := ReadBootstrapHighFileID(filepath.Dir(db.AcctsDir))
	if err != nil {
		return 0, fmt.Errorf("accountsdb: read bootstrap high file ID: %w", err)
	}
	return high, nil
}

// removeUnreferencedDataFiles deletes data files with fileId above the
// bootstrap high-water mark that no manifest (fold, compact, or rewound)
// references — segments whose commit never got decided (crash matrix C1/C2).
func (db *AccountsDb) removeUnreferencedDataFiles(referenced map[uint64]struct{}) ([]string, error) {
	bootstrapHigh, err := db.bootstrapHighFileId()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != "" && !isDataFileName(name) {
			continue
		}
		_, fileId, ok := parseDataFileName(name)
		if !ok || fileId <= bootstrapHigh {
			continue
		}
		if _, ok := referenced[fileId]; ok {
			continue
		}
		p := filepath.Join(db.AcctsDir, name)
		if rerr := os.Remove(p); rerr != nil && !os.IsNotExist(rerr) {
			return removed, rerr
		}
		mlog.Log.Warnf("accountsdb: removed orphan segment %s (no manifest — commit never decided)", name)
		removed = append(removed, p)
	}
	return removed, nil
}

// isDataFileName reports whether name looks like "<slot>.<fileId>" (both
// numeric). Appendvec/segment files match; manifests and sidecars do not.
func isDataFileName(name string) bool {
	_, _, ok := parseDataFileName(name)
	return ok
}

// hasParkedRewindManifests reports whether any ".rewound" manifest remains — an
// interrupted RewindToBatchBoundary. Re-running the rewind (or the startup
// reconcile) completes it; the leftovers are never orphan-GC'd.
func hasParkedRewindManifests(acctsDir string) bool {
	entries, err := os.ReadDir(acctsDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), segManifestRewoundSuffix) {
			return true
		}
	}
	return false
}

// removeStaleTmpManifests unlinks leftover ".manifest.tmp" files. The manifest
// tmp-write is not the commit point (the rename is), so a tmp left behind by an
// interrupted WriteSegmentManifest is always discardable — its data segment,
// if any, is handled as an orphan by removeUnreferencedDataFiles.
func (db *AccountsDb) removeStaleTmpManifests() ([]string, error) {
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		p := filepath.Join(db.AcctsDir, name)
		if rerr := os.Remove(p); rerr != nil && !os.IsNotExist(rerr) {
			return removed, rerr
		}
		mlog.Log.Warnf("accountsdb: removed stale manifest tmp %s (interrupted write)", name)
		removed = append(removed, p)
	}
	return removed, nil
}
