package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
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
	// without a durable manifest, tmp manifests, torn manifests).
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
//  2. scan fold manifests; REPLAY the contiguous BatchSeq run above the
//     watermark into the index (verifying segment length + CRC first)
//  3. stop at the first gap or torn manifest; everything above the stop point
//     is undecided — delete those manifests + segments as orphans
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

	headers, err := ListFoldManifests(db.AcctsDir)
	if err != nil {
		return res, err
	}
	bySeq := make(map[uint64]ManifestHeader, len(headers))
	for _, h := range headers {
		if h.BatchSeq == 0 { // unparseable header: quarantine below via orphan pass
			res.OrphansRemoved = append(res.OrphansRemoved, h.Path)
			if rerr := os.Remove(h.Path); rerr != nil && !os.IsNotExist(rerr) {
				return res, rerr
			}
			continue
		}
		bySeq[h.BatchSeq] = h
	}

	// The applied frontier: meta if present, else fresh (seq 0).
	frontier := foldMeta{}
	if haveMeta {
		frontier = meta
	}

	// Replay the contiguous run above the frontier.
	stopSeq := frontier.BatchSeq // last VALID seq (inclusive)
	for seq := frontier.BatchSeq + 1; ; seq++ {
		hdr, ok := bySeq[seq]
		if !ok {
			break
		}
		manifest, verr := db.verifyAndReadManifest(hdr)
		if verr != nil {
			mlog.Log.Warnf("accountsdb: fold manifest seq %d failed verification (%v); treating batch and everything above as undecided", seq, verr)
			break
		}
		if err := db.applyManifestToIndex(manifest); err != nil {
			return res, fmt.Errorf("accountsdb: replay fold manifest seq %d: %w", seq, err)
		}
		db.storeManifestBankhashes(manifest)
		res.ReplayedBatches = append(res.ReplayedBatches, seq)
		stopSeq = seq
	}

	// Everything above the stop point is undecided: remove manifests + data.
	for seq, hdr := range bySeq {
		if seq <= stopSeq {
			continue
		}
		dataPath := filepath.Join(db.AcctsDir, SegmentDataName(hdr.ThroughSlot, hdr.FileId))
		for _, p := range []string{hdr.Path, dataPath} {
			if rerr := os.Remove(p); rerr != nil && !os.IsNotExist(rerr) {
				return res, rerr
			}
			res.OrphansRemoved = append(res.OrphansRemoved, p)
		}
	}

	// Orphan data files: newer than bootstrap, no manifest of any kind.
	orphans, err := db.removeUnreferencedDataFiles()
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

	// Resolve the final frontier's manifest for bankhash + resume context.
	if stopSeq > 0 {
		if hdr, ok := bySeq[stopSeq]; ok {
			if manifest, err := ReadSegmentManifest(hdr.Path); err == nil {
				res.DurableThrough = manifest.ThroughSlot
				res.BatchSeq = manifest.BatchSeq
				res.ResumeCtx = manifest.ResumeCtx
				for _, bh := range manifest.Bankhashes {
					if bh.Slot == manifest.ThroughSlot {
						res.RootedBankhash = bh.Bankhash
					}
				}
			} else {
				// Meta says this seq committed but its manifest is unreadable:
				// the index is still authoritative; report the watermark alone.
				mlog.Log.Warnf("accountsdb: manifest for committed fold seq %d unreadable (%v); resume context unavailable from store", stopSeq, err)
				res.DurableThrough = hdr.ThroughSlot
				res.BatchSeq = hdr.BatchSeq
			}
		} else if haveMeta && stopSeq == meta.BatchSeq {
			// Committed long ago and its manifest already GC'd (post-horizon).
			res.DurableThrough = meta.ThroughSlot
			res.BatchSeq = meta.BatchSeq
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

func (db *AccountsDb) storeManifestBankhashes(m *SegmentManifest) {
	for _, bh := range m.Bankhashes {
		var slotBytes [8]byte
		binary.LittleEndian.PutUint64(slotBytes[:], bh.Slot)
		if err := db.BankHashStore.Set(slotBytes[:], bh.Bankhash[:], nil); err != nil {
			mlog.Log.Warnf("accountsdb: recovery bankhash store slot %d: %v", bh.Slot, err)
		}
	}
}

// bootstrapHighFileId reads the write-once sidecar recorded by snapshot build.
// Missing sidecar (pre-upgrade store) returns MaxUint64: every manifest-less
// file is then classified as bootstrap — a space leak at worst, never a
// correctness issue.
func (db *AccountsDb) bootstrapHighFileId() uint64 {
	path := filepath.Join(filepath.Dir(db.AcctsDir), "bootstrap_high_file_id")
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 8 {
		return ^uint64(0)
	}
	return binary.LittleEndian.Uint64(data[:8])
}

// removeUnreferencedDataFiles deletes data files with fileId above the
// bootstrap high-water mark that no manifest (fold, compact, or rewound)
// references — segments whose commit never got decided (crash matrix C1/C2).
func (db *AccountsDb) removeUnreferencedDataFiles() ([]string, error) {
	bootstrapHigh := db.bootstrapHighFileId()
	if bootstrapHigh == ^uint64(0) {
		return nil, nil
	}
	referenced, err := listAllManifestFileIds(db.AcctsDir)
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
