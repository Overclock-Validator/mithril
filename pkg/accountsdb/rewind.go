package accountsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/cockroachdb/pebble"
)

// Rewind support (types + discovery). RewindToBatchBoundary — applying undo
// pointers in reverse to restore the index to an earlier batch boundary —
// lands with the compactor, which shares the pin rule that keeps every
// in-horizon Prev target alive.

// RewindPoint is a batch boundary the store can be rewound to: every fold
// manifest still on disk is one (its undo pointers and their target files are
// retained until GC'd past the rewind horizon).
type RewindPoint struct {
	BatchSeq    uint64
	ThroughSlot uint64
	Bankhash    [32]byte
}

// ListRewindPoints returns the available batch boundaries, ascending. Only
// committed batches qualify (BatchSeq <= the fold meta watermark).
func (db *AccountsDb) ListRewindPoints() ([]RewindPoint, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()

	meta, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return nil, err
	}
	if !haveMeta {
		return nil, nil
	}
	headers, err := ListFoldManifests(db.AcctsDir)
	if err != nil {
		return nil, err
	}
	out := make([]RewindPoint, 0, len(headers))
	for _, h := range headers {
		if h.BatchSeq == 0 || h.BatchSeq > meta.BatchSeq {
			continue
		}
		pt := RewindPoint{BatchSeq: h.BatchSeq, ThroughSlot: h.ThroughSlot}
		if m, err := ReadSegmentManifest(h.Path); err == nil {
			for _, bh := range m.Bankhashes {
				if bh.Slot == m.ThroughSlot {
					pt.Bankhash = bh.Bankhash
				}
			}
		}
		out = append(out, pt)
	}
	return out, nil
}

// RewindResult reports a completed rewind.
type RewindResult struct {
	NewThrough    uint64
	ResumeCtx     []byte // manifest-carried resume context at the target boundary
	UndoneBatches int
	UndoneKeys    int
}

// manifestPathEither returns the manifest path for a segment, accepting the
// parked ".rewound" form (an interrupted rewind's leftovers) so re-running a
// rewind resumes cleanly.
func (db *AccountsDb) manifestPathEither(throughSlot, fileId uint64) (string, bool) {
	p := segmentManifestPath(db.AcctsDir, throughSlot, fileId)
	if _, err := os.Stat(p); err == nil {
		return p, true
	}
	rp := p + ".rewound"
	// segmentManifestPath ends in ".manifest"; the parked form is ".manifest.rewound".
	if _, err := os.Stat(rp); err == nil {
		return rp, true
	}
	return "", false
}

// finalizeParkedRewindLeftoversLocked moves any parked ".rewound" fold manifests
// (and their segments) ABOVE throughSlot into rewound/ — the Step 3 that an
// interrupted rewind may have skipped after already rolling the meta back to
// this boundary. Idempotent and best-effort: it only clears the leftovers so
// RecoverFoldState stops flagging a rewind in progress. Caller holds foldMu.
func (db *AccountsDb) finalizeParkedRewindLeftoversLocked(throughSlot uint64) {
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return
	}
	rewoundDir := filepath.Join(db.AcctsDir, "rewound")
	madeDir := false
	moved := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segManifestRewoundSuffix) {
			continue
		}
		path := filepath.Join(db.AcctsDir, name)
		m, rerr := ReadSegmentManifest(path)
		if rerr != nil || m.Kind != ManifestKindFold || m.ThroughSlot <= throughSlot {
			continue // keep anything at/below the boundary
		}
		if !madeDir {
			if mkerr := os.MkdirAll(rewoundDir, 0o755); mkerr != nil {
				return
			}
			madeDir = true
		}
		dataName := SegmentDataName(m.ThroughSlot, m.FileId)
		for src, dst := range map[string]string{
			filepath.Join(db.AcctsDir, dataName): filepath.Join(rewoundDir, dataName),
			path:                                 filepath.Join(rewoundDir, name),
		} {
			if merr := os.Rename(src, dst); merr != nil && !os.IsNotExist(merr) {
				mlog.Log.Warnf("accountsdb: rewind finalize: could not move %s aside: %v", src, merr)
			}
		}
		moved = true
	}
	if moved {
		_ = fsyncDir(db.AcctsDir)
	}
}

// RewindToBatchBoundary restores the index to the state as of the fold batch
// whose ThroughSlot == throughSlot by applying the undo pointers of every
// LATER fold manifest in reverse order. The store must be quiesced (no
// replay, no folds) — callers are node startup (--rewind-to-slot) and the
// halt-path recovery loop.
//
// Crash-safe and idempotent: suffix manifests are parked as ".rewound" first
// (recovery ignores them; a re-run picks them back up), then ONE index batch
// applies all undo pointers + the meta rollback atomically, then the undone
// segment files move to rewound/ for forensics.
func (db *AccountsDb) RewindToBatchBoundary(throughSlot uint64) (RewindResult, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()

	res := RewindResult{}
	meta, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return res, err
	}
	if !haveMeta {
		return res, fmt.Errorf("accountsdb: rewind: store has no fold meta (nothing folded)")
	}
	if meta.ThroughSlot == throughSlot {
		res.NewThrough = throughSlot
		if path, ok := db.manifestPathEither(throughSlot, meta.FileId); ok {
			if m, rerr := ReadSegmentManifest(path); rerr == nil {
				res.ResumeCtx = m.ResumeCtx
			}
		}
		// A prior rewind may have rolled the meta back to this boundary but
		// crashed before moving the undone files aside — finalize any leftover
		// parked manifests now so recovery stops reporting a rewind in progress.
		db.finalizeParkedRewindLeftoversLocked(throughSlot)
		return res, nil // already at the target boundary
	}

	// Collect every fold manifest (including parked ones) by seq.
	type seqManifest struct {
		manifest *SegmentManifest
		path     string
	}
	bySeq := make(map[uint64]*seqManifest)
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		return res, err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(name, segManifestSuffix) && !strings.HasSuffix(name, segManifestRewoundSuffix) {
			continue
		}
		if strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		m, rerr := ReadSegmentManifest(filepath.Join(db.AcctsDir, name))
		if rerr != nil || m.Kind != ManifestKindFold {
			continue
		}
		bySeq[m.BatchSeq] = &seqManifest{manifest: m, path: filepath.Join(db.AcctsDir, name)}
	}

	target, ok := func() (*seqManifest, bool) {
		for _, sm := range bySeq {
			if sm.manifest.ThroughSlot == throughSlot {
				return sm, true
			}
		}
		return nil, false
	}()
	if !ok {
		return res, fmt.Errorf("accountsdb: rewind: no fold boundary at slot %d within the retained horizon (see ListRewindPoints)", throughSlot)
	}
	seqT := target.manifest.BatchSeq
	if seqT >= meta.BatchSeq {
		return res, fmt.Errorf("accountsdb: rewind: boundary seq %d is not below the committed head seq %d", seqT, meta.BatchSeq)
	}
	// Every batch in (seqT, head] must be present to unwind completely.
	for seq := seqT + 1; seq <= meta.BatchSeq; seq++ {
		if bySeq[seq] == nil {
			return res, fmt.Errorf("accountsdb: rewind: fold manifest seq %d missing — cannot unwind past it (horizon GC'd?)", seq)
		}
	}

	// Step 1: park suffix manifests (ascending) so recovery treats the
	// batches as undone even if we crash mid-rewind.
	for seq := seqT + 1; seq <= meta.BatchSeq; seq++ {
		sm := bySeq[seq]
		if strings.HasSuffix(sm.path, segManifestRewoundSuffix) {
			continue // already parked by an interrupted rewind
		}
		parked := sm.path + ".rewound"
		if err := os.Rename(sm.path, parked); err != nil {
			return res, fmt.Errorf("accountsdb: rewind: park manifest seq %d: %w", seq, err)
		}
		sm.path = parked
	}
	if err := fsyncDir(db.AcctsDir); err != nil {
		return res, err
	}

	// Step 2: ONE index batch applying undo pointers newest-batch-first (a
	// key overwritten in several undone batches ends at its pre-suffix
	// entry) + the meta rollback. Atomic under Pebble.
	batch := db.Index.NewBatch()
	defer batch.Close()
	var idxBuf [24]byte
	for seq := meta.BatchSeq; seq > seqT; seq-- {
		m := bySeq[seq].manifest
		for i := range m.Records {
			r := &m.Records[i]
			if r.PrevValid {
				r.Prev.Marshal(&idxBuf)
				if err := batch.Set(r.Pubkey[:], idxBuf[:], nil); err != nil {
					return res, err
				}
			} else {
				if err := batch.Delete(r.Pubkey[:], nil); err != nil {
					return res, err
				}
			}
			res.UndoneKeys++
		}
		res.UndoneBatches++
	}
	if err := batch.Set(metaKeyLastBatch, encodeFoldMeta(foldMeta{
		BatchSeq:    seqT,
		ThroughSlot: target.manifest.ThroughSlot,
		FileId:      target.manifest.FileId,
	}), nil); err != nil {
		return res, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return res, err
	}
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return res, err
		}
	}

	// Step 3: move the undone segments + parked manifests aside (forensics).
	rewoundDir := filepath.Join(db.AcctsDir, "rewound")
	if err := os.MkdirAll(rewoundDir, 0o755); err != nil {
		return res, err
	}
	for seq := seqT + 1; seq <= meta.BatchSeq; seq++ {
		sm := bySeq[seq]
		dataName := SegmentDataName(sm.manifest.ThroughSlot, sm.manifest.FileId)
		for src, dst := range map[string]string{
			filepath.Join(db.AcctsDir, dataName): filepath.Join(rewoundDir, dataName),
			sm.path:                              filepath.Join(rewoundDir, filepath.Base(sm.path)),
		} {
			if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
				mlog.Log.Warnf("accountsdb: rewind: could not move %s aside: %v", src, err)
			}
		}
	}
	_ = fsyncDir(db.AcctsDir)

	db.lastBatchSeq = seqT
	db.durableThrough.Store(target.manifest.ThroughSlot)
	// Read caches may hold folded-then-rewound values; rebuild them.
	db.InitCaches()

	res.NewThrough = target.manifest.ThroughSlot
	res.ResumeCtx = target.manifest.ResumeCtx
	mlog.Log.Warnf("accountsdb: REWOUND %d fold batch(es) (%d keys) — durable state restored to slot %d", res.UndoneBatches, res.UndoneKeys, res.NewThrough)
	return res, nil
}
