package accountsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
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

type rewindSeqManifest struct {
	manifest *SegmentManifest
	path     string
}

func collectValidatedRewindManifests(
	acctsDir string,
	includeParked bool,
) (map[uint64]*rewindSeqManifest, map[uint64]*rewindSeqManifest, error) {
	entries, err := os.ReadDir(acctsDir)
	if err != nil {
		return nil, nil, err
	}
	bySeq := make(map[uint64]*rewindSeqManifest)
	bySlot := make(map[uint64]*rewindSeqManifest)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		parked := strings.HasSuffix(name, segManifestRewoundSuffix)
		ordinary := strings.HasSuffix(name, segManifestSuffix)
		if (!ordinary && !parked) || (parked && !includeParked) ||
			strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		path := filepath.Join(acctsDir, name)
		hdr, err := readManifestHeader(path)
		if err != nil {
			return nil, nil, fmt.Errorf("accountsdb: rewind: read manifest header %s: %w", path, err)
		}
		if hdr.Kind != ManifestKindFold {
			continue
		}
		manifest, err := ReadSegmentManifest(path)
		if err != nil {
			return nil, nil, fmt.Errorf("accountsdb: rewind: fold manifest %s is not fully CRC-valid: %w", path, err)
		}
		if err := validateManifestHeaderIdentity(hdr, manifest); err != nil {
			return nil, nil, fmt.Errorf("accountsdb: rewind: fold manifest header changed at %s: %w", path, err)
		}
		if err := validateFoldManifestIdentity(manifest, path, includeParked); err != nil {
			return nil, nil, fmt.Errorf("accountsdb: rewind: invalid fold manifest identity: %w", err)
		}
		selected := &rewindSeqManifest{manifest: manifest, path: path}
		if prior, duplicate := bySeq[manifest.BatchSeq]; duplicate {
			return nil, nil, fmt.Errorf(
				"accountsdb: rewind: duplicate fold manifest sequence %d: %s and %s",
				manifest.BatchSeq, prior.path, path,
			)
		}
		if prior, duplicate := bySlot[manifest.ThroughSlot]; duplicate {
			return nil, nil, fmt.Errorf(
				"accountsdb: rewind: duplicate fold through slot %d: %s and %s",
				manifest.ThroughSlot, prior.path, path,
			)
		}
		bySeq[manifest.BatchSeq] = selected
		bySlot[manifest.ThroughSlot] = selected
	}
	return bySeq, bySlot, nil
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
	// Include parked manifests so a crash part-way through the ascending park
	// loop cannot make an ordinary suffix look like a valid rewind target. The
	// first parked sequence identifies the originally requested boundary.
	bySeq, _, err := collectValidatedRewindManifests(db.AcctsDir, true)
	if err != nil {
		return nil, err
	}
	if head := bySeq[meta.BatchSeq]; head != nil &&
		(head.manifest.ThroughSlot != meta.ThroughSlot || head.manifest.FileId != meta.FileId) {
		return nil, fmt.Errorf(
			"accountsdb: rewind points: head manifest does not match authoritative fold meta",
		)
	}
	ceiling := meta.BatchSeq
	interruptedTarget, interrupted, err := validateInterruptedRewindLayout(meta, bySeq)
	if err != nil {
		return nil, err
	}
	if interrupted {
		ceiling = interruptedTarget
	}
	sequences := make([]uint64, 0, len(bySeq))
	for sequence := range bySeq {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	out := make([]RewindPoint, 0, len(sequences))
	for _, sequence := range sequences {
		selected := bySeq[sequence]
		manifest := selected.manifest
		if strings.HasSuffix(selected.path, segManifestRewoundSuffix) || manifest.BatchSeq > ceiling {
			continue
		}
		pt := RewindPoint{BatchSeq: manifest.BatchSeq, ThroughSlot: manifest.ThroughSlot}
		for _, bh := range manifest.Bankhashes {
			if bh.Slot == manifest.ThroughSlot {
				pt.Bankhash = bh.Bankhash
			}
		}
		out = append(out, pt)
	}
	return out, nil
}

// validateInterruptedRewindLayout returns the exact target sequence encoded by
// an interrupted ascending park operation. Before the fold-meta rollback, the
// parked range starts at target+1 and may be followed by the not-yet-parked
// ordinary suffix. After the rollback, meta itself is the target and Step 3
// may already have moved an initial part of the parked suffix to rewound/.
// Any other parked/ordinary pattern is ambiguous and fails closed.
func validateInterruptedRewindLayout(
	meta foldMeta,
	bySeq map[uint64]*rewindSeqManifest,
) (uint64, bool, error) {
	minParked := ^uint64(0)
	maxPresent := uint64(0)
	for sequence, selected := range bySeq {
		maxPresent = max(maxPresent, sequence)
		if strings.HasSuffix(selected.path, segManifestRewoundSuffix) {
			minParked = min(minParked, sequence)
		}
	}
	if minParked == ^uint64(0) {
		return 0, false, nil
	}
	if meta.BatchSeq == 0 {
		return 0, false, fmt.Errorf("accountsdb: rewind points: parked manifests exist without authoritative fold meta")
	}

	targetSequence := meta.BatchSeq
	beforeMetaRollback := meta.BatchSeq >= minParked
	if beforeMetaRollback {
		if minParked == 0 {
			return 0, false, fmt.Errorf("accountsdb: rewind points: parked sequence zero cannot identify a target")
		}
		targetSequence = minParked - 1
		if maxPresent > meta.BatchSeq {
			return 0, false, fmt.Errorf(
				"accountsdb: rewind points: manifest sequence %d exists beyond authoritative head %d during interrupted rewind",
				maxPresent, meta.BatchSeq,
			)
		}
	}
	target := bySeq[targetSequence]
	if target == nil || strings.HasSuffix(target.path, segManifestRewoundSuffix) {
		return 0, false, fmt.Errorf(
			"accountsdb: rewind points: interrupted rewind target sequence %d is missing or parked",
			targetSequence,
		)
	}
	if targetSequence == meta.BatchSeq &&
		(target.manifest.ThroughSlot != meta.ThroughSlot || target.manifest.FileId != meta.FileId) {
		return 0, false, fmt.Errorf("accountsdb: rewind points: interrupted rewind target does not match authoritative fold meta")
	}

	if beforeMetaRollback {
		chain := foldMeta{
			BatchSeq:    target.manifest.BatchSeq,
			ThroughSlot: target.manifest.ThroughSlot,
			FileId:      target.manifest.FileId,
		}
		seenOrdinarySuffix := false
		for sequence := targetSequence + 1; ; sequence++ {
			selected := bySeq[sequence]
			if selected == nil {
				return 0, false, fmt.Errorf(
					"accountsdb: rewind points: interrupted pre-rollback chain is missing sequence %d",
					sequence,
				)
			}
			if err := validateFoldManifestSuccessor(chain, selected.manifest); err != nil {
				return 0, false, fmt.Errorf(
					"accountsdb: rewind points: interrupted chain at sequence %d: %w",
					sequence, err,
				)
			}
			parked := strings.HasSuffix(selected.path, segManifestRewoundSuffix)
			if parked && seenOrdinarySuffix {
				return 0, false, fmt.Errorf(
					"accountsdb: rewind points: parked sequence %d follows an ordinary suffix entry",
					sequence,
				)
			}
			seenOrdinarySuffix = seenOrdinarySuffix || !parked
			chain = foldMeta{
				BatchSeq:    selected.manifest.BatchSeq,
				ThroughSlot: selected.manifest.ThroughSlot,
				FileId:      selected.manifest.FileId,
			}
			if sequence == meta.BatchSeq {
				break
			}
		}
	} else {
		// Meta already committed the target. Step 3 moves parked manifests in
		// ascending sequence order, so remaining root-level entries above meta
		// must be one contiguous parked suffix. Gaps before minParked are allowed:
		// those manifests have already reached rewound/.
		var chain foldMeta
		haveChain := false
		for sequence := minParked; ; sequence++ {
			selected := bySeq[sequence]
			if selected == nil || !strings.HasSuffix(selected.path, segManifestRewoundSuffix) {
				return 0, false, fmt.Errorf(
					"accountsdb: rewind points: post-rollback suffix sequence %d is missing or ordinary",
					sequence,
				)
			}
			if haveChain {
				if err := validateFoldManifestSuccessor(chain, selected.manifest); err != nil {
					return 0, false, fmt.Errorf(
						"accountsdb: rewind points: interrupted post-rollback chain at sequence %d: %w",
						sequence, err,
					)
				}
			}
			chain = foldMeta{
				BatchSeq:    selected.manifest.BatchSeq,
				ThroughSlot: selected.manifest.ThroughSlot,
				FileId:      selected.manifest.FileId,
			}
			haveChain = true
			if sequence == maxPresent {
				break
			}
		}
		for sequence, selected := range bySeq {
			if sequence > meta.BatchSeq && !strings.HasSuffix(selected.path, segManifestRewoundSuffix) {
				return 0, false, fmt.Errorf(
					"accountsdb: rewind points: ordinary sequence %d exists beyond a committed rewind target",
					sequence,
				)
			}
		}
	}
	return targetSequence, true, nil
}

// RewindResult reports a completed rewind.
type RewindResult struct {
	NewThrough    uint64
	ResumeCtx     []byte // manifest-carried resume context at the target boundary
	UndoneBatches int
	UndoneKeys    int
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
// (recovery ignores them; a re-run picks them back up), then bounded index
// frames apply exact undo assignments and a final meta rollback commits the
// operation. The store is quiescent and account readers are excluded, so no
// intermediate frame is externally visible; a crash simply replays the whole
// idempotent rewind. Finally, undone segment files move to rewound/.
func (db *AccountsDb) RewindToBatchBoundary(throughSlot uint64) (RewindResult, error) {
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()

	res := RewindResult{}
	meta, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return res, err
	}
	if !haveMeta {
		return res, fmt.Errorf("accountsdb: rewind: store has no fold meta (nothing folded)")
	}
	bySeq, bySlot, err := collectValidatedRewindManifests(db.AcctsDir, true)
	if err != nil {
		return res, err
	}
	if head := bySeq[meta.BatchSeq]; head != nil &&
		(head.manifest.ThroughSlot != meta.ThroughSlot || head.manifest.FileId != meta.FileId) {
		return res, fmt.Errorf(
			"accountsdb: rewind: head manifest seq=%d slot=%d file=%d does not match authoritative meta seq=%d slot=%d file=%d",
			head.manifest.BatchSeq, head.manifest.ThroughSlot, head.manifest.FileId,
			meta.BatchSeq, meta.ThroughSlot, meta.FileId,
		)
	}
	interruptedTarget, interrupted, err := validateInterruptedRewindLayout(meta, bySeq)
	if err != nil {
		return res, err
	}
	if interrupted {
		exactTarget := bySeq[interruptedTarget]
		if exactTarget == nil {
			return res, fmt.Errorf(
				"accountsdb: rewind: interrupted rewind exact target sequence %d disappeared",
				interruptedTarget,
			)
		}
		if exactTarget.manifest.ThroughSlot != throughSlot {
			return res, fmt.Errorf(
				"accountsdb: rewind: interrupted rewind must complete its exact target sequence %d slot %d (requested slot %d)",
				interruptedTarget,
				exactTarget.manifest.ThroughSlot,
				throughSlot,
			)
		}
	}
	if meta.ThroughSlot == throughSlot {
		if target := bySlot[throughSlot]; target != nil {
			if target.manifest.BatchSeq != meta.BatchSeq || target.manifest.FileId != meta.FileId {
				return res, fmt.Errorf("accountsdb: rewind: target slot %d does not match authoritative meta identity", throughSlot)
			}
			res.ResumeCtx = target.manifest.ResumeCtx
		}
		// This also repairs the in-memory publication state after a prior
		// WAL-less commit succeeded but its explicit Flush reported an error.
		db.readCacheEpochMu.Lock()
		db.readCacheEpoch++
		db.resetReadCachesLocked()
		db.readCacheEpochMu.Unlock()
		db.lastBatchSeq = meta.BatchSeq
		db.durableThrough.Store(meta.ThroughSlot)
		res.NewThrough = throughSlot
		// A prior rewind may have rolled the meta back to this boundary but
		// crashed before moving the undone files aside — finalize any leftover
		// parked manifests now so recovery stops reporting a rewind in progress.
		db.finalizeParkedRewindLeftoversLocked(throughSlot)
		return res, nil // already at the target boundary
	}

	target := bySlot[throughSlot]
	if target == nil {
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
	chain := foldMeta{BatchSeq: target.manifest.BatchSeq, ThroughSlot: target.manifest.ThroughSlot, FileId: target.manifest.FileId}
	for seq := seqT + 1; seq <= meta.BatchSeq; seq++ {
		manifest := bySeq[seq].manifest
		if err := validateFoldManifestSuccessor(chain, manifest); err != nil {
			return res, fmt.Errorf("accountsdb: rewind: invalid fold slot chain at seq %d: %w", seq, err)
		}
		chain = foldMeta{BatchSeq: manifest.BatchSeq, ThroughSlot: manifest.ThroughSlot, FileId: manifest.FileId}
	}
	if chain.BatchSeq != meta.BatchSeq || chain.ThroughSlot != meta.ThroughSlot || chain.FileId != meta.FileId {
		return res, fmt.Errorf("accountsdb: rewind: retained fold chain does not terminate at authoritative meta")
	}

	// Compute the exact final assignment per key before parking anything. We
	// traverse newest-batch-first and overwrite with each older undo, so a key
	// changed in several suffix batches ends at its pre-suffix entry. Dedupe is
	// important for a long rewind horizon: WAL and memory scale with unique
	// accounts, not historical writes.
	undo := make(map[solana.PublicKey]deltaIndexValue)
	for seq := meta.BatchSeq; seq > seqT; seq-- {
		m := bySeq[seq].manifest
		for i := range m.Records {
			r := &m.Records[i]
			if r.PrevValid {
				undo[r.Pubkey] = deltaIndexValue{Entry: r.Prev}
			} else {
				// Absence is an exact full-key entry in the RAM head. Removing the
				// override would expose an older immutable generation.
				undo[r.Pubkey] = deltaIndexValue{Tombstone: true}
			}
			res.UndoneKeys++
		}
		res.UndoneBatches++
	}
	undoKeys := make([]solana.PublicKey, 0, len(undo))
	for key := range undo {
		undoKeys = append(undoKeys, key)
	}
	sort.Slice(undoKeys, func(i, j int) bool {
		return bytes.Compare(undoKeys[i][:], undoKeys[j][:]) < 0
	})
	mutations := make([]deltaIndexMutation, 0, len(undoKeys))
	for _, key := range undoKeys {
		value := undo[key]
		if value.Tombstone {
			mutations = append(mutations, tombstoneDeltaMutation(key))
		} else {
			mutations = append(mutations, liveDeltaMutation(key, value.Entry))
		}
	}

	// Step 1: park suffix manifests (ascending) so recovery treats the
	// batches as undone even if we crash mid-rewind.
	// (The complete mutation transaction above was prepared first so allocation
	// failure cannot leave a half-prepared rewind on disk.)
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
		// Persist each ascending rename before beginning the next one. After
		// power loss, the durable parked set is therefore provably a prefix,
		// which is the ordering used to derive the exact interrupted target.
		if err := fsyncDir(db.AcctsDir); err != nil {
			return res, fmt.Errorf("accountsdb: rewind: sync parked manifest seq %d: %w", seq, err)
		}
	}
	// A prior attempt may have returned after a rename whose directory fsync
	// failed. Re-sync the complete visible prefix even when this retry skipped
	// every already-parked entry; meta must not roll back while any observed
	// parking rename still has uncertain durability.
	if err := fsyncDir(db.AcctsDir); err != nil {
		return res, fmt.Errorf("accountsdb: rewind: sync complete parked manifest prefix: %w", err)
	}

	// Step 2: bounded exact WAL frames, followed by the fold-meta rollback
	// commit marker. Account readers remain excluded across the whole sequence.
	targetMeta := foldMeta{
		BatchSeq:    seqT,
		ThroughSlot: target.manifest.ThroughSlot,
		FileId:      target.manifest.FileId,
	}
	db.readCacheEpochMu.Lock()
	commitErr := db.applyAccountIndexMutationTransaction(mutations, &targetMeta)
	if commitErr == nil {
		db.readCacheEpoch++
		db.resetReadCachesLocked()
	}
	db.readCacheEpochMu.Unlock()
	if commitErr != nil {
		return res, commitErr
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

	res.NewThrough = target.manifest.ThroughSlot
	res.ResumeCtx = target.manifest.ResumeCtx
	mlog.Log.Warnf("accountsdb: REWOUND %d fold batch(es) (%d keys) — durable state restored to slot %d", res.UndoneBatches, res.UndoneKeys, res.NewThrough)
	return res, nil
}
