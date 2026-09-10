package accountsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
)

type foldMeta struct {
	BatchSeq    uint64
	ThroughSlot uint64
	FileId      uint64
}

// readFoldMeta returns the current fold watermark, or (zero, false) on a fresh
// (never-folded) store.
func (db *AccountsDb) readFoldMeta() (foldMeta, bool, error) {
	if db.ProductionIndex != nil {
		meta, ok := db.ProductionIndex.ReadFoldMeta()
		return meta, ok, nil
	}
	if db.Index == nil {
		return foldMeta{}, false, nil
	}
	meta, ok := db.Index.ReadFoldMeta()
	return meta, ok, nil
}

// foldTestHooks fire between CommitBatch stages so tests can inject crashes
// (each hook may panic) at every point of the crash matrix.
type foldTestHooks struct {
	afterSegmentFsync         func()
	afterManifestRename       func()
	beforeIndexCommit         func()
	afterPublicationStart     func()
	afterIndexTxnFrame        func(completed, total int)
	afterIndexCommit          func()
	afterCompactionSourceScan func()
}

func fire(h func()) {
	if h != nil {
		h()
	}
}

// BatchCommitResult summarizes one durable fold.
type BatchCommitResult struct {
	BatchSeq    uint64
	FileId      uint64
	ThroughSlot uint64
	Keys        int
	Bytes       int64
}

// FoldDiskAdmission reserves filesystem headroom for one fold. Admission runs
// before CommitBatch takes foldMu, allocates a file ID, or writes any data. A
// successful callback must return a non-nil, idempotent release function that
// remains held through the complete fold.
type FoldDiskAdmission func(requiredBytes uint64) (release func(), err error)

// SetFoldDiskAdmission installs the process-local appendvec disk policy. Node
// startup calls this before replay; the mutex also makes clearing it during a
// quiesced shutdown race-safe for embedded callers.
func (db *AccountsDb) SetFoldDiskAdmission(admission FoldDiskAdmission) error {
	if db == nil {
		return errors.New("accountsdb: set fold disk admission on nil database")
	}
	db.foldDiskAdmissionMu.Lock()
	db.foldDiskAdmission = admission
	db.foldDiskAdmissionMu.Unlock()
	return nil
}

const (
	// Besides the appendvec body, a fold may publish its manifest, index WAL
	// frames, bank-hash metadata and filesystem bookkeeping. Charging 512 bytes
	// per input account (before dedupe) plus a fixed 64 MiB is intentionally an
	// upper bound rather than a prediction of the final segment size.
	foldDiskBytesPerInputAccount = uint64(512)
	foldDiskFixedHeadroomBytes   = uint64(64 << 20)
)

func estimateFoldDiskBytes(deltas []accounts.SlotDelta) (uint64, error) {
	required := foldDiskFixedHeadroomBytes
	for _, delta := range deltas {
		for _, account := range delta.Delta {
			if account == nil {
				continue
			}
			dataBytes := uint64(len(account.Data))
			if dataBytes > math.MaxUint64-7 {
				return 0, errors.New("accountsdb: fold account data length overflows alignment")
			}
			alignedDataBytes := (dataBytes + 7) &^ uint64(7)
			charge := saturatingAddUint64(foldDiskBytesPerInputAccount, alignedDataBytes)
			if charge == math.MaxUint64 || required > math.MaxUint64-charge {
				return 0, errors.New("accountsdb: estimated fold disk requirement overflows uint64")
			}
			required += charge
		}
	}
	return required, nil
}

func (db *AccountsDb) acquireFoldDiskAdmission(deltas []accounts.SlotDelta) (func(), error) {
	db.foldDiskAdmissionMu.RLock()
	admit := db.foldDiskAdmission
	db.foldDiskAdmissionMu.RUnlock()
	if admit == nil {
		return func() {}, nil
	}
	required, err := estimateFoldDiskBytes(deltas)
	if err != nil {
		return nil, err
	}
	release, err := admit(required)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: admit fold requiring up to %d bytes: %w", required, err)
	}
	if release == nil {
		return nil, errors.New("accountsdb: fold disk admission returned a nil release function")
	}
	return release, nil
}

type dedupedVersion struct {
	acct      *accounts.Account
	ownerSlot uint64
}

// CommitBatch durably folds a batch of slot deltas as ONE sequential segment +
// ONE manifest + ONE index batch:
//
//  1. union-dedupe newest-wins across the deltas
//  2. allocate a fileId (persisting the high-water mark first — never reused)
//  3. write + fsync the segment data file (appendvec record encoding)
//  4. capture undo pointers (each key's current index entry)
//  5. write + fsync the manifest — once durable, the commit is DECIDED;
//     recovery completes it from here (crash matrix C3)
//  6. record bankhashes (advisory, NoSync — recoverable from the manifest)
//  7. commit index entries + fold meta in one journal frame (the epoch flip)
//  8. refresh read caches and advance the in-memory watermark
//
// The fold never overwrites existing data files, so concurrent readers are
// never invalidated and old versions remain for rewind until GC'd past the
// horizon.
func (db *AccountsDb) CommitBatch(
	deltas []accounts.SlotDelta,
	throughSlot uint64,
	bankhashes map[uint64][32]byte,
	resumeCtx []byte,
) (BatchCommitResult, error) {
	// Disk admission is the first operation: failure must leave the file-ID
	// selector, appendvec directory, manifest chain, bankhash store and account
	// index untouched. The admission guard stays held until every fold side
	// effect has completed, serializing it with emergency compaction.
	releaseDiskAdmission, err := db.acquireFoldDiskAdmission(deltas)
	if err != nil {
		return BatchCommitResult{}, err
	}
	defer releaseDiskAdmission()

	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	if db.foldFatal != nil {
		return BatchCommitResult{}, fmt.Errorf(
			"accountsdb: refusing fold after an unresolved decided commit: %w",
			db.foldFatal,
		)
	}
	if db.lastBatchSeq == ^uint64(0) {
		return BatchCommitResult{}, errors.New("accountsdb: fold batch sequence exhausted")
	}

	// (1) Union-dedupe, newest wins. Deltas MUST arrive strictly ascending by
	// slot: newest-wins depends on iterating in order so a later slot's version
	// overwrites an earlier one. Validate it so a future fork-aware caller can
	// never silently fold stale state from mis-ordered deltas.
	// A manifest-only batch still belongs to the durable slot chain. Production
	// promotion supplies at least one SlotDelta (even for an empty block), but
	// retain a correct lower bound for direct callers that commit an empty slice.
	fromSlot := db.durableThrough.Load()
	prevSlot := uint64(0)
	havePrev := false
	union := make(map[[32]byte]dedupedVersion)
	for i, sd := range deltas {
		if havePrev && sd.Slot <= prevSlot {
			return BatchCommitResult{}, fmt.Errorf("accountsdb: CommitBatch deltas must be strictly ascending by slot; slot %d does not follow %d", sd.Slot, prevSlot)
		}
		prevSlot, havePrev = sd.Slot, true
		if i == 0 && sd.Slot > 0 {
			fromSlot = sd.Slot - 1
		}
		if sd.Slot > throughSlot {
			return BatchCommitResult{}, fmt.Errorf("accountsdb: CommitBatch delta slot %d exceeds throughSlot %d", sd.Slot, throughSlot)
		}
		for _, a := range sd.Delta {
			if a == nil {
				continue
			}
			union[[32]byte(a.Key)] = dedupedVersion{acct: a, ownerSlot: sd.Slot}
		}
	}
	// Preflight the selected index before any file-id/data/manifest side effect.
	// Replay normally shortens a fold to one WAL frame at a slot boundary. A
	// single exceptionally large valid slot remains supported: pendingFold
	// supplies its atomic read view while bounded idempotent frames are synced,
	// and only the final frame advances fold meta.
	if err := db.validateBatchAccountIndexMutations(uint64(len(union))); err != nil {
		return BatchCommitResult{}, err
	}
	// An all-empty batch (empty blocks) still folds: the manifest records the
	// batch's bankhashes + resume context and advances the watermark; the data
	// file is empty and no index entries are written.

	// Deterministic segment layout (test reproducibility + stable offsets).
	keys := make([][32]byte, 0, len(union))
	for k := range union {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	// (2) fileId allocation; persist high-water BEFORE the first data byte (I7).
	fileId, err := db.allocateFileID()
	if err != nil {
		return BatchCommitResult{}, err
	}

	// The old-location lookup is independent of encoding the new immutable
	// segment. Start it now so base-index mmap misses and appendvec header reads
	// overlap segment serialization and fsync, then join before the manifest
	// makes the commit durable.
	type prevLookupResult struct {
		entries []AccountIndexEntry
		found   []bool
		err     error
	}
	prevKeys := make([]solana.PublicKey, len(keys))
	for i := range keys {
		prevKeys[i] = keys[i]
	}
	prevLookupDone := make(chan prevLookupResult, 1)
	go func() {
		entries, found, err := db.lookupExactAccountIndexEntries(prevKeys)
		prevLookupDone <- prevLookupResult{entries: entries, found: found, err: err}
	}()
	var (
		prevLookup     prevLookupResult
		prevLookupOnce sync.Once
	)
	waitForPrevLookup := func() {
		prevLookupOnce.Do(func() { prevLookup = <-prevLookupDone })
	}
	defer waitForPrevLookup()

	// (3) Stream each deduped record straight to the segment file (buffered),
	// updating a running CRC and byte count as we go, so the fully serialized
	// segment is never materialized in RAM — only the bufio buffer is. Record
	// offsets are the byte position before each record.
	dataName := filepath.Join(db.AcctsDir, SegmentDataName(throughSlot, fileId))
	// File IDs are durably monotonic. An existing path therefore signals a
	// broken high-water invariant (or an unexpected concurrent writer) and must
	// fail closed rather than truncating potentially committed account data.
	f, err := os.OpenFile(dataName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return BatchCommitResult{}, err
	}
	crc := crc32.NewIEEE()
	bw := bufio.NewWriterSize(f, 1<<20)
	segWriter := io.MultiWriter(bw, crc)
	records := make([]ManifestRecord, 0, len(keys))
	var dataLen uint64
	segErr := func() error {
		for _, k := range keys {
			v := union[k]
			records = append(records, ManifestRecord{
				Pubkey:    k,
				Offset:    dataLen,
				OwnerSlot: v.ownerSlot,
				Tombstone: v.acct.Lamports == 0,
			})
			ava := AppendVecAccount{
				DataLen:    uint64(len(v.acct.Data)),
				Pubkey:     v.acct.Key,
				Lamports:   v.acct.Lamports,
				RentEpoch:  v.acct.RentEpoch,
				Owner:      v.acct.Owner,
				Executable: v.acct.Executable,
				Data:       v.acct.Data,
			}
			n, err := ava.MarshalReturningLength(segWriter)
			if err != nil {
				return fmt.Errorf("accountsdb: encode segment record: %w", err)
			}
			dataLen += uint64(n)
		}
		return bw.Flush()
	}()
	if segErr != nil {
		f.Close()
		return BatchCommitResult{}, segErr
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return BatchCommitResult{}, err
	}
	if err := f.Close(); err != nil {
		return BatchCommitResult{}, err
	}
	if err := fsyncDir(db.AcctsDir); err != nil {
		return BatchCommitResult{}, err
	}
	fire(db.foldHooks.afterSegmentFsync)
	dataCRC := crc.Sum32()

	// (4) Undo pointers: the index entry each key had before this batch. Bloom
	// filters make misses cheap; bounded parallelism keeps fold latency down.
	waitForPrevLookup()
	prevs := make([]ManifestRecord, len(records))
	copy(prevs, records)
	if prevLookup.err != nil {
		return BatchCommitResult{}, fmt.Errorf("accountsdb: capture undo pointers: %w", prevLookup.err)
	}
	for i := range prevs {
		if prevLookup.found[i] {
			prevs[i].PrevValid = true
			prevs[i].Prev = prevLookup.entries[i]
		}
	}

	// (5) Manifest: once this rename is durable the commit is decided.
	batchSeq := db.lastBatchSeq + 1
	manifest := &SegmentManifest{
		Version:     segManifestVersion,
		Kind:        ManifestKindFold,
		BatchSeq:    batchSeq,
		FromSlot:    fromSlot,
		ThroughSlot: throughSlot,
		FileId:      fileId,
		DataLen:     dataLen,
		DataCRC:     dataCRC,
		Bankhashes:  sortedBankhashes(bankhashes),
		Records:     prevs,
		ResumeCtx:   resumeCtx,
	}
	if err := WriteSegmentManifest(db.AcctsDir, manifest); err != nil {
		if segmentManifestWasRenamed(err) {
			return BatchCommitResult{}, db.failDecidedFold(
				batchSeq,
				throughSlot,
				"manifest-directory-sync",
				err,
			)
		}
		return BatchCommitResult{}, err
	}
	fire(db.foldHooks.afterManifestRename)

	// (6) Advisory bankhash rows; recoverable from the manifest, so NoSync.
	for slot, bh := range bankhashes {
		var slotBytes [8]byte
		binary.LittleEndian.PutUint64(slotBytes[:], slot)
		if err := db.BankHashStore.Set(slotBytes[:], bh[:], pebble.NoSync); err != nil {
			// Advisory rows are reconstructed from the already-durable
			// manifest. Aborting here would invite a duplicate BatchSeq retry
			// even though the commit decision has already been made.
			mlog.Log.Warnf(
				"accountsdb: defer bankhash row for slot %d to recovery after advisory write failed: %v",
				slot,
				err,
			)
		}
	}

	// Prepare the cache refresh before entering the short publication section.
	live := make([]*accounts.Account, 0, len(union))
	for _, k := range keys {
		live = append(live, union[k].acct)
	}

	// (7) Publish the immutable changed-key view and advance the cache epoch
	// under a short lock. While pendingFold is installed, readers resolve every
	// changed key from the new union; unchanged keys are identical on both sides
	// of the atomic index commit. This lets the expensive index fsync and cache
	// refresh proceed without blocking account loaders.
	fire(db.foldHooks.beforeIndexCommit)
	db.readCacheEpochMu.Lock()
	db.pendingFold = union
	db.readCacheEpoch++
	db.readCacheEpochMu.Unlock()
	fire(db.foldHooks.afterPublicationStart)

	if err := db.applyManifestToIndex(manifest); err != nil {
		// Keep pendingFold installed: it is the only complete read view if one
		// or more bounded frames landed before the failure. The manifest is
		// already the durable decision, so this process must fail closed and let
		// startup idempotently finish it rather than attempt another live fold.
		return BatchCommitResult{}, db.failDecidedFold(batchSeq, throughSlot, "account-index", err)
	}

	// Old readers captured the preceding epoch and therefore cannot publish
	// stale values. New readers use pendingFold for changed keys until these
	// ordinary concurrent-safe cache operations finish.
	db.refreshReadCacheEntries(live)

	db.readCacheEpochMu.Lock()
	db.pendingFold = nil
	db.readCacheEpochMu.Unlock()
	fire(db.foldHooks.afterIndexCommit)

	// (8) Publish.
	db.lastBatchSeq = batchSeq
	db.durableThrough.Store(throughSlot)
	frames := db.accountIndexMutationFrameCount(uint64(len(union)))
	db.foldCommits.Add(1)
	db.foldWALFrames.Add(frames)
	if frames > 1 {
		db.foldOversized.Add(1)
	}
	updateAtomicMaximum(&db.foldMaxKeys, uint64(len(union)))

	return BatchCommitResult{
		BatchSeq:    batchSeq,
		FileId:      fileId,
		ThroughSlot: throughSlot,
		Keys:        len(records),
		Bytes:       int64(dataLen),
	}, nil
}

// failDecidedFold permanently fences this AccountsDb instance. Caller holds
// foldMu. A fresh process must inspect the manifest path and idempotently
// recover (or discard an unpersisted rename) before writes or reads resume.
func (db *AccountsDb) failDecidedFold(
	batchSeq uint64,
	throughSlot uint64,
	stage string,
	cause error,
) error {
	decided := foldCommitDecidedError(batchSeq, throughSlot, stage, cause)
	db.foldFatal = decided
	if db.ProductionIndex != nil {
		_ = db.ProductionIndex.setPoison(decided)
	}
	return decided
}

// applyManifestToIndex installs a manifest's index entries and fold watermark.
// Normal replay planning aims for one CRC-protected frame, but a single valid
// oversized slot can require several. Bounded idempotent data frames precede
// the final fold-meta commit marker, and startup retries the entire decided
// manifest after a crash.
func (db *AccountsDb) applyManifestToIndex(m *SegmentManifest) error {
	mutations := make([]deltaIndexMutation, 0, len(m.Records))
	for i := range m.Records {
		r := &m.Records[i]
		if r.Tombstone {
			mutations = append(mutations, tombstoneDeltaMutation(r.Pubkey))
			continue
		}
		entry := AccountIndexEntry{Slot: m.ThroughSlot, FileId: m.FileId, Offset: r.Offset}
		mutations = append(mutations, liveDeltaMutation(r.Pubkey, entry))
	}
	meta := foldMeta{
		BatchSeq:    m.BatchSeq,
		ThroughSlot: m.ThroughSlot,
		FileId:      m.FileId,
	}
	return db.applyAccountIndexMutationTransaction(mutations, &meta)
}

func sortedBankhashes(bankhashes map[uint64][32]byte) []SlotBankhash {
	out := make([]SlotBankhash, 0, len(bankhashes))
	for slot, bh := range bankhashes {
		out = append(out, SlotBankhash{Slot: slot, Bankhash: bh})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}
