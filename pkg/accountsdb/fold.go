package accountsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/errgroup"
)

// metaKeyLastBatch is the Pebble index key holding the fold commit watermark
// {batchSeq, throughSlot, fileId}. Its length (!= 32) cannot collide with an
// account pubkey key. Writing it in the same batch as the index entries is the
// atomic "index epoch flip" that decides batch visibility.
var metaKeyLastBatch = []byte("\x00mithril.meta.last_batch")

type foldMeta struct {
	BatchSeq    uint64
	ThroughSlot uint64
	FileId      uint64
}

func encodeFoldMeta(m foldMeta) []byte {
	var b [24]byte
	binary.LittleEndian.PutUint64(b[0:8], m.BatchSeq)
	binary.LittleEndian.PutUint64(b[8:16], m.ThroughSlot)
	binary.LittleEndian.PutUint64(b[16:24], m.FileId)
	return b[:]
}

func decodeFoldMeta(data []byte) (foldMeta, error) {
	if len(data) < 24 {
		return foldMeta{}, fmt.Errorf("accountsdb: fold meta record has %d < 24 bytes", len(data))
	}
	return foldMeta{
		BatchSeq:    binary.LittleEndian.Uint64(data[0:8]),
		ThroughSlot: binary.LittleEndian.Uint64(data[8:16]),
		FileId:      binary.LittleEndian.Uint64(data[16:24]),
	}, nil
}

// readFoldMeta returns the current fold watermark, or (zero, false) on a fresh
// (never-folded) store.
func (db *AccountsDb) readFoldMeta() (foldMeta, bool, error) {
	val, closer, err := db.Index.Get(metaKeyLastBatch)
	if err != nil {
		if err == pebble.ErrNotFound {
			return foldMeta{}, false, nil
		}
		return foldMeta{}, false, err
	}
	meta, derr := decodeFoldMeta(val)
	closer.Close()
	if derr != nil {
		return foldMeta{}, false, derr
	}
	return meta, true, nil
}

// foldTestHooks fire between CommitBatch stages so tests can inject crashes
// (each hook may panic) at every point of the crash matrix.
type foldTestHooks struct {
	afterSegmentFsync   func()
	afterManifestRename func()
	beforeIndexCommit   func()
	afterIndexCommit    func()
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
//  7. commit index entries + fold meta in one Pebble batch (the epoch flip)
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
	db.foldMu.Lock()
	defer db.foldMu.Unlock()

	// (1) Union-dedupe, newest wins. Deltas MUST arrive strictly ascending by
	// slot: newest-wins depends on iterating in order so a later slot's version
	// overwrites an earlier one. Validate it so a future fork-aware caller can
	// never silently fold stale state from mis-ordered deltas.
	fromSlot := uint64(0)
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
	fileId := db.LargestFileId.Add(1)
	if err := db.persistLargestFileId(); err != nil {
		return BatchCommitResult{}, err
	}

	// (3) Stream each deduped record straight to the segment file (buffered),
	// updating a running CRC and byte count as we go, so the fully serialized
	// segment is never materialized in RAM — only the bufio buffer is. Record
	// offsets are the byte position before each record.
	dataName := filepath.Join(db.AcctsDir, SegmentDataName(throughSlot, fileId))
	f, err := os.OpenFile(dataName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
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
			records = append(records, ManifestRecord{Pubkey: k, Offset: dataLen, OwnerSlot: v.ownerSlot})
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
	prevs := make([]ManifestRecord, len(records))
	copy(prevs, records)
	var g errgroup.Group
	g.SetLimit(max(4, runtime.NumCPU()/2))
	for i := range prevs {
		g.Go(func() error {
			val, closer, err := db.Index.Get(prevs[i].Pubkey[:])
			if err != nil {
				if err == pebble.ErrNotFound {
					return nil // PrevValid stays false
				}
				return err
			}
			entry, derr := UnmarshalAcctIdxEntry(val)
			closer.Close()
			if derr != nil {
				return derr
			}
			prevs[i].PrevValid = true
			prevs[i].Prev = *entry
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return BatchCommitResult{}, fmt.Errorf("accountsdb: capture undo pointers: %w", err)
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
		return BatchCommitResult{}, err
	}
	fire(db.foldHooks.afterManifestRename)

	// (6) Advisory bankhash rows; recoverable from the manifest, so NoSync.
	for slot, bh := range bankhashes {
		var slotBytes [8]byte
		binary.LittleEndian.PutUint64(slotBytes[:], slot)
		if err := db.BankHashStore.Set(slotBytes[:], bh[:], pebble.NoSync); err != nil {
			return BatchCommitResult{}, err
		}
	}

	// (7) The index epoch flip: entries + meta in one batch.
	fire(db.foldHooks.beforeIndexCommit)
	if err := db.applyManifestToIndex(manifest); err != nil {
		return BatchCommitResult{}, err
	}
	fire(db.foldHooks.afterIndexCommit)

	// (8) Publish.
	live := make([]*accounts.Account, 0, len(union))
	for _, k := range keys {
		live = append(live, union[k].acct)
	}
	db.refreshReadCaches(live)
	db.lastBatchSeq = batchSeq
	db.durableThrough.Store(throughSlot)

	return BatchCommitResult{
		BatchSeq:    batchSeq,
		FileId:      fileId,
		ThroughSlot: throughSlot,
		Keys:        len(records),
		Bytes:       int64(dataLen),
	}, nil
}

// applyManifestToIndex installs a manifest's index entries + the fold meta in
// one Pebble batch. Idempotent: re-applying an already-applied manifest writes
// identical values. Sync honors the WAL mode (a WAL-less index recovers its
// tail by replaying manifests instead).
func (db *AccountsDb) applyManifestToIndex(m *SegmentManifest) error {
	batch := db.Index.NewBatch()
	defer batch.Close()
	var idxBuf [24]byte
	for i := range m.Records {
		r := &m.Records[i]
		entry := AccountIndexEntry{Slot: m.ThroughSlot, FileId: m.FileId, Offset: r.Offset}
		entry.Marshal(&idxBuf)
		if err := batch.Set(r.Pubkey[:], idxBuf[:], nil); err != nil {
			return err
		}
	}
	if err := batch.Set(metaKeyLastBatch, encodeFoldMeta(foldMeta{
		BatchSeq:    m.BatchSeq,
		ThroughSlot: m.ThroughSlot,
		FileId:      m.FileId,
	}), nil); err != nil {
		return err
	}
	opts := pebble.Sync
	if db.IndexWALDisabled {
		opts = pebble.NoSync
	}
	return batch.Commit(opts)
}

func sortedBankhashes(bankhashes map[uint64][32]byte) []SlotBankhash {
	out := make([]SlotBankhash, 0, len(bankhashes))
	for slot, bh := range bankhashes {
		out = append(out, SlotBankhash{Slot: slot, Bankhash: bh})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}
