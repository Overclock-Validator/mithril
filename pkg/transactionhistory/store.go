package transactionhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

var (
	// ErrUnavailable means no complete, internally consistent result is available.
	ErrUnavailable = errors.New("transaction history unavailable")
	// ErrLimit means an application storage or query ceiling was exceeded.
	ErrLimit = errors.New("transaction history limit exceeded")
	// ErrConflict rejects a different immutable block under an existing identity.
	ErrConflict = errors.New("transaction history block identity conflict")
	// ErrClosed reports an already closed store or snapshot.
	ErrClosed = errors.New("transaction history closed")
	// ErrSnapshotsOpen prevents closing the engine beneath an active reader.
	ErrSnapshotsOpen = errors.New("transaction history snapshots are open")
)

var namespaceKey = []byte("namespace")

// Store owns a separate WAL-enabled Pebble database. Its fatal storage failure
// policy is Pebble's default: it can terminate the hosting process. The caller
// must not enable this optional store on a voting node under this contract.
type Store struct {
	// ponytail: one synchronous writer gate; add acknowledgement generations
	// only if measured contention requires concurrent commits.
	mu        sync.Mutex
	db        *pebble.DB
	lock      *pebble.Lock
	limits    Limits
	poison    error
	closed    bool
	snapshots int
	commit    func(*pebble.Batch) error
	fs        vfs.FS
	dir       string
	retention *Retention
	blocks    uint64
	bytes     uint64
}

// Open admits only an empty directory or this exact format and genesis.
// Recovered complete records are candidates; canonical eligibility must be
// reconciled by the caller before serving them after a restart.
func Open(dir string, genesis [32]byte, limits Limits) (*Store, error) {
	return openStore(dir, genesis, limits, vfs.Default)
}

func openStore(dir string, genesis [32]byte, limits Limits, fs vfs.FS) (*Store, error) {
	return openStoreWithRetention(dir, genesis, limits, fs, nil)
}

func openStoreWithRetention(dir string, genesis [32]byte, limits Limits, fs vfs.FS, retention *Retention) (*Store, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if retention != nil {
		if err := retention.validate(limits); err != nil {
			return nil, err
		}
		if err := checkHistoryDiskSpace(fs, dir, retention.MinFreeBytes, limits.MaxBatchBytes); err != nil {
			return nil, err
		}
	}
	if err := fs.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := pebble.LockDirectory(dir, fs)
	if err != nil {
		return nil, err
	}
	db, err := openLocked(dir, genesis, fs, lock)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	s := &Store{db: db, lock: lock, limits: limits, fs: fs, dir: dir, retention: retention,
		commit: func(batch *pebble.Batch) error { return batch.Commit(pebble.Sync) }}
	if err := s.rebuildStats(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

func openLocked(dir string, genesis [32]byte, fs vfs.FS, lock *pebble.Lock) (*pebble.DB, error) {
	entries, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	empty := len(entries) == 0 || (len(entries) == 1 && entries[0] == "LOCK")
	namespace := binary.BigEndian.AppendUint32([]byte("mithril-transaction-history\x00"), formatVersion)
	namespace = append(namespace, genesis[:]...)
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	opts := &pebble.Options{FS: fs, Lock: lock, Cache: cache, MemTableSize: 4 << 20, MemTableStopWritesThreshold: 2, MaxConcurrentCompactions: func() int { return 1 }, DisableWAL: false}
	if !empty {
		// A read-only probe rejects foreign or uninitialized nonempty stores
		// without adding our namespace to them.
		opts.ReadOnly, opts.ErrorIfNotExists = true, true
		probe, err := pebble.Open(dir, opts)
		if err != nil {
			return nil, fmt.Errorf("inspect transaction history namespace: %w", err)
		}
		value, closer, readErr := probe.Get(namespaceKey)
		if readErr == nil {
			if !bytes.Equal(value, namespace) {
				readErr = errors.New("transaction history genesis or format mismatch")
			}
			readErr = errors.Join(readErr, closer.Close())
		}
		if err := errors.Join(readErr, probe.Close()); err != nil {
			return nil, err
		}
	}
	opts.ReadOnly, opts.ErrorIfNotExists, opts.ErrorIfExists = false, !empty, empty
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	if empty {
		if err := db.Set(namespaceKey, namespace, pebble.Sync); err != nil {
			return nil, errors.Join(err, db.Close())
		}
	}
	return db, nil
}

func (s *Store) ready() error {
	if s.closed {
		return ErrClosed
	}
	if s.poison != nil {
		return errors.Join(ErrUnavailable, s.poison)
	}
	return nil
}

// WriteBlock atomically writes ordered bodies, first-signature references and
// completion, returning only after synchronous acknowledgement. Callers must
// not mutate input slices until it returns. Identical retries are idempotent.
func (s *Store) WriteBlock(block Block) error {
	c, err := prepareBlock(block, s.limits)
	if err != nil {
		return err
	}
	header := encodeCompletion(c)
	headerDigest := sha256.Sum256(header)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return err
	}
	old, closer, err := s.db.Get(blockKey('c', block.ID))
	if err == nil {
		same := bytes.Equal(old, header)
		if err := closer.Close(); err != nil {
			return err
		}
		if !same {
			return ErrConflict
		}
		snapshot := s.db.NewSnapshot()
		_, scanErr := scanBlock(snapshot, block.ID, s.limits, nil)
		return errors.Join(scanErr, snapshot.Close())
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if err := s.makeRoomLocked(c); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for i, record := range block.Records {
		value := encodeRecord(record)
		digest := sha256.Sum256(value)
		var reference [64]byte
		copy(reference[:32], digest[:])
		copy(reference[32:], headerDigest[:])
		ref := Ref{Block: block.ID, Position: uint32(i), FirstSignature: record.FirstSignature}
		if err := batch.Set(transactionKey(block.ID, uint32(i)), value, nil); err != nil {
			return err
		}
		if err := batch.Set(signatureKey(ref), reference[:], nil); err != nil {
			return err
		}
	}
	if err := batch.Set(blockKey('c', block.ID), header, nil); err != nil {
		return err
	}
	if uint64(len(batch.Repr())) > s.limits.MaxBatchBytes {
		return ErrLimit
	}
	if err := s.commitLocked(batch); err != nil {
		return err
	}
	s.blocks++
	s.bytes += completionBytes(c)
	return nil
}

func (s *Store) commitLocked(batch *pebble.Batch) error {
	if err := s.checkDiskSpace(); err != nil {
		return err
	}
	if err := s.commit(batch); err != nil {
		// Visibility can precede acknowledgement, including an ambiguous error.
		// Existing snapshots remain valid; no new reads or writes are admitted.
		s.poison = err
		return errors.Join(ErrUnavailable, err)
	}
	return nil
}

// ReadSnapshot is a single consistent engine view. It does not freeze the
// caller's canonical chain view and must be closed before slow network writes.
// Callers bound concurrent views; per-operation limits do not bound pinned disk.
type ReadSnapshot struct {
	mu    sync.Mutex
	store *Store
	db    *pebble.Snapshot
}

// BlockHeader is an owned completion declaration without transaction payloads.
// Its count does not establish that the declared records are present or valid.
type BlockHeader struct {
	ID                BlockID
	ParentSlot        uint64
	PreviousBlockhash [32]byte
	Bankhash          [32]byte
	ParentBankhash    [32]byte
	CaptureVersion    uint32
	TransactionCount  uint32
}

// Snapshot waits for any in-flight synchronous write acknowledgement before
// creating its view. The caller separately coordinates canonical selection.
func (s *Store) Snapshot() (*ReadSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	s.snapshots++
	return &ReadSnapshot{store: s, db: s.db.NewSnapshot()}, nil
}

// Close releases the snapshot. Repeated calls are harmless.
func (r *ReadSnapshot) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	r.db = nil
	r.store.mu.Lock()
	r.store.snapshots--
	r.store.mu.Unlock()
	return err
}

// BlockHeader validates the completion's format, identity and configured bounds.
// It reads no bodies or indexes and makes no payload-integrity or canonicality
// claim. Transaction and Block perform their documented stronger checks.
func (r *ReadSnapshot) BlockHeader(id BlockID) (BlockHeader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return BlockHeader{}, ErrClosed
	}
	c, err := readCompletion(r.db, id, r.store.limits)
	if err != nil {
		return BlockHeader{}, err
	}
	return BlockHeader{
		ID: c.block.ID, ParentSlot: c.block.ParentSlot,
		PreviousBlockhash: c.block.PreviousBlockhash, Bankhash: c.block.Bankhash,
		ParentBankhash: c.block.ParentBankhash, CaptureVersion: c.block.CaptureVersion,
		TransactionCount: c.count,
	}, nil
}

// Block returns owned records only after validating the complete ordered block.
func (r *ReadSnapshot) Block(id BlockID) (Block, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return Block{}, ErrClosed
	}
	var records []Record
	c, err := scanBlock(r.db, id, r.store.limits, func(_ Ref, record Record) error {
		records = append(records, ownedRecord(record))
		return nil
	})
	if err != nil {
		return Block{}, err
	}
	c.block.Records = records
	return c.block, nil
}

// SignatureRefs returns all bounded candidates, never an arbitrary fork.
// Every candidate must resolve to a matching completion and body in this view.
func (r *ReadSnapshot) SignatureRefs(signature [64]byte) ([]Ref, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return nil, ErrClosed
	}
	var refs []Ref
	err := r.signatureCandidates(context.Background(), signature, nil, func(ref Ref, _ BlockHeader) {
		refs = append(refs, ref)
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

// Transaction validates the requested receipt and completion declaration, not
// the presence of unrelated bodies. Block validates the whole block. Both use
// the same immutable payload when successful; neither chooses a canonical fork.
func (r *ReadSnapshot) Transaction(ref Ref) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return Record{}, ErrClosed
	}
	return readTransaction(r.db, ref, r.store.limits, true)
}

func readCompletion(snapshot *pebble.Snapshot, id BlockID, limits Limits) (completion, error) {
	return readCompletionWithBudget(context.Background(), snapshot, id, limits, nil)
}

func checkReference(snapshot *pebble.Snapshot, ref Ref, value []byte, headerDigest [32]byte) error {
	return checkReferenceWithBudget(context.Background(), snapshot, ref, value, headerDigest, nil)
}

func checkReferenceValue(want, value []byte, headerDigest [32]byte) error {
	digest := sha256.Sum256(value)
	if len(want) != 64 || !bytes.Equal(want[:32], digest[:]) || !bytes.Equal(want[32:], headerDigest[:]) {
		return ErrUnavailable
	}
	return nil
}

func readTransaction(snapshot *pebble.Snapshot, ref Ref, limits Limits, owned bool) (Record, error) {
	record, _, err := readTransactionWithBudget(context.Background(), snapshot, ref, limits, owned, nil)
	return record, err
}

func scanBlock(snapshot *pebble.Snapshot, id BlockID, limits Limits, visit func(Ref, Record) error) (completion, error) {
	c, err := readCompletion(snapshot, id, limits)
	if err != nil {
		return c, err
	}
	prefix := blockKey('t', id)
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return c, err
	}
	h := sha256.New()
	header := encodeCompletion(c)
	headerDigest := sha256.Sum256(header)
	h.Write(header[:164])
	var count uint32
	var size uint64
	for valid := iter.First(); valid; valid = iter.Next() {
		key, value := iter.Key(), iter.Value()
		if count >= c.count || len(key) != 45 || binary.BigEndian.Uint32(key[41:]) != count || uint64(len(value)) > c.bytes-size {
			return c, errors.Join(ErrUnavailable, iter.Close())
		}
		record, err := viewRecord(value, limits)
		if err != nil {
			return c, errors.Join(err, iter.Close())
		}
		ref := Ref{Block: id, Position: count, FirstSignature: record.FirstSignature}
		if err := checkReference(snapshot, ref, value, headerDigest); err != nil {
			return c, errors.Join(err, iter.Close())
		}
		h.Write(value)
		if visit != nil {
			if err := visit(ref, record); err != nil {
				return c, errors.Join(err, iter.Close())
			}
		}
		count++
		size += uint64(len(value))
	}
	if count != c.count || size != c.bytes || !bytes.Equal(h.Sum(nil), c.digest[:]) {
		err = ErrUnavailable
	}
	return c, errors.Join(err, iter.Error(), iter.Close())
}

// PruneBefore removes at most maxBlocks complete blocks below slot. maxBlocks
// must be 1..64; callers repeat bounded calls. Each block is deleted atomically,
// with the writer gate and internal snapshot released between blocks. External
// snapshots may retain older complete blocks and delay physical reclamation.
func (s *Store) PruneBefore(slot uint64, maxBlocks uint32) (uint32, error) {
	if maxBlocks == 0 || maxBlocks > 64 {
		return 0, ErrLimit
	}
	var removed uint32
	for removed < maxBlocks {
		s.mu.Lock()
		done, err := s.pruneOneLocked(slot)
		s.mu.Unlock()
		if err != nil {
			return removed, err
		}
		if !done {
			break
		}
		removed++
	}
	return removed, nil
}

func (s *Store) pruneOneLocked(slot uint64) (bool, error) {
	// The exclusive upper bound avoids slot+1 overflow, including MaxUint64.
	upper := binary.BigEndian.AppendUint64([]byte{'c'}, slot)
	return s.pruneFirstLocked(upper)
}

func (s *Store) pruneFirstLocked(upper []byte) (bool, error) {
	if err := s.ready(); err != nil {
		return false, err
	}
	snapshot := s.db.NewSnapshot()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte{'c'}, UpperBound: upper})
	if err != nil {
		return false, errors.Join(err, snapshot.Close())
	}
	if !iter.First() {
		return false, errors.Join(iter.Error(), iter.Close(), snapshot.Close())
	}
	key := iter.Key()
	if len(key) != 41 {
		return false, errors.Join(ErrUnavailable, iter.Close(), snapshot.Close())
	}
	id := BlockID{Slot: binary.BigEndian.Uint64(key[1:9])}
	copy(id.Blockhash[:], key[9:])
	c, err := decodeCompletion(iter.Value(), id, s.limits)
	err = errors.Join(err, iter.Close())
	if err == nil {
		err = s.pruneBlockLocked(snapshot, c)
	}
	return err == nil, errors.Join(err, snapshot.Close())
}

func (s *Store) pruneBlockLocked(snapshot *pebble.Snapshot, c completion) error {
	if s.blocks == 0 || completionBytes(c) > s.bytes {
		return ErrUnavailable
	}
	deleteBytes := 12 + uint64(c.count)*(deleteSize(45)+deleteSize(109)) + deleteSize(41)
	if deleteBytes > s.limits.MaxBatchBytes {
		return ErrLimit
	}
	if err := s.checkDiskSpace(); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	_, err := scanBlock(snapshot, c.block.ID, s.limits, func(ref Ref, _ Record) error {
		if err := batch.Delete(transactionKey(ref.Block, ref.Position), nil); err != nil {
			return err
		}
		return batch.Delete(signatureKey(ref), nil)
	})
	if err == nil {
		err = batch.Delete(blockKey('c', c.block.ID), nil)
	}
	if err == nil && uint64(len(batch.Repr())) > s.limits.MaxBatchBytes {
		err = ErrLimit
	}
	if err == nil {
		err = s.commitLocked(batch)
		if err == nil {
			s.blocks--
			s.bytes -= completionBytes(c)
		}
	}
	return errors.Join(err, batch.Close())
}

// Close refuses to close the engine while snapshots are outstanding. It never
// waits for their owners, and repeated successful closes are harmless.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.snapshots != 0 {
		return ErrSnapshotsOpen
	}
	s.closed = true
	return errors.Join(s.db.Close(), s.lock.Close())
}
