package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

const (
	// DeltaIndexJournalFileName is the only mutable account-index file. The
	// journal is replayed into an exact in-memory table at startup; point reads
	// never perform file I/O.
	DeltaIndexJournalFileName = "accounts_delta.journal"
	// DeltaIndexJournalRewriteFileName is a crash-leftover temporary journal.
	// It is never opened as canonical state and is safe for snapshot cleanup.
	DeltaIndexJournalRewriteFileName = DeltaIndexJournalFileName + ".rewrite.partial"

	deltaJournalVersion         = uint32(1)
	deltaJournalHeaderSize      = 32
	deltaFrameHeaderSize        = 64
	deltaMutationSize           = 64
	deltaMaxFrameBytes          = uint64(1 << 30)
	deltaStateFrameMaxMutations = 1 << 20

	deltaFrameHasFoldMeta = uint32(1 << 0)
	deltaFrameIsState     = uint32(1 << 1)

	deltaMutationLive      = uint8(1)
	deltaMutationTombstone = uint8(2)
	deltaMutationRetire    = uint8(3)

	defaultDeltaCheckpointMinKeys       = 4 << 20
	defaultDeltaCheckpointMaxKeys       = 32 << 20
	defaultDeltaCheckpointMaxActiveKeys = 4 << 20
	defaultDeltaCheckpointJournalBytes  = uint64(512 << 20)
	deltaReplayActiveGenerations        = 2
)

var (
	deltaJournalMagic = [8]byte{'M', 'I', 'T', 'H', 'D', 'J', '0', '1'}
	deltaFrameMagic   = [8]byte{'M', 'I', 'T', 'H', 'D', 'F', '0', '1'}
	deltaCRC          = crc32.MakeTable(crc32.Castagnoli)

	// Mutable for focused tests and deployment tuning before OpenDb. Each open
	// index snapshots the values, so later changes do not race a running node.
	DeltaCheckpointMinKeys = defaultDeltaCheckpointMinKeys
	// DeltaCheckpointMaxKeys bounds the exact 64-byte/key checkpoint so a
	// billion-account base remains inside the validator's memory envelope. A
	// checkpoint that would exceed it fails closed and requires a fresh
	// snapshot/base rebase before canonical writes can continue.
	DeltaCheckpointMaxKeys = defaultDeltaCheckpointMaxKeys
	// DeltaCheckpointMaxActiveKeys bounds writes accumulated while a frozen
	// generation is being built. Writers briefly wait for that build once the
	// bound is reached; account readers remain lock-free with respect to it.
	DeltaCheckpointMaxActiveKeys = defaultDeltaCheckpointMaxActiveKeys
	DeltaCheckpointJournalBytes  = defaultDeltaCheckpointJournalBytes

	// ErrMutableIndexActiveCapacity is returned before journal publication when
	// one mutation epoch cannot fit in a fresh bounded active generation.
	ErrMutableIndexActiveCapacity = errors.New("accountsdb: mutable account-index active generation capacity exhausted")
)

type deltaIndexValue struct {
	Entry     AccountIndexEntry
	Tombstone bool
}

type deltaIndexMutation struct {
	Key   solana.PublicKey
	Value deltaIndexValue
	Kind  uint8
}

type retiredAppendVec struct {
	Slot   uint64
	FileID uint64
}

// MutableAccountIndex is the exact, write-optimized layer above immutable
// StreamHash generations. Mutations are serialized into CRC-protected frames
// and fdatasync'd before a new table view is published. A single RWMutex makes
// a multi-key read snapshot and a whole mutation frame mutually atomic.
//
// The active table is intentionally exact: tombstones cannot be represented
// probabilistically because a false tombstone could hide a live older value.
type MutableAccountIndex struct {
	mu      sync.RWMutex
	entries map[solana.PublicKey]deltaIndexValue
	frozen  map[solana.PublicKey]deltaIndexValue
	retired map[retiredAppendVec]struct{}
	meta    foldMeta
	hasMeta bool

	checkpoint              *DeltaCheckpoint
	checkpointBuilding      bool
	checkpointDone          chan struct{}
	checkpointErr           error
	frozenSeq               uint64
	uncheckpointedBytes     uint64
	checkpointMinKeys       int
	checkpointMaxKeys       int
	checkpointMaxActiveKeys int
	checkpointReplayMaxKeys int
	checkpointJournalBytes  uint64
	checkpointCtx           context.Context
	cancelCheckpointBuild   context.CancelFunc
	checkpointBuildWait     sync.WaitGroup
	closing                 bool

	writeMu sync.Mutex
	file    *os.File
	path    string
	seq     uint64
	offset  int64
	closed  bool
	poison  error
}

type MutableAccountIndexStats struct {
	ActiveKeys              uint64
	FrozenKeys              uint64
	CheckpointKeys          uint64
	CheckpointCoveredSeq    uint64
	CheckpointMaxKeys       uint64
	CheckpointMaxActiveKeys uint64
	ReplayMaxKeys           uint64
	JournalSequence         uint64
	JournalBytes            uint64
	UncheckpointedBytes     uint64
	CheckpointBuilding      bool
}

// MutableAccountIndexSnapshot pins one exact table epoch. Close must be called.
// A snapshot is single-owner: Lookup and Close must not be invoked concurrently.
type MutableAccountIndexSnapshot struct {
	index  *MutableAccountIndex
	closed bool
}

func OpenMutableAccountIndex(accountsDbDir string) (*MutableAccountIndex, error) {
	path := filepath.Join(accountsDbDir, DeltaIndexJournalFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open mutable account-index journal: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("accountsdb: lock mutable account-index journal (another process may be using it): %w", err)
	}
	maxKeys := max(1, DeltaCheckpointMaxKeys)
	maxActiveKeys := max(1, DeltaCheckpointMaxActiveKeys)
	idx := &MutableAccountIndex{
		entries:                 make(map[solana.PublicKey]deltaIndexValue),
		retired:                 make(map[retiredAppendVec]struct{}),
		file:                    f,
		path:                    path,
		checkpointMinKeys:       max(1, min(DeltaCheckpointMinKeys, maxKeys, maxActiveKeys)),
		checkpointMaxKeys:       maxKeys,
		checkpointMaxActiveKeys: maxActiveKeys,
		checkpointReplayMaxKeys: replayActiveKeyLimit(maxActiveKeys),
		checkpointJournalBytes:  max(uint64(1), DeltaCheckpointJournalBytes),
	}
	idx.checkpointCtx, idx.cancelCheckpointBuild = context.WithCancel(context.Background())
	idx.checkpoint, err = OpenLatestDeltaCheckpoint(accountsDbDir)
	if err != nil {
		idx.cancelCheckpointBuild()
		_ = f.Close()
		return nil, err
	}
	if err := idx.openAndReplay(); err != nil {
		idx.cancelCheckpointBuild()
		if idx.checkpoint != nil {
			_ = idx.checkpoint.Close()
		}
		_ = f.Close()
		return nil, err
	}
	// A crash during checkpoint construction can leave the frozen and active
	// generations together. Start recovery checkpointing promptly when that
	// replayed state reaches the normal key/byte threshold; small compact tails
	// remain hot rather than pointlessly rebuilding a large existing checkpoint.
	idx.mu.Lock()
	idx.maybeFreezeLocked()
	idx.mu.Unlock()
	return idx, nil
}

func replayActiveKeyLimit(maxActiveKeys int) int {
	maxInt := int(^uint(0) >> 1)
	if maxActiveKeys > maxInt/deltaReplayActiveGenerations {
		return maxInt
	}
	return maxActiveKeys * deltaReplayActiveGenerations
}

// InitializeMutableAccountIndex creates the initial durable empty journal.
// Snapshot bootstrap is the only normal caller. Runtime open is deliberately
// non-creating: losing this file after fold/compaction history has been pruned
// is not recoverable and must never be mistaken for a fresh account index.
func InitializeMutableAccountIndex(accountsDbDir string) (retErr error) {
	checkpoint, err := OpenLatestDeltaCheckpoint(accountsDbDir)
	if err != nil {
		return err
	}
	if checkpoint != nil {
		generation, coveredSeq := checkpoint.Generation(), checkpoint.CoveredSeq()
		_ = checkpoint.Close()
		return fmt.Errorf(
			"accountsdb: refusing to initialize mutable journal over delta checkpoint generation %d at sequence %d",
			generation, coveredSeq,
		)
	}

	path := filepath.Join(accountsDbDir, DeltaIndexJournalFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create initial mutable account-index journal: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if err := writeFullAt(file, encodeDeltaJournalHeader(0, 0), 0); err != nil {
		return fmt.Errorf("accountsdb: write initial mutable account-index journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync initial mutable account-index journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("accountsdb: close initial mutable account-index journal: %w", err)
	}
	if err := fsyncDir(accountsDbDir); err != nil {
		return fmt.Errorf("accountsdb: sync initial mutable account-index directory: %w", err)
	}
	keep = true
	return nil
}

func (idx *MutableAccountIndex) openAndReplay() error {
	info, err := idx.file.Stat()
	if err != nil {
		return fmt.Errorf("accountsdb: stat mutable account-index journal: %w", err)
	}
	if info.Size() == 0 {
		return errors.New("accountsdb: mutable account-index journal is empty; bootstrap from a fresh snapshot")
	}
	if info.Size() < deltaJournalHeaderSize {
		return fmt.Errorf("accountsdb: mutable account-index journal is %d bytes, shorter than its header", info.Size())
	}
	header := make([]byte, deltaJournalHeaderSize)
	if _, err := idx.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("accountsdb: read mutable account-index journal header: %w", err)
	}
	if !bytes.Equal(header[:8], deltaJournalMagic[:]) {
		return fmt.Errorf("accountsdb: invalid mutable account-index journal magic %x", header[:8])
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != deltaJournalVersion {
		return fmt.Errorf("accountsdb: unsupported mutable account-index journal version %d", version)
	}
	if size := binary.LittleEndian.Uint32(header[12:16]); size != deltaJournalHeaderSize {
		return fmt.Errorf("accountsdb: invalid mutable account-index journal header size %d", size)
	}
	wantHeaderCRC := binary.LittleEndian.Uint32(header[28:32])
	if got := crc32.Checksum(header[:28], deltaCRC); got != wantHeaderCRC {
		return fmt.Errorf("accountsdb: mutable account-index journal header CRC mismatch: got %08x want %08x", got, wantHeaderCRC)
	}

	offset := int64(deltaJournalHeaderSize)
	baseSeq := binary.LittleEndian.Uint64(header[16:24])
	stateFrameCount := binary.LittleEndian.Uint32(header[24:28])
	lastSeq := baseSeq
	if stateFrameCount != 0 && baseSeq == 0 {
		return errors.New("accountsdb: mutable-index journal declares compact state without a checkpoint base")
	}
	if baseSeq != 0 && idx.checkpoint == nil {
		return fmt.Errorf(
			"accountsdb: mutable-index journal begins at checkpoint sequence %d but no delta checkpoint is published",
			baseSeq,
		)
	}
	if idx.checkpoint != nil && idx.checkpoint.CoveredSeq() < baseSeq {
		return fmt.Errorf(
			"accountsdb: delta checkpoint covers sequence %d before compact journal base %d",
			idx.checkpoint.CoveredSeq(), baseSeq,
		)
	}
	frameHeader := make([]byte, deltaFrameHeaderSize)
	stateFramesSeen := uint32(0)
	for offset < info.Size() {
		remaining := info.Size() - offset
		if remaining < deltaFrameHeaderSize {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf("accountsdb: compact mutable-index state is torn after %d of %d frames", stateFramesSeen, stateFrameCount)
			}
			if err := truncateAndSync(idx.file, offset); err != nil {
				return fmt.Errorf("accountsdb: truncate torn mutable-index frame header: %w", err)
			}
			break
		}
		if _, err := idx.file.ReadAt(frameHeader, offset); err != nil {
			return fmt.Errorf("accountsdb: read mutable-index frame header at %d: %w", offset, err)
		}
		if !bytes.Equal(frameHeader[:8], deltaFrameMagic[:]) {
			return fmt.Errorf("accountsdb: invalid mutable-index frame magic at %d", offset)
		}
		if version := binary.LittleEndian.Uint32(frameHeader[8:12]); version != deltaJournalVersion {
			return fmt.Errorf("accountsdb: unsupported mutable-index frame version %d at %d", version, offset)
		}
		frameLen := binary.LittleEndian.Uint64(frameHeader[16:24])
		count := binary.LittleEndian.Uint32(frameHeader[56:60])
		wantLen := uint64(deltaFrameHeaderSize) + uint64(count)*deltaMutationSize
		if frameLen != wantLen || frameLen > deltaMaxFrameBytes {
			return fmt.Errorf("accountsdb: invalid mutable-index frame length %d for %d mutations at %d", frameLen, count, offset)
		}
		if frameLen > uint64(remaining) {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf("accountsdb: compact mutable-index state frame %d of %d is torn", stateFramesSeen+1, stateFrameCount)
			}
			if err := truncateAndSync(idx.file, offset); err != nil {
				return fmt.Errorf("accountsdb: truncate torn mutable-index frame at %d: %w", offset, err)
			}
			break
		}
		payload := make([]byte, int(frameLen)-deltaFrameHeaderSize)
		if len(payload) > 0 {
			if _, err := idx.file.ReadAt(payload, offset+deltaFrameHeaderSize); err != nil {
				return fmt.Errorf("accountsdb: read mutable-index frame payload at %d: %w", offset, err)
			}
		}
		wantCRC := binary.LittleEndian.Uint32(frameHeader[60:64])
		crc := crc32.Update(0, deltaCRC, frameHeader[:60])
		crc = crc32.Update(crc, deltaCRC, payload)
		if crc != wantCRC {
			return fmt.Errorf("accountsdb: mutable-index frame CRC mismatch at %d: got %08x want %08x", offset, crc, wantCRC)
		}
		seq := binary.LittleEndian.Uint64(frameHeader[24:32])
		if lastSeq == ^uint64(0) {
			return errors.New("accountsdb: mutable-index journal sequence overflow")
		}
		if seq != lastSeq+1 {
			return fmt.Errorf("accountsdb: mutable-index frame sequence %d does not follow %d", seq, lastSeq)
		}
		mutations, err := decodeDeltaMutations(payload, count)
		if err != nil {
			return fmt.Errorf("accountsdb: decode mutable-index frame %d: %w", seq, err)
		}
		flags := binary.LittleEndian.Uint32(frameHeader[12:16])
		if flags&^(deltaFrameHasFoldMeta|deltaFrameIsState) != 0 {
			return fmt.Errorf("accountsdb: mutable-index frame %d has unknown flags %#x", seq, flags)
		}
		expectState := stateFramesSeen < stateFrameCount
		isState := flags&deltaFrameIsState != 0
		if isState != expectState {
			return fmt.Errorf(
				"accountsdb: mutable-index frame %d state flag=%t, want %t for compact-state frame %d of %d",
				seq, isState, expectState, stateFramesSeen+1, stateFrameCount,
			)
		}
		if isState {
			stateFramesSeen++
		}
		coveredSeq := uint64(0)
		if idx.checkpoint != nil {
			coveredSeq = idx.checkpoint.CoveredSeq()
		}
		if seq <= coveredSeq {
			// Checkpoint records already contain every live/tombstone mutation
			// through this sequence. Retirement and fold-frontier metadata are
			// deliberately replayed because they are not part of the key run.
			for i := range mutations {
				if mutations[i].Kind == deltaMutationRetire {
					idx.applyInMemory(mutations[i : i+1])
				}
			}
		} else {
			if err := idx.checkReplayCapacity(mutations, seq); err != nil {
				return err
			}
			idx.applyInMemory(mutations)
			if flags&deltaFrameIsState == 0 {
				idx.uncheckpointedBytes += frameLen
			}
		}
		if flags&deltaFrameHasFoldMeta != 0 {
			idx.meta = foldMeta{
				BatchSeq:    binary.LittleEndian.Uint64(frameHeader[32:40]),
				ThroughSlot: binary.LittleEndian.Uint64(frameHeader[40:48]),
				FileId:      binary.LittleEndian.Uint64(frameHeader[48:56]),
			}
			idx.hasMeta = true
		}
		lastSeq = seq
		offset += int64(frameLen)
	}
	if stateFramesSeen != stateFrameCount {
		return fmt.Errorf("accountsdb: compact mutable-index state has %d of %d frames", stateFramesSeen, stateFrameCount)
	}
	idx.seq = lastSeq
	idx.offset = offset
	if idx.checkpoint != nil && idx.checkpoint.CoveredSeq() > lastSeq {
		return fmt.Errorf(
			"accountsdb: delta checkpoint covers journal sequence %d beyond journal tail %d",
			idx.checkpoint.CoveredSeq(), lastSeq,
		)
	}
	return nil
}

func (idx *MutableAccountIndex) checkReplayCapacity(mutations []deltaIndexMutation, seq uint64) error {
	if len(idx.entries) <= idx.checkpointReplayMaxKeys &&
		len(mutations) <= idx.checkpointReplayMaxKeys-len(idx.entries) {
		return nil
	}
	_, newKeys, incomingOverflow := deltaMutationKeyGrowth(idx.entries, mutations, idx.checkpointReplayMaxKeys)
	if incomingOverflow || len(idx.entries) > idx.checkpointReplayMaxKeys-newKeys {
		return fmt.Errorf(
			"%w: replay frame %d would exceed %d exact uncheckpointed keys; bootstrap from a fresh snapshot",
			ErrMutableIndexActiveCapacity, seq, idx.checkpointReplayMaxKeys,
		)
	}
	return nil
}

func liveDeltaMutation(key solana.PublicKey, entry AccountIndexEntry) deltaIndexMutation {
	return deltaIndexMutation{Key: key, Value: deltaIndexValue{Entry: entry}, Kind: deltaMutationLive}
}

func encodeDeltaJournalHeader(baseSeq uint64, stateFrameCount uint32) []byte {
	header := make([]byte, deltaJournalHeaderSize)
	copy(header[:8], deltaJournalMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], deltaJournalVersion)
	binary.LittleEndian.PutUint32(header[12:16], deltaJournalHeaderSize)
	binary.LittleEndian.PutUint64(header[16:24], baseSeq)
	binary.LittleEndian.PutUint32(header[24:28], stateFrameCount)
	binary.LittleEndian.PutUint32(header[28:32], crc32.Checksum(header[:28], deltaCRC))
	return header
}

func tombstoneDeltaMutation(key solana.PublicKey) deltaIndexMutation {
	return deltaIndexMutation{Key: key, Value: deltaIndexValue{Tombstone: true}, Kind: deltaMutationTombstone}
}

func retireDeltaMutation(slot, fileID uint64) deltaIndexMutation {
	return deltaIndexMutation{
		Value: deltaIndexValue{Entry: AccountIndexEntry{Slot: slot, FileId: fileID}},
		Kind:  deltaMutationRetire,
	}
}

func encodeDeltaMutations(mutations []deltaIndexMutation) ([]byte, error) {
	if uint64(len(mutations)) > (deltaMaxFrameBytes-deltaFrameHeaderSize)/deltaMutationSize {
		return nil, fmt.Errorf("accountsdb: mutable-index frame has too many mutations: %d", len(mutations))
	}
	payload := make([]byte, len(mutations)*deltaMutationSize)
	for i := range mutations {
		mutation := mutations[i]
		record := payload[i*deltaMutationSize : (i+1)*deltaMutationSize]
		copy(record[:32], mutation.Key[:])
		binary.LittleEndian.PutUint64(record[32:40], mutation.Value.Entry.Slot)
		binary.LittleEndian.PutUint64(record[40:48], mutation.Value.Entry.FileId)
		binary.LittleEndian.PutUint64(record[48:56], mutation.Value.Entry.Offset)
		record[56] = mutation.Kind
		switch mutation.Kind {
		case deltaMutationLive:
			if mutation.Value.Tombstone {
				return nil, fmt.Errorf("accountsdb: live mutable-index mutation for %s is marked tombstone", mutation.Key)
			}
		case deltaMutationTombstone:
			if !mutation.Value.Tombstone {
				return nil, fmt.Errorf("accountsdb: tombstone mutable-index mutation for %s is not marked tombstone", mutation.Key)
			}
		case deltaMutationRetire:
			if mutation.Key != (solana.PublicKey{}) {
				return nil, errors.New("accountsdb: retired appendvec mutation has a non-zero pubkey")
			}
		default:
			return nil, fmt.Errorf("accountsdb: invalid mutable-index mutation kind %d", mutation.Kind)
		}
	}
	return payload, nil
}

func encodeDeltaFrame(seq uint64, mutations []deltaIndexMutation, meta *foldMeta, state bool) ([]byte, error) {
	payload, err := encodeDeltaMutations(mutations)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, deltaFrameHeaderSize+len(payload))
	copy(frame[:8], deltaFrameMagic[:])
	binary.LittleEndian.PutUint32(frame[8:12], deltaJournalVersion)
	var flags uint32
	if meta != nil {
		flags |= deltaFrameHasFoldMeta
		binary.LittleEndian.PutUint64(frame[32:40], meta.BatchSeq)
		binary.LittleEndian.PutUint64(frame[40:48], meta.ThroughSlot)
		binary.LittleEndian.PutUint64(frame[48:56], meta.FileId)
	}
	if state {
		flags |= deltaFrameIsState
	}
	binary.LittleEndian.PutUint32(frame[12:16], flags)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(len(frame)))
	binary.LittleEndian.PutUint64(frame[24:32], seq)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(len(mutations)))
	copy(frame[deltaFrameHeaderSize:], payload)
	crc := crc32.Update(0, deltaCRC, frame[:60])
	crc = crc32.Update(crc, deltaCRC, frame[deltaFrameHeaderSize:])
	binary.LittleEndian.PutUint32(frame[60:64], crc)
	return frame, nil
}

func decodeDeltaMutations(payload []byte, count uint32) ([]deltaIndexMutation, error) {
	if len(payload) != int(count)*deltaMutationSize {
		return nil, fmt.Errorf("payload has %d bytes, want %d", len(payload), int(count)*deltaMutationSize)
	}
	mutations := make([]deltaIndexMutation, int(count))
	for i := range mutations {
		record := payload[i*deltaMutationSize : (i+1)*deltaMutationSize]
		copy(mutations[i].Key[:], record[:32])
		mutations[i].Value.Entry = AccountIndexEntry{
			Slot:   binary.LittleEndian.Uint64(record[32:40]),
			FileId: binary.LittleEndian.Uint64(record[40:48]),
			Offset: binary.LittleEndian.Uint64(record[48:56]),
		}
		mutations[i].Kind = record[56]
		for _, reserved := range record[57:64] {
			if reserved != 0 {
				return nil, fmt.Errorf("mutation %d has non-zero reserved bytes", i)
			}
		}
		switch mutations[i].Kind {
		case deltaMutationLive:
		case deltaMutationTombstone:
			mutations[i].Value.Tombstone = true
		case deltaMutationRetire:
			if mutations[i].Key != (solana.PublicKey{}) {
				return nil, fmt.Errorf("retired appendvec mutation %d has non-zero pubkey", i)
			}
		default:
			return nil, fmt.Errorf("mutation %d has invalid kind %d", i, mutations[i].Kind)
		}
	}
	return mutations, nil
}

// Apply publishes one atomic mutation epoch. durable must be true for every
// canonical AccountsDB transition; false exists only for explicitly
// non-durable legacy/test callers.
func (idx *MutableAccountIndex) Apply(
	mutations []deltaIndexMutation,
	meta *foldMeta,
	durable bool,
) error {
	if idx == nil {
		return errors.New("accountsdb: nil mutable account index")
	}
	// Decouple the durable encoding and eventual RAM publication from any
	// caller-owned backing array or metadata pointer while the journal sync is
	// in flight.
	mutations = append([]deltaIndexMutation(nil), mutations...)
	var metaCopy *foldMeta
	if meta != nil {
		copied := *meta
		metaCopy = &copied
	}
	for {
		idx.writeMu.Lock()
		if idx.closed {
			idx.writeMu.Unlock()
			return errors.New("accountsdb: mutable account index is closed")
		}
		if idx.poison != nil {
			err := idx.poison
			idx.writeMu.Unlock()
			return fmt.Errorf("accountsdb: mutable account index is poisoned after a journal failure: %w", err)
		}
		// The normal case is safely below the active-generation bound. Inspect it
		// under a read lock so an already pinned lookup snapshot blocks only the
		// atomic RAM publication, after the new frame has reached the journal.
		idx.mu.RLock()
		checkpointErr := idx.checkpointErr
		capacityFits := checkpointErr == nil && idx.checkpointCapacityFitsLocked(mutations)
		idx.mu.RUnlock()
		if checkpointErr != nil {
			idx.writeMu.Unlock()
			return fmt.Errorf("accountsdb: mutable account-index checkpoint failed: %w", checkpointErr)
		}

		var waitForCheckpoint <-chan struct{}
		var capacityErr error
		if !capacityFits {
			idx.mu.Lock()
			checkpointErr = idx.checkpointErr
			if checkpointErr == nil {
				waitForCheckpoint, capacityErr = idx.checkpointCapacityWaitLocked(mutations)
			}
			idx.mu.Unlock()
		}
		if checkpointErr != nil {
			idx.writeMu.Unlock()
			return fmt.Errorf("accountsdb: mutable account-index checkpoint failed: %w", checkpointErr)
		}
		if capacityErr != nil {
			idx.writeMu.Unlock()
			return capacityErr
		}
		if len(mutations) == 0 && metaCopy == nil {
			idx.writeMu.Unlock()
			return nil
		}
		if waitForCheckpoint == nil {
			break
		}
		idx.writeMu.Unlock()
		<-waitForCheckpoint
	}
	defer idx.writeMu.Unlock()

	if idx.seq == ^uint64(0) {
		return errors.New("accountsdb: mutable-index journal sequence exhausted")
	}
	seq := idx.seq + 1
	frame, err := encodeDeltaFrame(seq, mutations, metaCopy, false)
	if err != nil {
		return err
	}

	frameOffset := idx.offset
	if err := writeFullAt(idx.file, frame, frameOffset); err != nil {
		idx.poison = err
		return fmt.Errorf("accountsdb: append mutable-index frame %d: %w", seq, err)
	}
	if durable {
		if err := idx.file.Sync(); err != nil {
			idx.poison = err
			return fmt.Errorf("accountsdb: sync mutable-index frame %d: %w", seq, err)
		}
	}

	idx.mu.Lock()
	// A checkpoint can fail while the frame is being synced. Leave that valid
	// frame for restart recovery, but do not publish a new in-process epoch on
	// top of a failed checkpoint state.
	if idx.checkpointErr != nil {
		err := idx.checkpointErr
		idx.poison = err
		idx.mu.Unlock()
		return fmt.Errorf("accountsdb: mutable account-index checkpoint failed after journaling frame %d: %w", seq, err)
	}
	idx.applyInMemory(mutations)
	if metaCopy != nil {
		idx.meta = *metaCopy
		idx.hasMeta = true
	}
	idx.uncheckpointedBytes += uint64(len(frame))
	idx.seq = seq
	idx.offset += int64(len(frame))
	idx.maybeFreezeLocked()
	idx.mu.Unlock()
	return nil
}

// checkpointCapacityFitsLocked is a conservative allocation-free admission
// check. idx.mu must be held for reading or writing. Treating every mutation
// as a new key can only select the exact slow path; it cannot admit overflow.
func (idx *MutableAccountIndex) checkpointCapacityFitsLocked(mutations []deltaIndexMutation) bool {
	return len(idx.entries) <= idx.checkpointMaxActiveKeys &&
		len(mutations) <= idx.checkpointMaxActiveKeys-len(idx.entries)
}

// checkpointCapacityWaitLocked enforces the active-generation key bound before
// journal publication. If the current active map is full and no build is in
// progress, it is force-rotated even when its normal amortization threshold has
// not yet fired. idx.mu and writeMu must both be held.
func (idx *MutableAccountIndex) checkpointCapacityWaitLocked(mutations []deltaIndexMutation) (<-chan struct{}, error) {
	// This upper-bound check covers the normal small fold without allocating the
	// exact duplicate-detection set below.
	if idx.checkpointCapacityFitsLocked(mutations) {
		return nil, nil
	}
	uniqueIncoming, newActiveKeys, incomingOverflow := deltaMutationKeyGrowth(
		idx.entries, mutations, idx.checkpointMaxActiveKeys,
	)
	if len(idx.entries) <= idx.checkpointMaxActiveKeys-newActiveKeys {
		return nil, nil
	}
	if incomingOverflow || uniqueIncoming > idx.checkpointMaxActiveKeys {
		return nil, fmt.Errorf(
			"%w: one publication needs more than %d distinct account keys",
			ErrMutableIndexActiveCapacity, idx.checkpointMaxActiveKeys,
		)
	}
	if idx.checkpointBuilding {
		if idx.checkpointDone == nil {
			return nil, errors.New("accountsdb: mutable account-index checkpoint is building without a completion signal")
		}
		return idx.checkpointDone, nil
	}
	if len(idx.entries) == 0 {
		return nil, fmt.Errorf(
			"%w: publication would exceed %d active account keys",
			ErrMutableIndexActiveCapacity, idx.checkpointMaxActiveKeys,
		)
	}
	idx.freezeLocked()
	return nil, nil
}

// deltaMutationKeyGrowth counts distinct live/tombstone keys in one mutation
// epoch and how many are absent from entries. It stops once the supplied limit
// is exceeded, bounding its temporary set as well as the eventual active map.
func deltaMutationKeyGrowth(
	entries map[solana.PublicKey]deltaIndexValue,
	mutations []deltaIndexMutation,
	limit int,
) (unique, newKeys int, overflow bool) {
	seen := make(map[solana.PublicKey]struct{}, min(len(mutations), limit))
	for i := range mutations {
		mutation := mutations[i]
		if mutation.Kind != deltaMutationLive && mutation.Kind != deltaMutationTombstone {
			continue
		}
		if _, duplicate := seen[mutation.Key]; duplicate {
			continue
		}
		seen[mutation.Key] = struct{}{}
		unique++
		if _, exists := entries[mutation.Key]; !exists {
			newKeys++
		}
		if unique > limit {
			return unique, newKeys, true
		}
	}
	return unique, newKeys, false
}

func (idx *MutableAccountIndex) applyInMemory(mutations []deltaIndexMutation) {
	for i := range mutations {
		mutation := mutations[i]
		switch mutation.Kind {
		case deltaMutationLive, deltaMutationTombstone:
			idx.entries[mutation.Key] = mutation.Value
		case deltaMutationRetire:
			idx.retired[retiredAppendVec{
				Slot:   mutation.Value.Entry.Slot,
				FileID: mutation.Value.Entry.FileId,
			}] = struct{}{}
		}
	}
}

func (idx *MutableAccountIndex) Lookup(key solana.PublicKey) (deltaIndexValue, bool) {
	value, ok, _ := idx.LookupWithError(key)
	return value, ok
}

func (idx *MutableAccountIndex) LookupWithError(key solana.PublicKey) (deltaIndexValue, bool, error) {
	if idx == nil {
		return deltaIndexValue{}, false, nil
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if value, ok := idx.entries[key]; ok {
		return value, true, nil
	}
	if value, ok := idx.frozen[key]; ok {
		return value, true, nil
	}
	if idx.checkpoint == nil {
		return deltaIndexValue{}, false, nil
	}
	return idx.checkpoint.Lookup(key)
}

func (idx *MutableAccountIndex) NewSnapshot() *MutableAccountIndexSnapshot {
	if idx == nil {
		return nil
	}
	idx.mu.RLock()
	return &MutableAccountIndexSnapshot{index: idx}
}

func (snapshot *MutableAccountIndexSnapshot) Lookup(key solana.PublicKey) (deltaIndexValue, bool) {
	value, ok, _ := snapshot.LookupWithError(key)
	return value, ok
}

func (snapshot *MutableAccountIndexSnapshot) LookupWithError(key solana.PublicKey) (deltaIndexValue, bool, error) {
	if snapshot == nil || snapshot.closed {
		return deltaIndexValue{}, false, nil
	}
	if value, ok := snapshot.index.entries[key]; ok {
		return value, true, nil
	}
	if value, ok := snapshot.index.frozen[key]; ok {
		return value, true, nil
	}
	if snapshot.index.checkpoint == nil {
		return deltaIndexValue{}, false, nil
	}
	// NewSnapshot holds index.mu for this snapshot's complete lifetime. A
	// checkpoint swap and the old generation's Close both require index.mu for
	// writing, so taking checkpoint.mu again for every key is redundant.
	return snapshot.index.checkpoint.lookupPinned(key)
}

// lookupBatchWithError resolves one pinned mutable-index epoch. keyAt and
// accept may be invoked concurrently for distinct jobs once the hot RAM maps
// have been examined. A compact bitset records RAM hits so only exact misses
// probe the immutable checkpoint, without allocating a second key/value array.
// The caller must keep the snapshot open until this method returns.
func (snapshot *MutableAccountIndexSnapshot) lookupBatchWithError(
	ctx context.Context,
	count int,
	keyAt func(job int) solana.PublicKey,
	accept func(job int, value deltaIndexValue, found bool),
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil mutable-index batch lookup context")
	}
	if snapshot == nil || snapshot.closed || count == 0 {
		return ctx.Err()
	}

	checkpoint := snapshot.index.checkpoint
	var resolved []uint64
	if checkpoint != nil {
		resolved = make([]uint64, (count+63)/64)
	}
	for job := 0; job < count; job++ {
		if job&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		key := keyAt(job)
		value, found := snapshot.index.entries[key]
		if !found {
			value, found = snapshot.index.frozen[key]
		}
		if !found {
			continue
		}
		if resolved != nil {
			resolved[job>>6] |= uint64(1) << (job & 63)
		}
		accept(job, value, true)
	}
	if checkpoint == nil {
		return nil
	}

	return checkpoint.lookupBatchPinned(
		ctx,
		count,
		keyAt,
		func(job int) bool {
			return resolved[job>>6]&(uint64(1)<<(job&63)) != 0
		},
		accept,
	)
}

func (snapshot *MutableAccountIndexSnapshot) Close() error {
	if snapshot == nil || snapshot.closed {
		return nil
	}
	snapshot.closed = true
	snapshot.index.mu.RUnlock()
	return nil
}

func (idx *MutableAccountIndex) ReadFoldMeta() (foldMeta, bool) {
	if idx == nil {
		return foldMeta{}, false
	}
	idx.mu.RLock()
	meta, ok := idx.meta, idx.hasMeta
	idx.mu.RUnlock()
	return meta, ok
}

func (idx *MutableAccountIndex) IsRetired(slot, fileID uint64) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	_, ok := idx.retired[retiredAppendVec{Slot: slot, FileID: fileID}]
	idx.mu.RUnlock()
	return ok
}

func (idx *MutableAccountIndex) Len() int {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	length := len(idx.entries) + len(idx.frozen)
	idx.mu.RUnlock()
	return length
}

func (idx *MutableAccountIndex) Stats() MutableAccountIndexStats {
	if idx == nil {
		return MutableAccountIndexStats{}
	}
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	stats := MutableAccountIndexStats{
		ActiveKeys:              uint64(len(idx.entries)),
		FrozenKeys:              uint64(len(idx.frozen)),
		JournalSequence:         idx.seq,
		JournalBytes:            uint64(idx.offset),
		UncheckpointedBytes:     idx.uncheckpointedBytes,
		CheckpointBuilding:      idx.checkpointBuilding,
		CheckpointMaxKeys:       uint64(idx.checkpointMaxKeys),
		CheckpointMaxActiveKeys: uint64(idx.checkpointMaxActiveKeys),
		ReplayMaxKeys:           uint64(idx.checkpointReplayMaxKeys),
	}
	if idx.checkpoint != nil {
		stats.CheckpointKeys = idx.checkpoint.Len()
		stats.CheckpointCoveredSeq = idx.checkpoint.CoveredSeq()
	}
	return stats
}

func (idx *MutableAccountIndex) maybeFreezeLocked() {
	if idx.closing || idx.checkpointBuilding {
		return
	}
	threshold := idx.checkpointMinKeys
	if idx.checkpoint != nil {
		threshold = max(threshold, int(min(idx.checkpoint.Len()/8, uint64(^uint(0)>>1))))
	}
	threshold = min(threshold, idx.checkpointMaxActiveKeys)
	if len(idx.entries) < threshold && idx.uncheckpointedBytes < idx.checkpointJournalBytes {
		return
	}
	if len(idx.entries) == 0 && idx.uncheckpointedBytes == 0 {
		return
	}
	idx.freezeLocked()
}

func (idx *MutableAccountIndex) freezeLocked() {
	frozen := idx.entries
	coveredSeq := idx.seq
	old := idx.checkpoint
	idx.entries = make(map[solana.PublicKey]deltaIndexValue)
	idx.frozen = frozen
	idx.frozenSeq = coveredSeq
	idx.uncheckpointedBytes = 0
	idx.checkpointBuilding = true
	idx.checkpointDone = make(chan struct{})
	idx.checkpointBuildWait.Add(1)
	go idx.buildCheckpoint(old, frozen, coveredSeq)
}

func (idx *MutableAccountIndex) buildCheckpoint(
	old *DeltaCheckpoint,
	frozen map[solana.PublicKey]deltaIndexValue,
	coveredSeq uint64,
) {
	defer idx.checkpointBuildWait.Done()
	dir := filepath.Dir(idx.path)
	checkpoint, err := buildDeltaCheckpoint(
		idx.checkpointCtx,
		dir,
		old,
		frozen,
		coveredSeq,
		runtime.GOMAXPROCS(0),
		uint64(idx.checkpointMaxKeys),
	)

	idx.mu.Lock()
	if err != nil {
		if errors.Is(err, ErrDeltaCheckpointCapacity) {
			mlog.Log.Errorf("AccountsDB mutable index reached its %d-key delta ceiling; bootstrap from a fresh snapshot before replay can continue: %v", idx.checkpointMaxKeys, err)
		}
		idx.checkpointErr = err
		idx.finishCheckpointBuildLocked()
		idx.mu.Unlock()
		return
	}
	if idx.frozenSeq != coveredSeq {
		idx.checkpointErr = fmt.Errorf(
			"accountsdb: built delta checkpoint sequence %d does not match frozen sequence %d",
			coveredSeq, idx.frozenSeq,
		)
		idx.finishCheckpointBuildLocked()
		idx.mu.Unlock()
		_ = checkpoint.Close()
		return
	}
	idx.checkpoint = checkpoint
	idx.frozen = nil
	idx.frozenSeq = 0
	checkpointKeys := checkpoint.Len()
	checkpointMaxKeys := uint64(idx.checkpointMaxKeys)
	idx.mu.Unlock()
	if checkpointMaxKeys > 0 && checkpointKeys >= checkpointMaxKeys*3/4 {
		mlog.Log.Warnf(
			"AccountsDB mutable index checkpoint contains %d/%d changed keys (%.1f%%); schedule a fresh snapshot before the hard delta ceiling",
			checkpointKeys, checkpointMaxKeys, 100*float64(checkpointKeys)/float64(checkpointMaxKeys),
		)
	}

	if old != nil {
		_ = old.Close()
	}
	_, _ = GarbageCollectDeltaCheckpointFiles(dir, checkpoint.Generation())
	if err := idx.rewriteJournalAfterCheckpoint(coveredSeq); err != nil {
		idx.mu.Lock()
		if !idx.closing {
			idx.checkpointErr = err
		}
		idx.finishCheckpointBuildLocked()
		idx.mu.Unlock()
		return
	}
	idx.mu.Lock()
	idx.finishCheckpointBuildLocked()
	idx.maybeFreezeLocked()
	idx.mu.Unlock()
}

func (idx *MutableAccountIndex) finishCheckpointBuildLocked() {
	idx.checkpointBuilding = false
	done := idx.checkpointDone
	idx.checkpointDone = nil
	if done != nil {
		close(done)
	}
}

// rewriteJournalAfterCheckpoint replaces the append-only history with one
// exact state frame above coveredSeq. The published checkpoint descriptor and
// the old journal remain a valid crash pair until the replacement journal is
// renamed; after the rename, the replacement file is fsync'd and already held
// under the same exclusive advisory lock as the old file.
func (idx *MutableAccountIndex) rewriteJournalAfterCheckpoint(coveredSeq uint64) error {
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	if idx.closed {
		return nil
	}

	idx.mu.RLock()
	if idx.checkpoint == nil || idx.checkpoint.CoveredSeq() != coveredSeq {
		idx.mu.RUnlock()
		return fmt.Errorf("accountsdb: cannot compact mutable-index journal at sequence %d: active checkpoint does not match", coveredSeq)
	}
	mutations := make([]deltaIndexMutation, 0, len(idx.entries)+len(idx.retired))
	for key, value := range idx.entries {
		if value.Tombstone {
			mutations = append(mutations, tombstoneDeltaMutation(key))
		} else {
			mutations = append(mutations, liveDeltaMutation(key, value.Entry))
		}
	}
	for retired := range idx.retired {
		mutations = append(mutations, retireDeltaMutation(retired.Slot, retired.FileID))
	}
	var meta *foldMeta
	if idx.hasMeta {
		copied := idx.meta
		meta = &copied
	}
	idx.mu.RUnlock()

	// Stable state frames make restart artifacts reproducible and avoid
	// exposing Go's randomized map iteration order on disk.
	sort.Slice(mutations, func(i, j int) bool {
		if mutations[i].Kind == deltaMutationRetire || mutations[j].Kind == deltaMutationRetire {
			if mutations[i].Kind != mutations[j].Kind {
				return mutations[i].Kind != deltaMutationRetire
			}
			left, right := mutations[i].Value.Entry, mutations[j].Value.Entry
			if left.Slot != right.Slot {
				return left.Slot < right.Slot
			}
			return left.FileId < right.FileId
		}
		return bytes.Compare(mutations[i].Key[:], mutations[j].Key[:]) < 0
	})

	stateFrames := 0
	if len(mutations) != 0 {
		stateFrames = (len(mutations) + deltaStateFrameMaxMutations - 1) / deltaStateFrameMaxMutations
	} else if meta != nil {
		stateFrames = 1
	}
	if uint64(stateFrames) > uint64(^uint32(0)) || uint64(stateFrames) > ^uint64(0)-coveredSeq {
		return fmt.Errorf("accountsdb: compact mutable-index state needs too many frames: %d", stateFrames)
	}
	stateFrameCount := uint32(stateFrames)
	seq := coveredSeq

	tmpPath := filepath.Join(filepath.Dir(idx.path), DeltaIndexJournalRewriteFileName)
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("accountsdb: remove stale mutable-index journal rewrite: %w", err)
	}
	replacement, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create mutable-index journal rewrite: %w", err)
	}
	keepReplacement := false
	defer func() {
		if !keepReplacement {
			_ = replacement.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if err := unix.Flock(int(replacement.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("accountsdb: lock mutable-index journal rewrite: %w", err)
	}
	header := encodeDeltaJournalHeader(coveredSeq, stateFrameCount)
	if err := writeFullAt(replacement, header, 0); err != nil {
		return fmt.Errorf("accountsdb: write compact mutable-index journal header: %w", err)
	}
	writeOffset := int64(deltaJournalHeaderSize)
	for frameOrdinal := 0; frameOrdinal < stateFrames; frameOrdinal++ {
		start := min(frameOrdinal*deltaStateFrameMaxMutations, len(mutations))
		end := min(start+deltaStateFrameMaxMutations, len(mutations))
		var frameMeta *foldMeta
		if frameOrdinal == stateFrames-1 {
			frameMeta = meta
		}
		seq++
		frame, err := encodeDeltaFrame(seq, mutations[start:end], frameMeta, true)
		if err != nil {
			return fmt.Errorf("accountsdb: encode compact mutable-index journal state frame %d: %w", frameOrdinal+1, err)
		}
		if err := writeFullAt(replacement, frame, writeOffset); err != nil {
			return fmt.Errorf("accountsdb: write compact mutable-index journal state frame %d: %w", frameOrdinal+1, err)
		}
		writeOffset += int64(len(frame))
	}
	if err := replacement.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync compact mutable-index journal: %w", err)
	}
	if err := os.Rename(tmpPath, idx.path); err != nil {
		return fmt.Errorf("accountsdb: publish compact mutable-index journal: %w", err)
	}
	keepReplacement = true
	old := idx.file
	idx.file = replacement
	idx.seq = seq
	idx.offset = writeOffset
	idx.mu.Lock()
	idx.uncheckpointedBytes = 0
	idx.mu.Unlock()
	closeErr := old.Close()
	if err := fsyncDir(filepath.Dir(idx.path)); err != nil {
		return fmt.Errorf("accountsdb: sync compact mutable-index journal directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("accountsdb: close superseded mutable-index journal: %w", closeErr)
	}
	return nil
}

func (idx *MutableAccountIndex) Close() error {
	if idx == nil {
		return nil
	}
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	if idx.closed {
		return nil
	}
	idx.closed = true
	idx.mu.Lock()
	idx.closing = true
	idx.mu.Unlock()
	if idx.cancelCheckpointBuild != nil {
		idx.cancelCheckpointBuild()
	}
	idx.writeMu.Unlock()
	idx.checkpointBuildWait.Wait()
	idx.writeMu.Lock()

	idx.mu.Lock()
	checkpoint := idx.checkpoint
	idx.checkpoint = nil
	idx.mu.Unlock()
	var checkpointErr error
	if checkpoint != nil {
		checkpointErr = checkpoint.Close()
	}
	if idx.file == nil {
		return checkpointErr
	}
	err := errors.Join(checkpointErr, idx.file.Sync(), idx.file.Close())
	idx.file = nil
	return err
}

func writeFullAt(file *os.File, data []byte, offset int64) error {
	for len(data) > 0 {
		n, err := file.WriteAt(data, offset)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
		offset += int64(n)
	}
	return nil
}

func truncateAndSync(file *os.File, size int64) error {
	if err := file.Truncate(size); err != nil {
		return err
	}
	return file.Sync()
}
