package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func TestMutableAccountIndexApplyAndReplayAllState(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)

	liveKey := deltaIndexTestKey(1)
	tombstoneKey := deltaIndexTestKey(2)
	firstEntry := AccountIndexEntry{Slot: 101, FileId: 201, Offset: 304}
	firstMeta := foldMeta{BatchSeq: 11, ThroughSlot: 1_001, FileId: 301}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(liveKey, firstEntry),
		tombstoneDeltaMutation(tombstoneKey),
		retireDeltaMutation(91, 92),
	}, &firstMeta, true); err != nil {
		t.Fatalf("apply first frame: %v", err)
	}
	requireMutableLive(t, idx, liveKey, firstEntry)
	requireMutableTombstone(t, idx, tombstoneKey)
	if !idx.IsRetired(91, 92) {
		t.Fatal("first retired appendvec was not published")
	}
	requireMutableMeta(t, idx, firstMeta)

	secondEntry := AccountIndexEntry{Slot: 102, FileId: 202, Offset: 408}
	secondMeta := foldMeta{BatchSeq: 12, ThroughSlot: 1_002, FileId: 302}
	if err := idx.Apply([]deltaIndexMutation{
		tombstoneDeltaMutation(liveKey),
		liveDeltaMutation(tombstoneKey, secondEntry),
		retireDeltaMutation(93, 94),
	}, &secondMeta, true); err != nil {
		t.Fatalf("apply second frame: %v", err)
	}
	if got := idx.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close mutable index: %v", err)
	}

	reopened := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })
	requireMutableTombstone(t, reopened, liveKey)
	requireMutableLive(t, reopened, tombstoneKey, secondEntry)
	if !reopened.IsRetired(91, 92) || !reopened.IsRetired(93, 94) {
		t.Fatal("retired appendvec set did not survive replay")
	}
	if reopened.IsRetired(91, 95) {
		t.Fatal("unretired appendvec reported retired")
	}
	requireMutableMeta(t, reopened, secondMeta)
	if got := reopened.seq; got != 2 {
		t.Fatalf("replayed sequence = %d, want 2", got)
	}
}

func TestMutableAccountIndexSnapshotBatchNewestWinsAcrossAllLayers(t *testing.T) {
	dir := t.TempDir()
	activeKey := deltaIndexTestKey(51)
	frozenKey := deltaIndexTestKey(52)
	checkpointKey := deltaIndexTestKey(53)
	tombstoneKey := deltaIndexTestKey(54)
	missingKey := deltaIndexTestKey(55)
	oldEntry := AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}
	activeEntry := AccountIndexEntry{Slot: 10, FileId: 20, Offset: 80}
	frozenEntry := AccountIndexEntry{Slot: 11, FileId: 21, Offset: 88}
	checkpointEntry := AccountIndexEntry{Slot: 12, FileId: 22, Offset: 96}

	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		activeKey:     {Entry: oldEntry},
		frozenKey:     {Entry: oldEntry},
		checkpointKey: {Entry: checkpointEntry},
		tombstoneKey:  {Entry: oldEntry},
	}, 7, 2)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	defer checkpoint.Close()

	idx := &MutableAccountIndex{
		entries: map[solana.PublicKey]deltaIndexValue{
			activeKey:    {Entry: activeEntry},
			tombstoneKey: {Tombstone: true},
		},
		frozen: map[solana.PublicKey]deltaIndexValue{
			frozenKey: {Entry: frozenEntry},
		},
		checkpoint: checkpoint,
	}
	snapshot := idx.NewSnapshot()
	defer snapshot.Close()

	keys := []solana.PublicKey{checkpointKey, activeKey, missingKey, tombstoneKey, frozenKey, checkpointKey}
	values := make([]deltaIndexValue, len(keys))
	found := make([]bool, len(keys))
	if err := snapshot.lookupBatchWithError(
		context.Background(),
		len(keys),
		func(job int) solana.PublicKey { return keys[job] },
		func(job int, value deltaIndexValue, ok bool) {
			values[job], found[job] = value, ok
		},
	); err != nil {
		t.Fatalf("snapshot batch lookup: %v", err)
	}
	wantValues := []deltaIndexValue{
		{Entry: checkpointEntry},
		{Entry: activeEntry},
		{},
		{Tombstone: true},
		{Entry: frozenEntry},
		{Entry: checkpointEntry},
	}
	wantFound := []bool{true, true, false, true, true, true}
	for i := range keys {
		if values[i] != wantValues[i] || found[i] != wantFound[i] {
			t.Fatalf("batch result %d = (%+v, %v), want (%+v, %v)", i, values[i], found[i], wantValues[i], wantFound[i])
		}
	}
}

func TestMutableAccountIndexTornTailIsAtomicallyTruncated(t *testing.T) {
	for _, tc := range []struct {
		name       string
		secondTail int64
	}{
		{name: "partial frame header", secondTail: 17},
		{name: "partial frame payload", secondTail: deltaFrameHeaderSize + deltaMutationSize/2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, DeltaIndexJournalFileName)
			idx := openMutableIndexForTest(t, dir)
			firstKey := deltaIndexTestKey(10)
			secondKey := deltaIndexTestKey(11)
			firstEntry := AccountIndexEntry{Slot: 10, FileId: 20, Offset: 32}
			secondEntry := AccountIndexEntry{Slot: 11, FileId: 21, Offset: 40}
			firstMeta := foldMeta{BatchSeq: 1, ThroughSlot: 10, FileId: 20}
			secondMeta := foldMeta{BatchSeq: 2, ThroughSlot: 11, FileId: 21}
			if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(firstKey, firstEntry)}, &firstMeta, true); err != nil {
				t.Fatalf("apply first frame: %v", err)
			}
			firstEnd := idx.offset
			if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(secondKey, secondEntry)}, &secondMeta, true); err != nil {
				t.Fatalf("apply second frame: %v", err)
			}
			if err := idx.Close(); err != nil {
				t.Fatalf("close before truncation: %v", err)
			}

			if err := os.Truncate(path, firstEnd+tc.secondTail); err != nil {
				t.Fatalf("create torn journal tail: %v", err)
			}
			reopened := openMutableIndexForTest(t, dir)
			requireMutableLive(t, reopened, firstKey, firstEntry)
			if _, ok := reopened.Lookup(secondKey); ok {
				t.Fatal("partially persisted frame became visible")
			}
			requireMutableMeta(t, reopened, firstMeta)
			if reopened.seq != 1 {
				t.Fatalf("sequence after torn-tail replay = %d, want 1", reopened.seq)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat repaired journal: %v", err)
			}
			if info.Size() != firstEnd {
				t.Fatalf("repaired journal size = %d, want %d", info.Size(), firstEnd)
			}

			thirdKey := deltaIndexTestKey(12)
			thirdEntry := AccountIndexEntry{Slot: 12, FileId: 22, Offset: 48}
			thirdMeta := foldMeta{BatchSeq: 2, ThroughSlot: 12, FileId: 22}
			if err := reopened.Apply([]deltaIndexMutation{liveDeltaMutation(thirdKey, thirdEntry)}, &thirdMeta, true); err != nil {
				t.Fatalf("append after torn-tail repair: %v", err)
			}
			if reopened.seq != 2 {
				t.Fatalf("sequence after repaired append = %d, want 2", reopened.seq)
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("close repaired journal: %v", err)
			}

			final := openMutableIndexForTest(t, dir)
			t.Cleanup(func() { _ = final.Close() })
			requireMutableLive(t, final, firstKey, firstEntry)
			requireMutableLive(t, final, thirdKey, thirdEntry)
			if _, ok := final.Lookup(secondKey); ok {
				t.Fatal("discarded torn frame reappeared after another reopen")
			}
			requireMutableMeta(t, final, thirdMeta)
		})
	}
}

func TestMutableAccountIndexFrameCRCFailureIsFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, DeltaIndexJournalFileName)
	idx := openMutableIndexForTest(t, dir)
	key := deltaIndexTestKey(20)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 20, FileId: 30, Offset: 40}),
	}, nil, true); err != nil {
		t.Fatalf("apply frame: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close before corruption: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	originalSize := len(data)
	data[deltaJournalHeaderSize+deltaFrameHeaderSize+7] ^= 0x80
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt journal: %v", err)
	}

	corrupt, err := OpenMutableAccountIndex(dir)
	if err == nil {
		_ = corrupt.Close()
		t.Fatal("opening a frame with corrupt payload unexpectedly succeeded")
	}
	if corrupt != nil {
		t.Fatal("failed open returned a usable mutable index")
	}
	if !strings.Contains(err.Error(), "frame CRC mismatch") {
		t.Fatalf("open error = %q, want frame CRC mismatch", err)
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatalf("stat corrupt journal: %v", statErr)
	}
	if info.Size() != int64(originalSize) {
		t.Fatalf("CRC corruption was mistaken for a torn tail: size = %d, want %d", info.Size(), originalSize)
	}
}

func TestMutableAccountIndexSequenceCorruptionIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, DeltaIndexJournalFileName)
	idx := openMutableIndexForTest(t, dir)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(deltaIndexTestKey(30), AccountIndexEntry{Slot: 30, FileId: 40, Offset: 48}),
	}, nil, true); err != nil {
		t.Fatalf("apply first frame: %v", err)
	}
	secondOffset := idx.offset
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(deltaIndexTestKey(31), AccountIndexEntry{Slot: 31, FileId: 41, Offset: 56}),
	}, nil, true); err != nil {
		t.Fatalf("apply second frame: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close before sequence corruption: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	frame := data[secondOffset:]
	frameLen := binary.LittleEndian.Uint64(frame[16:24])
	if frameLen > uint64(len(frame)) {
		t.Fatalf("second frame length %d exceeds remaining file bytes %d", frameLen, len(frame))
	}
	frame = frame[:frameLen]
	binary.LittleEndian.PutUint64(frame[24:32], 7)
	crc := crc32.Update(0, deltaCRC, frame[:60])
	crc = crc32.Update(crc, deltaCRC, frame[deltaFrameHeaderSize:])
	binary.LittleEndian.PutUint32(frame[60:64], crc)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write sequence-corrupt journal: %v", err)
	}

	corrupt, err := OpenMutableAccountIndex(dir)
	if err == nil {
		_ = corrupt.Close()
		t.Fatal("opening a non-contiguous frame sequence unexpectedly succeeded")
	}
	if corrupt != nil {
		t.Fatal("failed sequence validation returned a usable mutable index")
	}
	if !strings.Contains(err.Error(), "sequence 7 does not follow 1") {
		t.Fatalf("open error = %q, want sequence discontinuity", err)
	}
}

func TestMutableAccountIndexSnapshotBlocksPublicationUntilClose(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = idx.Close() })
	left := deltaIndexTestKey(40)
	right := deltaIndexTestKey(41)
	oldEntry := AccountIndexEntry{Slot: 40, FileId: 50, Offset: 64}
	oldMeta := foldMeta{BatchSeq: 1, ThroughSlot: 40, FileId: 50}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(left, oldEntry),
		liveDeltaMutation(right, oldEntry),
	}, &oldMeta, true); err != nil {
		t.Fatalf("apply initial state: %v", err)
	}

	snapshot := idx.NewSnapshot()
	if snapshot == nil {
		t.Fatal("NewSnapshot returned nil")
	}
	before, err := os.Stat(idx.path)
	if err != nil {
		t.Fatalf("stat journal before blocked publication: %v", err)
	}
	newEntry := AccountIndexEntry{Slot: 41, FileId: 51, Offset: 72}
	newMeta := foldMeta{BatchSeq: 2, ThroughSlot: 41, FileId: 51}
	applyDone := make(chan error, 1)
	go func() {
		applyDone <- idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(left, newEntry),
			liveDeltaMutation(right, newEntry),
		}, &newMeta, true)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		info, statErr := os.Stat(idx.path)
		if statErr != nil {
			t.Fatalf("stat journal while publication is blocked: %v", statErr)
		}
		if info.Size() > before.Size() {
			break // The writer has journaled the frame and is waiting for idx.mu.
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not reach the snapshot publication barrier")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-applyDone:
		t.Fatalf("Apply completed while a snapshot was pinned: %v", err)
	default:
	}
	requireMutableSnapshotLive(t, snapshot, left, oldEntry)
	requireMutableSnapshotLive(t, snapshot, right, oldEntry)

	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("Apply after snapshot release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Apply remained blocked after snapshot Close")
	}
	requireMutableLive(t, idx, left, newEntry)
	requireMutableLive(t, idx, right, newEntry)
	requireMutableMeta(t, idx, newMeta)
}

func TestMutableAccountIndexConcurrentReadersObserveWholeFrames(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = idx.Close() })
	left := deltaIndexTestKey(50)
	right := deltaIndexTestKey(51)
	initial := AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8}
	initialMeta := foldMeta{BatchSeq: 1, ThroughSlot: 1, FileId: 1}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(left, initial),
		liveDeltaMutation(right, initial),
	}, &initialMeta, false); err != nil {
		t.Fatalf("apply initial frame: %v", err)
	}

	var stop atomic.Bool
	errCh := make(chan string, 1)
	report := func(message string) {
		select {
		case errCh <- message:
		default:
		}
	}
	var ready sync.WaitGroup
	var readers sync.WaitGroup
	const readerCount = 8
	ready.Add(readerCount)
	readers.Add(readerCount)
	for range readerCount {
		go func() {
			defer readers.Done()
			ready.Done()
			for !stop.Load() {
				snapshot := idx.NewSnapshot()
				leftValue, leftOK := snapshot.Lookup(left)
				rightValue, rightOK := snapshot.Lookup(right)
				if !leftOK || !rightOK {
					report("snapshot lost one side of a previously complete frame")
				}
				if leftValue != rightValue {
					report("snapshot observed keys from different mutation frames")
				}
				if err := snapshot.Close(); err != nil {
					report("snapshot Close failed: " + err.Error())
				}
				_, _ = idx.Lookup(left)
				_, _ = idx.ReadFoldMeta()
				_ = idx.IsRetired(500, 600)
				_ = idx.Len()
			}
		}()
	}
	ready.Wait()

	const frames = 500
	for seq := uint64(2); seq <= frames; seq++ {
		var mutations []deltaIndexMutation
		if seq%2 == 0 {
			mutations = []deltaIndexMutation{
				tombstoneDeltaMutation(left),
				tombstoneDeltaMutation(right),
			}
		} else {
			entry := AccountIndexEntry{Slot: seq, FileId: seq + 100, Offset: seq * 8}
			mutations = []deltaIndexMutation{
				liveDeltaMutation(left, entry),
				liveDeltaMutation(right, entry),
			}
		}
		if seq%50 == 0 {
			mutations = append(mutations, retireDeltaMutation(500, seq))
		}
		meta := foldMeta{BatchSeq: seq, ThroughSlot: seq, FileId: seq + 100}
		if err := idx.Apply(mutations, &meta, false); err != nil {
			stop.Store(true)
			readers.Wait()
			t.Fatalf("apply concurrent frame %d: %v", seq, err)
		}
	}
	stop.Store(true)
	readers.Wait()
	select {
	case message := <-errCh:
		t.Fatal(message)
	default:
	}
	requireMutableTombstone(t, idx, left)
	requireMutableTombstone(t, idx, right)
	requireMutableMeta(t, idx, foldMeta{BatchSeq: frames, ThroughSlot: frames, FileId: frames + 100})
	if !idx.IsRetired(500, frames) {
		t.Fatalf("retirement from final frame was not published")
	}
}

func TestMutableAccountIndexCloseAndReopenContinuesJournal(t *testing.T) {
	dir := t.TempDir()
	firstKey := deltaIndexTestKey(60)
	secondKey := deltaIndexTestKey(61)
	firstEntry := AccountIndexEntry{Slot: 60, FileId: 70, Offset: 80}
	secondEntry := AccountIndexEntry{Slot: 61, FileId: 71, Offset: 88}

	idx := openMutableIndexForTest(t, dir)
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(firstKey, firstEntry)}, nil, false); err != nil {
		t.Fatalf("apply unsynced frame: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
	if err := idx.Apply([]deltaIndexMutation{tombstoneDeltaMutation(firstKey)}, nil, true); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Apply after Close error = %v, want closed error", err)
	}

	reopened := openMutableIndexForTest(t, dir)
	requireMutableLive(t, reopened, firstKey, firstEntry)
	if reopened.seq != 1 {
		t.Fatalf("first reopen sequence = %d, want 1", reopened.seq)
	}
	meta := foldMeta{BatchSeq: 2, ThroughSlot: 61, FileId: 71}
	if err := reopened.Apply([]deltaIndexMutation{
		liveDeltaMutation(secondKey, secondEntry),
		retireDeltaMutation(60, 70),
	}, &meta, true); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if reopened.seq != 2 {
		t.Fatalf("continued sequence = %d, want 2", reopened.seq)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	final := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = final.Close() })
	requireMutableLive(t, final, firstKey, firstEntry)
	requireMutableLive(t, final, secondKey, secondEntry)
	requireMutableMeta(t, final, meta)
	if !final.IsRetired(60, 70) {
		t.Fatal("retirement appended after reopen was not replayed")
	}
	if final.seq != 2 {
		t.Fatalf("final replay sequence = %d, want 2", final.seq)
	}
}

func TestMutableAccountIndexJournalHasExclusiveProcessLock(t *testing.T) {
	dir := t.TempDir()
	first := openMutableIndexForTest(t, dir)
	second, err := OpenMutableAccountIndex(dir)
	if err == nil {
		_ = second.Close()
		t.Fatal("second mutable-index writer unexpectedly acquired the journal")
	}
	if !strings.Contains(err.Error(), "another process may be using it") {
		t.Fatalf("second open error = %q, want exclusive-lock error", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first writer: %v", err)
	}

	reopened := openMutableIndexForTest(t, dir)
	if err := reopened.Close(); err != nil {
		t.Fatalf("open after lock release: %v", err)
	}
}

func TestMutableAccountIndexNoopApplyDoesNotGrowJournal(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = idx.Close() })
	before := idx.Stats()
	if err := idx.Apply(nil, nil, true); err != nil {
		t.Fatalf("no-op Apply: %v", err)
	}
	after := idx.Stats()
	if after.JournalSequence != before.JournalSequence || after.JournalBytes != before.JournalBytes {
		t.Fatalf("no-op Apply changed journal: before=%+v after=%+v", before, after)
	}
}

func TestMutableAccountIndexBackpressuresNewKeysDuringCheckpointBuild(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = idx.Close() })
	idx.checkpointMinKeys = 1 << 20
	idx.checkpointMaxActiveKeys = 1
	existing := deltaIndexTestKey(68)
	idx.mu.Lock()
	idx.entries[existing] = deltaIndexValue{Entry: AccountIndexEntry{Slot: 68, FileId: 78, Offset: 80}}
	idx.checkpointBuilding = true
	idx.checkpointDone = make(chan struct{})
	idx.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(deltaIndexTestKey(69), AccountIndexEntry{Slot: 69, FileId: 79, Offset: 88}),
		}, nil, true)
	}()
	select {
	case err := <-done:
		t.Fatalf("new-key Apply escaped checkpoint backpressure: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	idx.mu.Lock()
	idx.finishCheckpointBuildLocked()
	idx.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Apply after checkpoint completion: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new-key Apply remained blocked after checkpoint completion")
	}
}

func TestMutableAccountIndexRejectsOversizedPublicationBeforeJournal(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = idx.Close() })
	idx.checkpointMaxActiveKeys = 2

	before := idx.Stats()
	err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(deltaIndexTestKey(80), AccountIndexEntry{Slot: 80}),
		tombstoneDeltaMutation(deltaIndexTestKey(81)),
		liveDeltaMutation(deltaIndexTestKey(82), AccountIndexEntry{Slot: 82}),
	}, nil, true)
	if !errors.Is(err, ErrMutableIndexActiveCapacity) {
		t.Fatalf("oversized Apply error = %v, want %v", err, ErrMutableIndexActiveCapacity)
	}
	after := idx.Stats()
	if after.JournalSequence != before.JournalSequence || after.JournalBytes != before.JournalBytes {
		t.Fatalf("rejected Apply changed journal: before=%+v after=%+v", before, after)
	}
	if after.ActiveKeys != 0 || after.FrozenKeys != 0 {
		t.Fatalf("rejected Apply published keys: %+v", after)
	}
}

func TestMutableAccountIndexBoundsStartupReplay(t *testing.T) {
	previousMaxActiveKeys := DeltaCheckpointMaxActiveKeys
	DeltaCheckpointMaxActiveKeys = 1 // Replay may retain at most two generations.
	t.Cleanup(func() { DeltaCheckpointMaxActiveKeys = previousMaxActiveKeys })

	dir := t.TempDir()
	requireNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	requireNoError(InitializeMutableAccountIndex(dir))
	frame, err := encodeDeltaFrame(1, []deltaIndexMutation{
		liveDeltaMutation(deltaIndexTestKey(83), AccountIndexEntry{Slot: 83}),
		liveDeltaMutation(deltaIndexTestKey(84), AccountIndexEntry{Slot: 84}),
		liveDeltaMutation(deltaIndexTestKey(85), AccountIndexEntry{Slot: 85}),
	}, nil, false)
	requireNoError(err)
	journal := append(encodeDeltaJournalHeader(0, 0), frame...)
	path := filepath.Join(dir, DeltaIndexJournalFileName)
	requireNoError(os.WriteFile(path, journal, 0o644))

	idx, err := OpenMutableAccountIndex(dir)
	if idx != nil {
		_ = idx.Close()
		t.Fatal("oversized replay unexpectedly returned an index")
	}
	if !errors.Is(err, ErrMutableIndexActiveCapacity) {
		t.Fatalf("oversized replay error = %v, want %v", err, ErrMutableIndexActiveCapacity)
	}
	info, statErr := os.Stat(path)
	requireNoError(statErr)
	if info.Size() != int64(len(journal)) {
		t.Fatalf("rejected replay changed journal size to %d, want %d", info.Size(), len(journal))
	}
}

func TestMutableAccountIndexCheckpointCompactsJournalAndReopens(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	idx.checkpointMinKeys = 2
	idx.checkpointJournalBytes = ^uint64(0)

	liveKey := deltaIndexTestKey(70)
	tombstoneKey := deltaIndexTestKey(71)
	liveEntry := AccountIndexEntry{Slot: 70, FileId: 80, Offset: 88}
	meta := foldMeta{BatchSeq: 7, ThroughSlot: 70, FileId: 80}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(liveKey, liveEntry),
		tombstoneDeltaMutation(tombstoneKey),
		retireDeltaMutation(69, 79),
	}, &meta, true); err != nil {
		t.Fatalf("apply checkpoint epoch: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		stats := idx.Stats()
		if !stats.CheckpointBuilding && stats.CheckpointCoveredSeq == 1 {
			break
		}
		idx.mu.RLock()
		checkpointErr := idx.checkpointErr
		idx.mu.RUnlock()
		if checkpointErr != nil {
			t.Fatalf("checkpoint failed: %v", checkpointErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint did not finish; stats = %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}

	header := make([]byte, deltaJournalHeaderSize)
	if _, err := idx.file.ReadAt(header, 0); err != nil {
		t.Fatalf("read compact journal header: %v", err)
	}
	if baseSeq := binary.LittleEndian.Uint64(header[16:24]); baseSeq != 1 {
		t.Fatalf("compact journal base sequence = %d, want 1", baseSeq)
	}
	// The checkpoint owns key state. The compact journal retains the fold
	// frontier and appendvec retirement in one state frame above it.
	if stats := idx.Stats(); stats.JournalSequence != 2 || stats.JournalBytes != deltaJournalHeaderSize+deltaFrameHeaderSize+deltaMutationSize {
		t.Fatalf("compact journal stats = %+v, want sequence 2 and one retirement state frame", stats)
	}
	recentKey := deltaIndexTestKey(72)
	recentEntry := AccountIndexEntry{Slot: 72, FileId: 82, Offset: 96}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(recentKey, recentEntry)}, nil, true); err != nil {
		t.Fatalf("apply above checkpoint: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close checkpointed index: %v", err)
	}

	reopened := openMutableIndexForTest(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })
	requireMutableLive(t, reopened, liveKey, liveEntry)
	requireMutableTombstone(t, reopened, tombstoneKey)
	requireMutableLive(t, reopened, recentKey, recentEntry)
	requireMutableMeta(t, reopened, meta)
	if !reopened.IsRetired(69, 79) {
		t.Fatal("retirement was lost during journal compaction")
	}
	if reopened.seq != 3 {
		t.Fatalf("reopened journal sequence = %d, want 3", reopened.seq)
	}
}

func TestMutableAccountIndexRejectsCompactedJournalWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	idx := openMutableIndexForTest(t, dir)
	idx.checkpointMinKeys = 1
	idx.checkpointJournalBytes = ^uint64(0)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(deltaIndexTestKey(73), AccountIndexEntry{Slot: 73, FileId: 83, Offset: 104}),
	}, nil, true); err != nil {
		t.Fatalf("apply checkpoint epoch: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		stats := idx.Stats()
		if !stats.CheckpointBuilding && stats.CheckpointCoveredSeq == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint did not finish; stats = %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close checkpointed index: %v", err)
	}
	if _, err := GarbageCollectDeltaCheckpointFiles(dir); err != nil {
		t.Fatalf("remove published checkpoint: %v", err)
	}

	missing, err := OpenMutableAccountIndex(dir)
	if err == nil {
		_ = missing.Close()
		t.Fatal("compacted journal opened without its required checkpoint")
	}
	if !strings.Contains(err.Error(), "no delta checkpoint is published") {
		t.Fatalf("open error = %q, want missing-checkpoint error", err)
	}
}

func TestMutableAccountIndexReplaysAtomicMultiFrameStatePrefix(t *testing.T) {
	dir := t.TempDir()
	baseKey := deltaIndexTestKey(74)
	baseEntry := AccountIndexEntry{Slot: 74, FileId: 84, Offset: 112}
	checkpoint, err := BuildDeltaCheckpoint(
		context.Background(), dir, nil,
		map[solana.PublicKey]deltaIndexValue{baseKey: {Entry: baseEntry}},
		1, 1,
	)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	if err := checkpoint.Close(); err != nil {
		t.Fatalf("close checkpoint: %v", err)
	}

	recentKey := deltaIndexTestKey(75)
	recentEntry := AccountIndexEntry{Slot: 75, FileId: 85, Offset: 120}
	frame1, err := encodeDeltaFrame(2, []deltaIndexMutation{
		liveDeltaMutation(recentKey, recentEntry),
	}, nil, true)
	if err != nil {
		t.Fatalf("encode first state frame: %v", err)
	}
	meta := foldMeta{BatchSeq: 8, ThroughSlot: 75, FileId: 85}
	frame2, err := encodeDeltaFrame(3, []deltaIndexMutation{
		retireDeltaMutation(73, 83),
	}, &meta, true)
	if err != nil {
		t.Fatalf("encode second state frame: %v", err)
	}
	journal := append(encodeDeltaJournalHeader(1, 2), frame1...)
	journal = append(journal, frame2...)
	journalPath := filepath.Join(dir, DeltaIndexJournalFileName)
	if err := os.WriteFile(journalPath, journal, 0o644); err != nil {
		t.Fatalf("write multi-frame state journal: %v", err)
	}

	idx := openMutableIndexForTest(t, dir)
	requireMutableLive(t, idx, baseKey, baseEntry)
	requireMutableLive(t, idx, recentKey, recentEntry)
	requireMutableMeta(t, idx, meta)
	if !idx.IsRetired(73, 83) {
		t.Fatal("multi-frame state retirement was not replayed")
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("close multi-frame state index: %v", err)
	}

	tornSize := int64(deltaJournalHeaderSize + len(frame1) + len(frame2)/2)
	if err := os.Truncate(journalPath, tornSize); err != nil {
		t.Fatalf("truncate second state frame: %v", err)
	}
	torn, err := OpenMutableAccountIndex(dir)
	if err == nil {
		_ = torn.Close()
		t.Fatal("torn atomic state prefix unexpectedly opened")
	}
	if !strings.Contains(err.Error(), "state frame 2 of 2 is torn") {
		t.Fatalf("torn-state error = %q", err)
	}
	info, statErr := os.Stat(journalPath)
	if statErr != nil {
		t.Fatalf("stat torn state journal: %v", statErr)
	}
	if info.Size() != tornSize {
		t.Fatalf("torn atomic state was truncated to %d; want fail-closed size %d", info.Size(), tornSize)
	}
}

func openMutableIndexForTest(t *testing.T, dir string) *MutableAccountIndex {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, DeltaIndexJournalFileName)); errors.Is(err, os.ErrNotExist) {
		if err := InitializeMutableAccountIndex(dir); err != nil {
			t.Fatalf("InitializeMutableAccountIndex(%s): %v", dir, err)
		}
	}
	idx, err := OpenMutableAccountIndex(dir)
	if err != nil {
		t.Fatalf("OpenMutableAccountIndex(%s): %v", dir, err)
	}
	return idx
}

func deltaIndexTestKey(id byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = id
	key[len(key)-1] = id ^ 0xa5
	return key
}

func requireMutableLive(t *testing.T, idx *MutableAccountIndex, key solana.PublicKey, want AccountIndexEntry) {
	t.Helper()
	got, ok := idx.Lookup(key)
	if !ok {
		t.Fatalf("live key %s missing", key)
	}
	if got.Tombstone {
		t.Fatalf("live key %s replayed as tombstone", key)
	}
	if got.Entry != want {
		t.Fatalf("live key %s entry = %+v, want %+v", key, got.Entry, want)
	}
}

func requireMutableTombstone(t *testing.T, idx *MutableAccountIndex, key solana.PublicKey) {
	t.Helper()
	got, ok := idx.Lookup(key)
	if !ok {
		t.Fatalf("tombstone key %s missing", key)
	}
	if !got.Tombstone {
		t.Fatalf("key %s = %+v, want tombstone", key, got)
	}
}

func requireMutableMeta(t *testing.T, idx *MutableAccountIndex, want foldMeta) {
	t.Helper()
	got, ok := idx.ReadFoldMeta()
	if !ok {
		t.Fatal("fold metadata missing")
	}
	if got != want {
		t.Fatalf("fold metadata = %+v, want %+v", got, want)
	}
}

func requireMutableSnapshotLive(
	t *testing.T,
	snapshot *MutableAccountIndexSnapshot,
	key solana.PublicKey,
	want AccountIndexEntry,
) {
	t.Helper()
	got, ok := snapshot.Lookup(key)
	if !ok {
		t.Fatalf("snapshot live key %s missing", key)
	}
	if got.Tombstone || got.Entry != want {
		t.Fatalf("snapshot key %s = %+v, want live entry %+v", key, got, want)
	}
}
