package global

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
)

func testStakeKey(seed byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = seed
	key[len(key)-1] = seed
	return key
}

func resetStakeIndexTestState(t *testing.T) {
	t.Helper()
	instance.pendingStakeMutex.Lock()
	oldPending := instance.pendingStakeBySlot
	oldCached := instance.cachedStakeEntries
	oldFlushed := instance.entriesFlushedSinceCompact
	instance.pendingStakeBySlot = nil
	instance.cachedStakeEntries = nil
	instance.entriesFlushedSinceCompact = 0
	instance.pendingStakeMutex.Unlock()
	t.Cleanup(func() {
		instance.pendingStakeMutex.Lock()
		instance.pendingStakeBySlot = oldPending
		instance.cachedStakeEntries = oldCached
		instance.entriesFlushedSinceCompact = oldFlushed
		instance.pendingStakeMutex.Unlock()
	})
}

func TestPendingStakeIndexIsSlotScoped(t *testing.T) {
	resetStakeIndexTestState(t)
	dir := t.TempDir()
	durable := testStakeKey(1)
	keep := testStakeKey(2)
	drop := testStakeKey(3)
	if err := accountsdb.WriteStakePubkeyIndex(
		filepath.Join(dir, StakePubkeyIndexFileName),
		[]accountsdb.StakeIndexEntry{{Pubkey: durable, FileId: 7, Offset: 9}},
	); err != nil {
		t.Fatal(err)
	}

	EnqueuePendingStakePubkey(11, keep)
	EnqueuePendingStakePubkey(12, drop)
	entries, err := LoadStakePubkeyIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("merged entries = %d, want 3", len(entries))
	}

	if dropped := DropPendingStakePubkeysFrom(12); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	entries, err = LoadStakePubkeyIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries after unwind = %d, want 2", len(entries))
	}

	flushed, err := FlushPendingStakePubkeysThrough(dir, 11)
	if err != nil {
		t.Fatal(err)
	}
	if flushed != 1 {
		t.Fatalf("flushed = %d, want 1", flushed)
	}
	if pending := PendingStakeEntriesSnapshot(); len(pending) != 0 {
		t.Fatalf("pending after fold = %d, want 0", len(pending))
	}

	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = nil
	instance.pendingStakeMutex.Unlock()
	entries, err = LoadStakePubkeyIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("durable entries after fold = %d, want 2", len(entries))
	}
}

func TestCompactStakePubkeyIndexLoadsFreshDurableView(t *testing.T) {
	resetStakeIndexTestState(t)
	dir := t.TempDir()
	first := testStakeKey(1)
	second := testStakeKey(2)
	if err := accountsdb.WriteStakePubkeyIndex(
		filepath.Join(dir, StakePubkeyIndexFileName),
		[]accountsdb.StakeIndexEntry{
			{Pubkey: first, FileId: 1, Offset: 1},
			{Pubkey: second, FileId: 2, Offset: 2},
			{Pubkey: first, FileId: 3, Offset: 3},
		},
	); err != nil {
		t.Fatal(err)
	}
	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = nil
	instance.entriesFlushedSinceCompact = compactThreshold
	instance.pendingStakeMutex.Unlock()

	if err := CompactStakePubkeyIndex(dir); err != nil {
		t.Fatal(err)
	}
	instance.pendingStakeMutex.Lock()
	instance.cachedStakeEntries = nil
	flushed := instance.entriesFlushedSinceCompact
	instance.pendingStakeMutex.Unlock()
	if flushed != 0 {
		t.Fatalf("entries flushed since compaction = %d", flushed)
	}
	entries, err := LoadStakePubkeyIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("compacted entries = %d, want 2", len(entries))
	}
}

func TestLoadStakePubkeyIndexRepairsInterruptedAppend(t *testing.T) {
	resetStakeIndexTestState(t)
	dir := t.TempDir()
	indexPath := filepath.Join(dir, StakePubkeyIndexFileName)
	durable := testStakeKey(1)
	if err := accountsdb.WriteStakePubkeyIndex(indexPath, []accountsdb.StakeIndexEntry{{
		Pubkey: durable,
		FileId: 7,
		Offset: 9,
	}}); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 17)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := LoadStakePubkeyIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Pubkey != durable {
		t.Fatalf("recovered entries = %+v, want only durable entry", entries)
	}
	info, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(8 + accountsdb.StakeIndexRecordSize)
	if info.Size() != wantSize {
		t.Fatalf("repaired index size = %d, want %d", info.Size(), wantSize)
	}
}
