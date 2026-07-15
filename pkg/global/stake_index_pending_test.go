package global

import (
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
