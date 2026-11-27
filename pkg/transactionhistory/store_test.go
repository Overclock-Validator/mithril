package transactionhistory

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// These are structurally valid wires, not signed execution or admission proof.
func testBlock(t *testing.T, slot uint64, count int, version solana.MessageVersion) Block {
	t.Helper()
	block := Block{ID: BlockID{Slot: slot, Blockhash: [32]byte{byte(slot), 1}}, ParentSlot: slot - 1, Bankhash: [32]byte{byte(slot), 2}, CaptureVersion: CaptureVersion}
	for i := 0; i < count; i++ {
		tx := solana.Transaction{Signatures: []solana.Signature{{byte(slot), byte(i), 3}}, Message: solana.Message{Header: solana.MessageHeader{NumRequiredSignatures: 1}, AccountKeys: []solana.PublicKey{{1}}}}
		_, err := tx.Message.SetVersion(version)
		require.NoError(t, err)
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		block.Records = append(block.Records, Record{FirstSignature: [64]byte(tx.Signatures[0]), Transaction: wire, Metadata: []byte{byte(i), 4}})
	}
	return block
}

func testStore(t *testing.T, limits Limits) (*Store, vfs.FS) {
	t.Helper()
	fs := vfs.NewMem()
	store, err := openStore("history", [32]byte{9}, limits, fs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, fs
}

func TestStoreWriteReadReopen(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0, solana.MessageVersionV1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			store, fs := testStore(t, DefaultLimits())
			block := testBlock(t, 8, 3, version)
			require.NoError(t, store.WriteBlock(block))
			require.NoError(t, store.WriteBlock(block))
			view, err := store.Snapshot()
			require.NoError(t, err)
			got, err := view.Block(block.ID)
			require.NoError(t, err)
			require.Equal(t, block, got)
			for i, want := range block.Records {
				refs, err := view.SignatureRefs(want.FirstSignature)
				require.NoError(t, err)
				require.Equal(t, []Ref{{Block: block.ID, Position: uint32(i), FirstSignature: want.FirstSignature}}, refs)
				record, err := view.Transaction(refs[0])
				require.NoError(t, err)
				require.Equal(t, want, record)
				record.Metadata[0] ^= 255
				record.Transaction[0] ^= 255
			}
			require.NoError(t, view.Close())
			require.NoError(t, store.Close())
			reopened, err := openStore("history", [32]byte{9}, DefaultLimits(), fs)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			view, err = reopened.Snapshot()
			require.NoError(t, err)
			got, err = view.Block(block.ID)
			require.NoError(t, err)
			require.Equal(t, block, got)
			require.NoError(t, view.Close())
		})
	}
}

func TestStoreEmptyBlockAndInputOwnership(t *testing.T) {
	store, _ := testStore(t, DefaultLimits())
	empty := testBlock(t, 8, 0, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(empty))
	block := testBlock(t, 9, 1, solana.MessageVersionLegacy)
	want := ownedRecord(block.Records[0])
	require.NoError(t, store.WriteBlock(block))
	block.Records[0].Metadata[0] ^= 255
	block.Records[0].Transaction[0] ^= 255
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	got, err := view.Block(empty.ID)
	require.NoError(t, err)
	require.Equal(t, empty, got)
	got, err = view.Block(block.ID)
	require.NoError(t, err)
	require.Equal(t, want, got.Records[0])
}

func TestStoreBlockHeaderSnapshotAndOwnership(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, _ := testStore(t, DefaultLimits())
			block := testBlock(t, 8, count, solana.MessageVersionLegacy)
			block.PreviousBlockhash, block.ParentBankhash = [32]byte{7, 4}, [32]byte{7, 5}
			require.NoError(t, store.WriteBlock(block))
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			want := BlockHeader{
				ID: block.ID, ParentSlot: block.ParentSlot,
				PreviousBlockhash: block.PreviousBlockhash, Bankhash: block.Bankhash,
				ParentBankhash: block.ParentBankhash, CaptureVersion: CaptureVersion,
				TransactionCount: uint32(count),
			}
			got, err := view.BlockHeader(block.ID)
			require.NoError(t, err)
			require.Equal(t, want, got)
			got.ID.Blockhash[0] ^= 255
			got.PreviousBlockhash[0] ^= 255
			got.Bankhash[0] ^= 255
			got.ParentBankhash[0] ^= 255
			got.TransactionCount++
			removed, err := store.PruneBefore(9, 1)
			require.NoError(t, err)
			require.Equal(t, uint32(1), removed)
			got, err = view.BlockHeader(block.ID)
			require.NoError(t, err)
			require.Equal(t, want, got)
			fresh, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, fresh.Close()) }()
			got, err = fresh.BlockHeader(block.ID)
			require.ErrorIs(t, err, ErrUnavailable)
			require.Equal(t, BlockHeader{}, got)
			require.NoError(t, view.Close())
			got, err = view.BlockHeader(block.ID)
			require.ErrorIs(t, err, ErrClosed)
			require.Equal(t, BlockHeader{}, got)
		})
	}
}

func TestStoreBlockHeaderDeclarationOnly(t *testing.T) {
	store, _ := testStore(t, DefaultLimits())
	block := testBlock(t, 8, 1, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(block))
	// Deliberate external corruption distinguishes a header declaration from
	// complete payload validation; ordinary store writes never split a block.
	require.NoError(t, store.db.Delete(transactionKey(block.ID, 0), pebble.Sync))
	require.NoError(t, store.db.Delete(signatureKey(Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature}), pebble.Sync))
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	header, err := view.BlockHeader(block.ID)
	require.NoError(t, err)
	require.Equal(t, uint32(1), header.TransactionCount)
	require.Equal(t, block.ID, header.ID)
	_, err = view.Block(block.ID)
	require.ErrorIs(t, err, ErrUnavailable)
	wrongID := block.ID
	wrongID.Blockhash[0] ^= 255
	header, err = view.BlockHeader(wrongID)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Equal(t, BlockHeader{}, header)
}

func TestStoreBlockHeaderRejectsMalformedCompletion(t *testing.T) {
	for _, name := range []string{"length", "format", "identity", "capture", "count", "bytes", "empty-count"} {
		t.Run(name, func(t *testing.T) {
			store, _ := testStore(t, DefaultLimits())
			block := testBlock(t, 8, 1, solana.MessageVersionLegacy)
			require.NoError(t, store.WriteBlock(block))
			c, err := prepareBlock(block, DefaultLimits())
			require.NoError(t, err)
			value := encodeCompletion(c)
			switch name {
			case "length":
				value = value[:len(value)-1]
			case "format":
				binary.BigEndian.PutUint32(value[:4], formatVersion+1)
			case "identity":
				value[12] ^= 255
			case "capture":
				binary.BigEndian.PutUint32(value[148:152], CaptureVersion+1)
			case "count":
				binary.BigEndian.PutUint32(value[152:156], DefaultLimits().MaxTransactions+1)
			case "bytes":
				binary.BigEndian.PutUint64(value[156:164], DefaultLimits().MaxBlockBytes+1)
			case "empty-count":
				binary.BigEndian.PutUint32(value[152:156], 0)
			}
			require.NoError(t, store.db.Set(blockKey('c', block.ID), value, pebble.Sync))
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			header, err := view.BlockHeader(block.ID)
			require.ErrorIs(t, err, ErrUnavailable)
			require.Equal(t, BlockHeader{}, header)
		})
	}
}

func TestStorePreflightLimits(t *testing.T) {
	block := testBlock(t, 8, 2, solana.MessageVersionLegacy)
	exactBatch := uint64(12) + setSize(41, completionSize)
	for _, record := range block.Records {
		exactBatch += setSize(45, recordSize(record)) + setSize(109, 64)
	}
	for _, name := range []string{"record", "block", "batch", "count", "contract", "signature", "empty-metadata"} {
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			candidate := testBlock(t, 8, 2, solana.MessageVersionLegacy)
			switch name {
			case "record":
				limits.MaxRecordBytes = recordSize(candidate.Records[0]) - 1
			case "block":
				limits.MaxBlockBytes = 2*recordSize(candidate.Records[0]) - 1
			case "batch":
				limits.MaxBatchBytes = exactBatch - 1
			case "count":
				limits.MaxTransactions = 1
			case "contract":
				candidate.CaptureVersion++
			case "signature":
				candidate.Records[0].FirstSignature[0] ^= 255
			case "empty-metadata":
				candidate.Records[0].Metadata = nil
			}
			store, _ := testStore(t, limits)
			store.commit = func(*pebble.Batch) error { t.Fatal("preflight reached commit"); return nil }
			require.Error(t, store.WriteBlock(candidate))
			view, err := store.Snapshot()
			require.NoError(t, err)
			_, err = view.Block(candidate.ID)
			require.ErrorIs(t, err, ErrUnavailable)
			require.NoError(t, view.Close())
		})
	}
	limits := DefaultLimits()
	limits.MaxBatchBytes = exactBatch
	store, _ := testStore(t, limits)
	store.commit = func(batch *pebble.Batch) error {
		require.Equal(t, exactBatch, uint64(len(batch.Repr())))
		return batch.Commit(pebble.Sync)
	}
	require.NoError(t, store.WriteBlock(block))
}

func TestStoreCorruptionFailsClosed(t *testing.T) {
	for _, name := range []string{"body", "reference", "extra-body", "length", "count", "digest"} {
		t.Run(name, func(t *testing.T) {
			store, _ := testStore(t, DefaultLimits())
			block := testBlock(t, 8, 2, solana.MessageVersionLegacy)
			require.NoError(t, store.WriteBlock(block))
			ref := Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature}
			switch name {
			case "body":
				require.NoError(t, store.db.Delete(transactionKey(block.ID, 0), pebble.Sync))
			case "reference":
				require.NoError(t, store.db.Delete(signatureKey(ref), pebble.Sync))
			case "extra-body":
				require.NoError(t, store.db.Set(transactionKey(block.ID, 2), encodeRecord(block.Records[0]), pebble.Sync))
			case "length":
				value := encodeRecord(block.Records[0])
				binary.BigEndian.PutUint32(value[68:72], ^uint32(0))
				require.NoError(t, store.db.Set(transactionKey(block.ID, 0), value, pebble.Sync))
			case "count", "digest":
				c, err := prepareBlock(block, DefaultLimits())
				require.NoError(t, err)
				if name == "count" {
					c.count = ^uint32(0)
				} else {
					c.digest[0] ^= 255
				}
				require.NoError(t, store.db.Set(blockKey('c', block.ID), encodeCompletion(c), pebble.Sync))
			}
			view, err := store.Snapshot()
			require.NoError(t, err)
			_, err = view.Block(block.ID)
			require.ErrorIs(t, err, ErrUnavailable)
			if name != "extra-body" {
				_, err = view.Transaction(ref)
				require.ErrorIs(t, err, ErrUnavailable)
			}
			require.NoError(t, view.Close())
		})
	}
}

func TestStorePruneSnapshotAndReopen(t *testing.T) {
	store, fs := testStore(t, DefaultLimits())
	first, second := testBlock(t, 8, 2, solana.MessageVersionLegacy), testBlock(t, 9, 2, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(first))
	require.NoError(t, store.WriteBlock(second))
	old, err := store.Snapshot()
	require.NoError(t, err)
	removed, err := store.PruneBefore(9, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), removed)
	got, err := old.Block(first.ID)
	require.NoError(t, err)
	require.Equal(t, first, got)
	require.NoError(t, old.Close())
	require.NoError(t, store.Close())
	reopened, err := openStore("history", [32]byte{9}, DefaultLimits(), fs)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, err = view.Block(first.ID)
	require.ErrorIs(t, err, ErrUnavailable)
	refs, err := view.SignatureRefs(first.Records[0].FirstSignature)
	require.NoError(t, err)
	require.Empty(t, refs)
	got, err = view.Block(second.ID)
	require.NoError(t, err)
	require.Equal(t, second, got)
}

func TestStoreAmbiguousWritePoisonsNewViews(t *testing.T) {
	store, fs := testStore(t, DefaultLimits())
	stable := testBlock(t, 7, 1, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(stable))
	old, err := store.Snapshot()
	require.NoError(t, err)
	block := testBlock(t, 8, 1, solana.MessageVersionLegacy)
	injected := errors.New("acknowledgement lost after visible commit")
	store.commit = func(batch *pebble.Batch) error {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		return injected
	}
	require.ErrorIs(t, store.WriteBlock(block), injected)
	_, err = store.Snapshot()
	require.ErrorIs(t, err, ErrUnavailable)
	require.ErrorIs(t, store.WriteBlock(block), ErrUnavailable)
	_, err = store.PruneBefore(9, 1)
	require.ErrorIs(t, err, ErrUnavailable)
	_, err = old.Block(block.ID)
	require.ErrorIs(t, err, ErrUnavailable)
	got, err := old.Block(stable.ID)
	require.NoError(t, err)
	require.Equal(t, stable, got)
	require.NoError(t, old.Close())
	require.NoError(t, store.Close())
	// A clean reopen may recover a complete candidate; this is not evidence
	// of the earlier acknowledgement or canonical-chain eligibility.
	reopened, err := openStore("history", [32]byte{9}, DefaultLimits(), fs)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Snapshot()
	require.NoError(t, err)
	_, err = view.Block(block.ID)
	require.NoError(t, err)
	require.NoError(t, view.Close())
}

func TestStorePruneProgressBoundsAndCorruption(t *testing.T) {
	store, fs := testStore(t, DefaultLimits())
	first := testBlock(t, 8, 2, solana.MessageVersionLegacy)
	second := testBlock(t, 9, 2, solana.MessageVersionLegacy)
	last := testBlock(t, ^uint64(0), 0, solana.MessageVersionLegacy)
	for _, block := range []Block{first, second, last} {
		require.NoError(t, store.WriteBlock(block))
	}
	for _, limit := range []uint32{0, 65} {
		_, err := store.PruneBefore(10, limit)
		require.ErrorIs(t, err, ErrLimit)
	}
	require.NoError(t, store.Close())
	small := DefaultLimits()
	small.MaxBatchBytes = 12 + 2*(deleteSize(45)+deleteSize(109)) + deleteSize(41) - 1
	reopened, err := openStore("history", [32]byte{9}, small, fs)
	require.NoError(t, err)
	removed, err := reopened.PruneBefore(10, 1)
	require.ErrorIs(t, err, ErrLimit)
	require.Zero(t, removed)
	view, err := reopened.Snapshot()
	require.NoError(t, err)
	got, err := view.Block(first.ID)
	require.NoError(t, err)
	require.Equal(t, first, got)
	require.NoError(t, view.Close())
	require.NoError(t, reopened.Close())
	reopened, err = openStore("history", [32]byte{9}, DefaultLimits(), fs)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	for range 2 {
		removed, err = reopened.PruneBefore(^uint64(0), 1)
		require.NoError(t, err)
		require.Equal(t, uint32(1), removed)
	}
	removed, err = reopened.PruneBefore(^uint64(0), 1)
	require.NoError(t, err)
	require.Zero(t, removed)
	view, err = reopened.Snapshot()
	require.NoError(t, err)
	got, err = view.Block(last.ID)
	require.NoError(t, err)
	require.Equal(t, last, got)
	require.NoError(t, view.Close())
	require.NoError(t, reopened.WriteBlock(first))
	c, err := prepareBlock(first, DefaultLimits())
	require.NoError(t, err)
	c.digest[0] ^= 255
	require.NoError(t, reopened.db.Set(blockKey('c', first.ID), encodeCompletion(c), pebble.Sync))
	removed, err = reopened.PruneBefore(9, 1)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Zero(t, removed)
	// A failed validation never commits the partially assembled deletion batch.
	_, closer, err := reopened.db.Get(transactionKey(first.ID, 0))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
}

func TestStoreForeignDirectoryRejected(t *testing.T) {
	fs := vfs.NewMem()
	require.NoError(t, fs.MkdirAll("foreign", 0o700))
	file, err := fs.Create("foreign/important")
	require.NoError(t, err)
	_, err = file.Write([]byte("not a history database"))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	store, err := openStore("foreign", [32]byte{9}, DefaultLimits(), fs)
	require.Error(t, err)
	require.Nil(t, store)
	info, err := fs.Stat("foreign/important")
	require.NoError(t, err)
	require.Equal(t, int64(22), info.Size())
}

func TestStoreProcessCrashRecovery(t *testing.T) {
	if mode := os.Getenv("MITHRIL_HISTORY_CRASH_MODE"); mode != "" {
		store, err := Open(os.Getenv("MITHRIL_HISTORY_CRASH_DIR"), [32]byte{9}, DefaultLimits())
		require.NoError(t, err)
		require.NoError(t, store.WriteBlock(testBlock(t, 7, 1, solana.MessageVersionLegacy)))
		if mode == "before" {
			store.commit = func(*pebble.Batch) error { os.Exit(23); return nil }
		}
		if mode == "visible" {
			store.commit = func(batch *pebble.Batch) error {
				require.NoError(t, batch.Commit(pebble.NoSync))
				os.Exit(24)
				return nil
			}
		}
		require.NoError(t, store.WriteBlock(testBlock(t, 8, 2, solana.MessageVersionLegacy)))
		os.Exit(25) // Deliberately bypass Close after a synchronous acknowledgement.
	}
	for i, mode := range []string{"before", "visible", "after"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreProcessCrashRecovery$")
			cmd.Env = append(os.Environ(), "MITHRIL_HISTORY_CRASH_MODE="+mode, "MITHRIL_HISTORY_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "%s", output)
			require.Equal(t, 23+i, exit.ExitCode(), "%s", output)
			store, err := Open(dir, [32]byte{9}, DefaultLimits())
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			_, err = view.Block(testBlock(t, 7, 1, solana.MessageVersionLegacy).ID)
			require.NoError(t, err)
			block := testBlock(t, 8, 2, solana.MessageVersionLegacy)
			got, readErr := view.Block(block.ID)
			if mode == "before" {
				require.ErrorIs(t, readErr, ErrUnavailable)
			}
			if mode == "after" {
				require.NoError(t, readErr)
			}
			// Unsynced process-exit recovery may preserve or lose the candidate;
			// this fixture is not a power-loss or device-durability simulation.
			if readErr == nil {
				require.Equal(t, block, got)
			}
			for _, record := range block.Records {
				refs, err := view.SignatureRefs(record.FirstSignature)
				require.NoError(t, err)
				if readErr == nil {
					require.Len(t, refs, 1)
					stored, err := view.Transaction(refs[0])
					require.NoError(t, err)
					require.Equal(t, record, stored)
				} else {
					require.Empty(t, refs)
				}
			}
		})
	}
}

type blockedSyncFS struct {
	vfs.FS
	beforeSync func()
}
type blockedSyncFile struct {
	vfs.File
	beforeSync func()
}

func (fs blockedSyncFS) Create(name string) (vfs.File, error) {
	file, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, ".log") {
		return blockedSyncFile{file, fs.beforeSync}, nil
	}
	return file, err
}
func (f blockedSyncFile) Sync() error     { f.beforeSync(); return f.File.Sync() }
func (f blockedSyncFile) SyncData() error { f.beforeSync(); return f.File.SyncData() }

func TestStoreSnapshotWaitsForSyncAcknowledgement(t *testing.T) {
	var armed atomic.Bool
	var once, released sync.Once
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := func() { released.Do(func() { close(release) }) }
	defer unblock()
	fs := blockedSyncFS{vfs.NewMem(), func() {
		if armed.Load() {
			once.Do(func() { close(entered) })
			<-release
		}
	}}
	store, err := openStore("history", [32]byte{9}, DefaultLimits(), fs)
	require.NoError(t, err)
	defer func() { unblock(); require.NoError(t, store.Close()) }()
	block := testBlock(t, 8, 1, solana.MessageVersionLegacy)
	armed.Store(true)
	written := make(chan error, 1)
	go func() { written <- store.WriteBlock(block) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("WAL sync was not reached")
	}
	// Pebble has already published the atomic batch, but the Store has not.
	require.Eventually(t, func() bool {
		value, closer, err := store.db.Get(blockKey('c', block.ID))
		if errors.Is(err, pebble.ErrNotFound) {
			return false
		}
		require.NoError(t, err)
		require.Len(t, value, completionSize)
		require.NoError(t, closer.Close())
		return true
	}, 5*time.Second, time.Millisecond)
	for _, key := range [][]byte{transactionKey(block.ID, 0), signatureKey(Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature})} {
		_, closer, err := store.db.Get(key)
		require.NoError(t, err)
		require.NoError(t, closer.Close())
	}
	ready := make(chan *ReadSnapshot, 1)
	readErr := make(chan error, 1)
	go func() {
		view, err := store.Snapshot()
		if err != nil {
			readErr <- err
		} else {
			ready <- view
		}
	}()
	select {
	case view := <-ready:
		unblock()
		require.NoError(t, view.Close())
		t.Fatal("snapshot crossed unacknowledged commit")
	case err := <-readErr:
		t.Fatal(err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-written)
	select {
	case view := <-ready:
		defer func() { require.NoError(t, view.Close()) }()
		got, err := view.Block(block.ID)
		require.NoError(t, err)
		require.True(t, bytes.Equal(got.Records[0].Metadata, block.Records[0].Metadata))
		require.NoError(t, view.Close())
	case err := <-readErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not follow acknowledgement")
	}
}
