package transactionhistory

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func consumerBlock(t *testing.T, slot uint64, fork byte) Block {
	t.Helper()
	raw, err := txfixture.SignedTransferWire(slot)
	require.NoError(t, err)
	view, err := wire.Sanitize(raw)
	require.NoError(t, err)
	record := Record{Transaction: raw, Metadata: []byte{1, 2, 3}}
	copy(record.FirstSignature[:], view.FirstSignature())
	return Block{
		ID:         BlockID{Slot: slot, Blockhash: [32]byte{fork}},
		ParentSlot: slot - 1, PreviousBlockhash: [32]byte{7},
		Bankhash: [32]byte{fork, 8}, ParentBankhash: [32]byte{9},
		CaptureVersion: CaptureVersion, Records: []Record{record},
	}
}

func consumerStore(t *testing.T) *Store {
	t.Helper()
	store, err := openStore("history", [32]byte{1}, DefaultLimits(), vfs.NewMem())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func TestConsumerReviewSnapshotLifetimeAndOwnership(t *testing.T) {
	store := consumerStore(t)
	block := consumerBlock(t, 42, 1)
	require.NoError(t, store.WriteBlock(block))
	view, err := store.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	require.ErrorIs(t, store.Close(), ErrSnapshotsOpen)

	ref := Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature}
	first, err := view.Transaction(ref)
	require.NoError(t, err)
	first.Transaction[0] ^= 255
	first.Metadata[0] ^= 255
	second, err := view.Transaction(ref)
	require.NoError(t, err)
	require.Equal(t, block.Records[0], second)

	require.NoError(t, view.Close())
	require.NoError(t, view.Close())
	_, err = view.Block(block.ID)
	require.ErrorIs(t, err, ErrClosed)
	_, err = view.Transaction(ref)
	require.ErrorIs(t, err, ErrClosed)
	_, err = view.SignatureRefs(ref.FirstSignature)
	require.ErrorIs(t, err, ErrClosed)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
	_, err = store.Snapshot()
	require.ErrorIs(t, err, ErrClosed)
}

func TestConsumerReviewNamespaceRefusal(t *testing.T) {
	for _, damage := range []string{"genesis", "missing", "version"} {
		t.Run(damage, func(t *testing.T) {
			fs := vfs.NewMem()
			genesis := [32]byte{1}
			store, err := openStore("history", genesis, DefaultLimits(), fs)
			require.NoError(t, err)
			block := consumerBlock(t, 42, 1)
			require.NoError(t, store.WriteBlock(block))
			switch damage {
			case "missing":
				require.NoError(t, store.db.Delete(namespaceKey, pebble.Sync))
			case "version":
				value := binary.BigEndian.AppendUint32([]byte("mithril-transaction-history\x00"), formatVersion+1)
				value = append(value, genesis[:]...)
				require.NoError(t, store.db.Set(namespaceKey, value, pebble.Sync))
			}
			require.NoError(t, store.Close())
			requested := genesis
			if damage == "genesis" {
				requested[0]++
			}
			invalid, err := openStore("history", requested, DefaultLimits(), fs)
			require.Error(t, err)
			require.Nil(t, invalid)
			if damage == "genesis" {
				reopened, err := openStore("history", genesis, DefaultLimits(), fs)
				require.NoError(t, err)
				view, err := reopened.Snapshot()
				require.NoError(t, err)
				got, err := view.Block(block.ID)
				require.NoError(t, err)
				require.Equal(t, block, got)
				require.NoError(t, view.Close())
				require.NoError(t, reopened.Close())
			}
		})
	}
}

func TestConsumerReviewForkIdentityIsImmutable(t *testing.T) {
	store := consumerStore(t)
	left := consumerBlock(t, 42, 1)
	right := consumerBlock(t, 42, 2)
	require.NoError(t, store.WriteBlock(left))
	require.NoError(t, store.WriteBlock(right))
	for _, change := range []string{"bank", "parent-bank", "parent-block", "parent-slot", "body"} {
		t.Run(change, func(t *testing.T) {
			conflict := left
			switch change {
			case "bank":
				conflict.Bankhash[0]++
			case "parent-bank":
				conflict.ParentBankhash[0]++
			case "parent-block":
				conflict.PreviousBlockhash[0]++
			case "parent-slot":
				conflict.ParentSlot--
			case "body":
				conflict.Records = append([]Record(nil), left.Records...)
				conflict.Records[0].Metadata = []byte{4}
			}
			require.ErrorIs(t, store.WriteBlock(conflict), ErrConflict)
		})
	}
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	refs, err := view.SignatureRefs(left.Records[0].FirstSignature)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, left.ID, refs[0].Block)
	require.Equal(t, right.ID, refs[1].Block)
	for _, want := range []Block{left, right} {
		got, err := view.Block(want.ID)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestConsumerReviewMalformedLookupCountIsBounded(t *testing.T) {
	record := consumerBlock(t, 42, 1).Records[0]
	view, err := wire.Parse(record.Transaction)
	require.NoError(t, err)
	bad := bytes.Clone(record.Transaction[:view.MessageOffset])
	bad = append(bad, 0x80)
	bad = append(bad, record.Transaction[view.MessageOffset:]...)
	bad = append(bad, 0xff, 0xff, 0x03) // 65,535 absent address-table lookups.
	record.Transaction = bad
	require.Less(t, len(bad), 512)
	require.Error(t, validateRecord(record, DefaultLimits()))

	// A full SDK decode used to allocate 65,535 lookup structs for this tiny
	// invalid wire. This is a generous per-call allocation guard, not RSS.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = validateRecord(record, DefaultLimits())
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}

func TestConsumerReviewPointReceiptBindsHeader(t *testing.T) {
	for _, field := range []string{"bank", "parent-bank", "parent-block", "parent-slot"} {
		t.Run(field, func(t *testing.T) {
			store := consumerStore(t)
			block := consumerBlock(t, 42, 1)
			require.NoError(t, store.WriteBlock(block))
			completion, err := prepareBlock(block, DefaultLimits())
			require.NoError(t, err)
			switch field {
			case "bank":
				completion.block.Bankhash[0]++
			case "parent-bank":
				completion.block.ParentBankhash[0]++
			case "parent-block":
				completion.block.PreviousBlockhash[0]++
			case "parent-slot":
				completion.block.ParentSlot--
			}
			require.NoError(t, store.db.Set(blockKey('c', block.ID), encodeCompletion(completion), pebble.Sync))
			view, err := store.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			ref := Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature}
			_, err = view.Transaction(ref)
			require.ErrorIs(t, err, ErrUnavailable)
			_, err = view.SignatureRefs(ref.FirstSignature)
			require.ErrorIs(t, err, ErrUnavailable)
		})
	}
}

func TestConsumerReviewPointReceiptDoesNotClaimBlockCoverage(t *testing.T) {
	store := consumerStore(t)
	block := consumerBlock(t, 42, 1)
	block.Records = append(block.Records, consumerBlock(t, 43, 1).Records[0])
	require.NoError(t, store.WriteBlock(block))
	require.NoError(t, store.db.Delete(transactionKey(block.ID, 1), pebble.Sync))
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	ref := Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature}
	got, err := view.Transaction(ref)
	require.NoError(t, err)
	require.Equal(t, block.Records[0], got)
	refs, err := view.SignatureRefs(ref.FirstSignature)
	require.NoError(t, err)
	require.Equal(t, []Ref{ref}, refs)
	_, err = view.Block(block.ID)
	require.ErrorIs(t, err, ErrUnavailable)
	_, err = view.Transaction(Ref{Block: block.ID, Position: 1, FirstSignature: block.Records[1].FirstSignature})
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestConsumerReviewSignatureCandidateLimit(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxSignatureRefs = 1
	store, err := openStore("history", [32]byte{1}, limits, vfs.NewMem())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	left, right := consumerBlock(t, 42, 1), consumerBlock(t, 42, 2)
	require.NoError(t, store.WriteBlock(left))
	require.NoError(t, store.WriteBlock(right))
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	refs, err := view.SignatureRefs(left.Records[0].FirstSignature)
	require.ErrorIs(t, err, ErrLimit)
	require.Nil(t, refs)
	for _, block := range []Block{left, right} {
		got, err := view.Transaction(Ref{Block: block.ID, FirstSignature: block.Records[0].FirstSignature})
		require.NoError(t, err)
		require.Equal(t, block.Records[0], got)
	}
}
