package turbine

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"github.com/gagliardetto/solana-go"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/stretchr/testify/require"
)

// referenceHydrateSlot is hydrateSlot before data-first ordering and the
// reused read buffer: spool order, a fresh read per call.
func referenceHydrateSlot(r *UDPReceiver, slot uint64) *slotCompletionWork {
	packets, err := r.spool.ReadSlot(slot)
	if err != nil {
		return nil
	}
	for _, pkt := range packets {
		shred, perr := ParseShred(pkt)
		if perr != nil {
			continue
		}
		candidate, aerr := r.assembler.addShredFrom(shred, false)
		if aerr != nil || candidate == nil {
			continue
		}
		return candidate
	}
	return nil
}

func finishHydrated(t *testing.T, r *UDPReceiver, work *slotCompletionWork) *block.Block {
	t.Helper()
	require.NotNil(t, work, "slot did not complete")
	processed := r.assembler.processCompletion(context.Background(), work)
	blk, err := r.assembler.finalizeCompletion(work, processed)
	require.NoError(t, err)
	require.NotNil(t, blk)
	return blk
}

func requireSameBlock(t *testing.T, want, got *block.Block) {
	t.Helper()
	require.Equal(t, want.Slot, got.Slot)
	require.Equal(t, want.AlpenglowBlockID, got.AlpenglowBlockID)
	require.Equal(t, want.AlpenglowLastChainedRoot, got.AlpenglowLastChainedRoot)
	require.Equal(t, want.Blockhash, got.Blockhash)
	require.Equal(t, want.ExpectedBankhash, got.ExpectedBankhash)
	require.Equal(t, len(want.Entries), len(got.Entries))
	require.Equal(t, len(want.Transactions), len(got.Transactions))
	for i := range want.Transactions {
		require.Equal(t, want.Transactions[i].Signatures, got.Transactions[i].Signatures)
	}
	for i := range want.Entries {
		require.Equal(t, want.Entries[i].Hash, got.Entries[i].Hash)
		require.Equal(t, want.Entries[i].NumHashes, got.Entries[i].NumHashes)
	}
}

// dropDataShreds removes every third data shred, which erasure recovery from
// the coding shreds must restore.
func dropDataShreds(t *testing.T, packets [][]byte) [][]byte {
	t.Helper()
	var out [][]byte
	dropped := 0
	for _, p := range packets {
		s, err := ParseShred(p)
		require.NoError(t, err)
		if s.Type == ShredTypeData && s.Index%3 == 1 {
			dropped++
			continue
		}
		out = append(out, p)
	}
	require.NotZero(t, dropped)
	return out
}

// Hydrating data shreds first, from a reused read buffer, yields the same
// block as the previous spool-order hydration: for complete slots, for slots
// that need erasure recovery, and across back-to-back slots that reuse the
// buffer (including a larger slot followed by a smaller one).
func TestHydrateSlotMatchesSpoolOrderHydration(t *testing.T) {
	full := hydrateFixturePackets(t, 700, 1200)
	small := hydrateFixturePackets(t, 701, 150)
	gappy := dropDataShreds(t, hydrateFixturePackets(t, 702, 600))
	slots := map[uint64][][]byte{700: full, 701: small, 702: gappy}

	spoolFor := func() *UDPReceiver {
		spool, err := OpenShredSpool(t.TempDir(), 0)
		require.NoError(t, err)
		t.Cleanup(spool.Close)
		for slot, packets := range slots {
			for _, p := range packets {
				spool.Append(slot, p)
			}
		}
		r := NewUDPReceiver("127.0.0.1:0")
		r.SetShredSpool(spool)
		return r
	}
	reference := spoolFor()
	want := map[uint64]*block.Block{}
	for slot := range slots {
		want[slot] = finishHydrated(t, reference, referenceHydrateSlot(reference, slot))
	}

	r := spoolFor()

	for _, slot := range []uint64{700, 701, 702, 700, 702, 701} {
		r.assembler.ResetSlot(slot)
		work, ok := r.hydrateSlot(slot)
		require.True(t, ok)
		requireSameBlock(t, want[slot], finishHydrated(t, r, work))
	}
}

// A slot whose data shreds are all present completes without any coding
// shred entering the assembler.
func TestHydrateSlotSkipsCodingWhenDataComplete(t *testing.T) {
	packets := hydrateFixturePackets(t, 710, 300)
	r := newHydrateBenchReceiver(t, 710, packets)

	work, ok := r.hydrateSlot(710)
	require.True(t, ok)
	require.NotNil(t, work)
	r.assembler.mu.Lock()
	defer r.assembler.mu.Unlock()
	state := r.assembler.slots[710]
	require.NotNil(t, state)
	for _, fec := range state.fecSets {
		require.Empty(t, fec.coding)
	}
}

// Packets returned by ReadSlotInto are only valid until the next call; a
// read of a smaller slot must not expose bytes of the earlier, larger one.
func TestReadSlotIntoMatchesReadSlot(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	spool.Append(1, []byte("first slot, longer packet"))
	spool.Append(1, []byte("second"))
	spool.Append(2, []byte("x"))
	var buf SpoolReadBuffer
	for _, slot := range []uint64{1, 2, 1, 2} {
		want, err := spool.ReadSlot(slot)
		require.NoError(t, err)
		got, err := spool.ReadSlotInto(slot, &buf)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err = spool.ReadSlotInto(99, &buf)
	require.Error(t, err)
}

// Strict verification must still visit a rejected coding packet preceding a
// complete data set, even though the trusted live spool can skip the coding pass.
func TestBufferedHydrationPreservesSignedSpoolRejections(t *testing.T) {
	const slot = 721
	packets := hydrateFixturePackets(t, slot, 200)
	var bad []byte
	for _, p := range packets {
		if spooledCodingPacket(p) {
			bad = bytes.Clone(p)
			bad[0] ^= 0x80
			break
		}
	}
	require.NotEmpty(t, bad)
	var seed [ed25519.SeedSize]byte
	seed[0] = 211
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:])).PublicKey()
	oldVerify, oldHook := VerifySpooledShreds, OnSpooledShredRejected
	t.Cleanup(func() { VerifySpooledShreds, OnSpooledShredRejected = oldVerify, oldHook })
	VerifySpooledShreds = true

	received := 0
	OnSpooledShredRejected = func(s uint64, err error) { require.Equal(t, uint64(slot), s); require.Error(t, err); received++ }
	r := newHydrateBenchReceiver(t, slot, append([][]byte{bad}, packets...))

	r.SetLeaderForSlot(func(s uint64) (solana.PublicKey, bool) { return leader, s == slot })
	work, ok := r.hydrateSlot(slot)
	require.True(t, ok)
	finishHydrated(t, r, work)
	require.Equal(t, 1, received)
	require.Equal(t, uint64(1), r.signatureErrors.Load())
}
