package alpenglow

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wbHash(b byte) solana.Hash { var h solana.Hash; h[0] = b; return h }

func wbCert(t *testing.T, tr *ChainTracker, ct CertificateType, slot uint64, hash solana.Hash) {
	t.Helper()
	_, err := tr.ObserveCertificate(Certificate{
		Type: ct, Slot: slot, BlockHash: hash,
		SignatureVerified: true, StakeVerified: true,
	})
	require.NoError(t, err)
}

// WantedBlocks lists certified-but-unobserved blocks ascending: observed
// blocks drop out, skip-certified slots are excluded unless finalized, and
// afterSlot/max bound the scan.
func TestWantedBlocksSelection(t *testing.T) {
	tr := NewChainTracker()

	// 101: notarized, unobserved -> wanted.
	wbCert(t, tr, CertificateNotarize, 101, wbHash(1))
	// 102: notarized but replay observed the data -> not wanted.
	wbCert(t, tr, CertificateNotarize, 102, wbHash(2))
	tr.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 102, Hash: wbHash(2)}, ParentSlot: 101, ParentHash: wbHash(1)})
	// 103: skip-certified with only a fallback candidate -> excluded (a skip
	// outranks an ambiguous fallback; repair must not chase discarded data).
	wbCert(t, tr, CertificateSkip, 103, solana.Hash{})
	wbCert(t, tr, CertificateNotarizeFallback, 103, wbHash(3))
	// 104: fast-finalized, unobserved -> wanted with Finalized set.
	wbCert(t, tr, CertificateFinalizeFast, 104, wbHash(4))

	wanted := tr.WantedBlocks(100, 10)
	require.Len(t, wanted, 2)
	assert.Equal(t, BlockID{Slot: 101, Hash: wbHash(1)}, wanted[0].Block)
	assert.Equal(t, CertificateNotarize, wanted[0].Strongest)
	assert.False(t, wanted[0].Finalized)
	assert.Equal(t, BlockID{Slot: 104, Hash: wbHash(4)}, wanted[1].Block)
	assert.Equal(t, CertificateFinalizeFast, wanted[1].Strongest)
	assert.True(t, wanted[1].Finalized)

	// afterSlot is exclusive; max caps the result.
	wanted = tr.WantedBlocks(101, 10)
	require.Len(t, wanted, 1)
	assert.Equal(t, uint64(104), wanted[0].Block.Slot)
	wanted = tr.WantedBlocks(100, 1)
	require.Len(t, wanted, 1)
	assert.Equal(t, uint64(101), wanted[0].Block.Slot)

	// Observing the data satisfies the want.
	tr.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 101, Hash: wbHash(1)}, ParentSlot: 100})
	wanted = tr.WantedBlocks(100, 10)
	require.Len(t, wanted, 1)
	assert.Equal(t, uint64(104), wanted[0].Block.Slot)

	// Pruning the retention window drops the remaining want.
	tr.PruneBeforeSlot(105)
	assert.Empty(t, tr.WantedBlocks(100, 10))
}

// Repair targets the DECISIVE block per slot, not a fallback sibling that
// merely sorts first by hash. A single target is returned per slot.
func TestWantedBlocksPrefersDecisiveOverLowerHashFallback(t *testing.T) {
	tr := NewChainTracker()
	notarizeHi := wbHash(0x09) // decisive, HIGH hash
	fallbackLo := wbHash(0x01) // fallback, LOW hash — would sort first

	wbCert(t, tr, CertificateNotarizeFallback, 200, fallbackLo)
	wbCert(t, tr, CertificateNotarize, 200, notarizeHi)

	wanted := tr.WantedBlocks(199, 10)
	require.Len(t, wanted, 1, "exactly one repair target per slot")
	assert.Equal(t, notarizeHi, wanted[0].Block.Hash, "must target the decisive notarized block, not the lower-hash fallback")
	assert.Equal(t, CertificateNotarize, wanted[0].Strongest)

	// A finalized block outranks a unique-strength cert on another sibling.
	wbCert(t, tr, CertificateFinalizeFast, 201, wbHash(0xF0))
	wbCert(t, tr, CertificateNotarize, 201, wbHash(0x02))
	wanted = tr.WantedBlocks(200, 10)
	require.Len(t, wanted, 1)
	assert.Equal(t, wbHash(0xF0), wanted[0].Block.Hash, "finalized block wins")
	assert.True(t, wanted[0].Finalized)

	// A fallback-only slot still yields the fallback (no decisive candidate).
	tr2 := NewChainTracker()
	wbCert(t, tr2, CertificateNotarizeFallback, 202, wbHash(0x05))
	only := tr2.WantedBlocks(201, 10)
	require.Len(t, only, 1)
	assert.Equal(t, CertificateNotarizeFallback, only[0].Strongest, "fallback repaired when nothing decisive exists")
}

// A certified sibling stays wanted while a DIFFERENT (uncertified) sibling was
// observed — the exact post-switch repair case: replay ran the wrong block,
// certs name the right one, and its data still has to be fetched.
func TestWantedBlocksCertifiedSiblingOfObservedBlock(t *testing.T) {
	tr := NewChainTracker()

	// Replay observed sibling A (no certificate); certs then name sibling B.
	tr.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 150, Hash: wbHash(0xA)}, ParentSlot: 149})
	wbCert(t, tr, CertificateNotarize, 150, wbHash(0xB))

	wanted := tr.WantedBlocks(149, 10)
	require.Len(t, wanted, 1, "uncertified observed sibling must not satisfy the want")
	assert.Equal(t, BlockID{Slot: 150, Hash: wbHash(0xB)}, wanted[0].Block)

	// Once the certified sibling's data is observed, the want clears.
	tr.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 150, Hash: wbHash(0xB)}, ParentSlot: 149})
	assert.Empty(t, tr.WantedBlocks(149, 10))
}
