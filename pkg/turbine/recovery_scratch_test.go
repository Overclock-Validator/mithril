package turbine

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type scratchCase struct {
	name    string
	packets [][]byte
	missing int
	corrupt bool
}

func scratchFixture(t *testing.T, slot uint64, seed byte, missing int, corrupt bool) scratchCase {
	t.Helper()
	gen := ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 1}
	payload := benchmarkPayload(128)
	for i := range payload {
		payload[i] ^= seed
	}
	packets, _, _, _, err := gen.MakeShredsFromData(benchmarkLeaderKey(), payload, false, solana.Hash{seed}, 0, 0)
	require.NoError(t, err)
	require.Len(t, packets, 64)
	if corrupt {
		// A received parity shard that does not match the signed tree:
		// recovery must fail authentication, leaving scratch dirty.
		packets[63][codingHeaderSize+128] ^= 1
		resignRecoveryFixture(t, packets)
	}
	return scratchCase{name: fmt.Sprintf("slot=%d/missing=%d/corrupt=%t", slot, missing, corrupt), packets: packets, missing: missing, corrupt: corrupt}
}

type recoveredSnapshot struct {
	payloads [][]byte
	roots    []solana.Hash
	err      error
}

func runScratchRecovery(t *testing.T, a *SlotAssembler, c scratchCase) ([]*Shred, recoveredSnapshot) {
	t.Helper()
	_, state := recoveryFixtureState(t, c.packets, c.missing)
	recovered, err := a.recoverFEC(state, 0)
	snap := recoveredSnapshot{err: err}
	for _, s := range recovered {
		snap.payloads = append(snap.payloads, bytes.Clone(s.Payload))
		root, rootErr := s.MerkleRoot()
		require.NoError(t, rootErr)
		snap.roots = append(snap.roots, root)
	}
	return recovered, snap
}

// TestFECRecoveryScratchReuseMatchesFreshBuffers recovers FEC sets through
// one assembler whose scratch is reused (same set twice with different sets,
// a failed authentication and a different missing pattern in between) and
// compares every result with an assembler that allocates fresh buffers. It
// also checks that shreds returned earlier are not changed by later
// recoveries, which would mean they alias scratch.
func TestFECRecoveryScratchReuseMatchesFreshBuffers(t *testing.T) {
	a := scratchFixture(t, 10, 1, 3, false)
	b := scratchFixture(t, 11, 2, 1, false)
	bad := scratchFixture(t, 12, 3, 3, true)
	c := scratchFixture(t, 13, 4, 32, false)
	sequence := []scratchCase{a, a, b, bad, a, c, b, a}

	reused := NewSlotAssembler()

	type kept struct {
		shreds []*Shred
		snap   recoveredSnapshot
	}
	var history []kept
	for step, c := range sequence {
		gotShreds, got := runScratchRecovery(t, reused, c)
		_, want := runScratchRecovery(t, NewSlotAssembler(), c)
		if c.corrupt {
			require.ErrorIs(t, got.err, ErrInvalidSignature, "step %d %s", step, c.name)
		} else {
			require.NoError(t, got.err, "step %d %s", step, c.name)
			require.NotEmpty(t, got.payloads)
		}
		require.Equal(t, want.err != nil, got.err != nil, "step %d %s", step, c.name)
		require.Equal(t, want.payloads, got.payloads, "step %d %s", step, c.name)
		require.Equal(t, want.roots, got.roots, "step %d %s", step, c.name)
		// Recovered data must match the original packets byte for byte.
		for i, s := range gotShreds {
			original := c.packets[s.Index]
			require.Equal(t, original[:len(s.Payload)], s.Payload, "step %d shred %d", step, i)
		}
		history = append(history, kept{shreds: gotShreds, snap: got})
		for h, prev := range history {
			for i, s := range prev.shreds {
				require.Equal(t, prev.snap.payloads[i], s.Payload, "shred from step %d changed after step %d", h, step)
			}
		}
	}
}
