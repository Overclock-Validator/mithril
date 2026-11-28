package sbpf

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

// SIMD-0460 keeps the same physical stack bytes but compacts SBPFv0's virtual
// stack into one range. Frame boundaries become ordinary byte boundaries.
func TestAgaveVasaLegacyStackCompactionAtVmAccess(t *testing.T) {
	legacy := newBufferTestVM(sbpfver.SbpfVersionV0, false, 4*1024)
	defer legacy.Finish()
	compact := newBufferTestVM(sbpfver.SbpfVersionV0, true, 4*1024)
	defer compact.Finish()
	require.Len(t, legacy.stack.mem, StackMax)
	require.Len(t, compact.stack.mem, StackMax)

	firstEnd := VaddrStack + StackFrameSize - 1
	require.NoError(t, compact.Write8(firstEnd, 0xa1))
	require.NoError(t, compact.Write8(firstEnd+1, 0xb2))
	across, err := compact.Translate(firstEnd, 2, false)
	require.NoError(t, err)
	require.Equal(t, []byte{0xa1, 0xb2}, across)
	// Agave's solana-sbpf v0.24.0 checks whether the starting address is in a
	// legacy gap, then maps the requested length in contiguous host memory.
	// An access starting in the frame can therefore reach the next frame.
	require.NoError(t, legacy.Write8(firstEnd, 0xa1))
	require.NoError(t, legacy.Write8(VaddrStack+StackFrameSize*2, 0xb2))
	legacyAcross, err := legacy.Translate(firstEnd, 2, false)
	require.NoError(t, err)
	require.Equal(t, []byte{0xa1, 0xb2}, legacyAcross)

	// The old second-frame address is a gap; the new second frame starts 4 KiB
	// lower. Addresses in the old upper half no longer belong to the stack.
	require.Error(t, legacy.Write8(firstEnd+1, 0xb2))
	require.NoError(t, legacy.Write8(VaddrStack+StackFrameSize*2, 0xb2))
	require.NoError(t, legacy.Write8(VaddrStack+StackMax, 0xc3))
	require.Error(t, compact.Write8(VaddrStack+StackMax, 0xc3))
}
