package sealevel

import (
	"crypto/sha512"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

// sol_sha512 hashes the concatenated slices into a 64-byte digest and charges
// exactly what sol_sha256 charges for the same slices.
func TestSyscallSha512(t *testing.T) {
	const (
		descOffset   = 0
		dataOffset   = 64
		resultOffset = 512
	)
	slices := [][]byte{[]byte("hello "), make([]byte, 300)}
	for i := range slices[1] {
		slices[1][i] = byte(i)
	}
	input := make([]byte, 1024)
	off := uint64(dataOffset)
	for i, s := range slices {
		binary.LittleEndian.PutUint64(input[descOffset+16*i:], sbpf.VaddrInput+off)
		binary.LittleEndian.PutUint64(input[descOffset+16*i+8:], uint64(len(s)))
		copy(input[off:], s)
		off += uint64(len(s))
	}

	vm, execCtx := newMemSyscallVM(t, input, nil)
	ret, err := SyscallSha512Impl(vm, sbpf.VaddrInput+descOffset, uint64(len(slices)), sbpf.VaddrInput+resultOffset)
	require.NoError(t, err)
	require.Zero(t, ret)
	want := sha512.Sum512(append(append([]byte{}, slices[0]...), slices[1]...))
	require.Equal(t, want[:], input[resultOffset:resultOffset+sha512.Size])
	sha512CU := execCtx.ComputeMeter.Used()
	require.Equal(t, uint64(cu.CUSha256BaseCost+cu.CUMemOpBaseCost+150), sha512CU)

	vm256, ctx256 := newMemSyscallVM(t, input, nil)
	_, err = SyscallSha256Impl(vm256, sbpf.VaddrInput+descOffset, uint64(len(slices)), sbpf.VaddrInput+resultOffset)
	require.NoError(t, err)
	require.Equal(t, ctx256.ComputeMeter.Used(), sha512CU)

	// The 64-byte result must be writable in full.
	vm, _ = newMemSyscallVM(t, input, nil)
	_, err = SyscallSha512Impl(vm, sbpf.VaddrInput+descOffset, uint64(len(slices)), sbpf.VaddrInput+uint64(len(input))-32)
	require.Error(t, err)

	// More slices than the sha256 limit fail before charging.
	vm, execCtx = newMemSyscallVM(t, input, nil)
	_, err = SyscallSha512Impl(vm, sbpf.VaddrInput, cu.CUSha256MaxSlices+1, sbpf.VaddrInput+resultOffset)
	require.Error(t, err)
	require.Zero(t, execCtx.ComputeMeter.Used())
}
