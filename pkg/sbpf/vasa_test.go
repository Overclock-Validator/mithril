package sbpf

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

func TestInputRegionVirtualAddressSpaceAdjustments(t *testing.T) {
	input := make([]byte, 32)
	ip := &Interpreter{
		input: input,
		inputRegions: []InputRegion{
			{Offset: 0, RegionSize: 8, AddressSpaceReserved: 8, Writable: true, AccountIndex: -1},
			{Offset: 8, RegionSize: 4, AddressSpaceReserved: 12, Writable: true, AccountIndex: 0},
			{Offset: 20, RegionSize: 4, AddressSpaceReserved: 4, Writable: false, AccountIndex: 1},
		},
	}

	_, err := ip.Read8(VaddrInput + 8 + 4)
	require.Error(t, err)

	require.NoError(t, ip.Write8(VaddrInput+8+4, 0xaa))
	require.Equal(t, uint64(12), ip.inputRegions[1].RegionSize)

	value, err := ip.Read8(VaddrInput + 8 + 4)
	require.NoError(t, err)
	require.Equal(t, uint8(0xaa), value)

	require.Error(t, ip.Write8(VaddrInput+20, 0xbb))
}

func TestInputRegionDirectMappedAccountData(t *testing.T) {
	backing := []byte{1, 2, 0, 0, 0}
	onWriteCalls := 0
	ip := &Interpreter{
		input: make([]byte, 1),
		inputRegions: []InputRegion{
			{
				Offset:               0,
				RegionSize:           2,
				AddressSpaceReserved: 5,
				Writable:             false,
				AccountIndex:         0,
				Data:                 backing[:2],
				OnWrite: func(region *InputRegion, requestedLen uint64) error {
					onWriteCalls++
					require.Equal(t, uint64(3), requestedLen)
					region.Data = backing
					region.RegionSize = uint64(len(backing))
					region.Writable = true
					return nil
				},
			},
		},
	}

	_, err := ip.Read8(VaddrInput + 2)
	require.Error(t, err)

	require.NoError(t, ip.Write8(VaddrInput+2, 0xcc))
	require.Equal(t, 1, onWriteCalls)
	require.Equal(t, uint8(0xcc), backing[2])

	data, err := ip.TranslateInput(VaddrInput, uint64(len(backing)))
	require.NoError(t, err)
	require.Equal(t, backing, data)
}

func TestStackFrameGapsCanBeDisabled(t *testing.T) {
	version := sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0}

	gapped := NewStack(version, false)
	defer gapped.Finish()
	gappedRegs := new([16]uint64)
	gappedRegs[10] = VaddrStack + StackFrameSize
	require.True(t, gapped.Push(gappedRegs, 0))
	require.Equal(t, VaddrStack+StackFrameSize*3, gappedRegs[10])
	require.Nil(t, gapped.GetFrame(StackFrameSize))

	contiguous := NewStack(version, true)
	defer contiguous.Finish()
	contiguousRegs := new([16]uint64)
	contiguousRegs[10] = VaddrStack + StackFrameSize
	require.True(t, contiguous.Push(contiguousRegs, 0))
	require.Equal(t, VaddrStack+StackFrameSize*2, contiguousRegs[10])
	require.NotNil(t, contiguous.GetFrame(StackFrameSize))
}

func TestStackFrameGapsAreLegacyOnly(t *testing.T) {
	version := sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV3}

	stack := NewStack(version, false)
	defer stack.Finish()

	regs := new([16]uint64)
	regs[10] = VaddrStack + StackFrameSize
	require.True(t, stack.Push(regs, 0))
	require.Equal(t, VaddrStack+StackFrameSize*2, regs[10])
	require.NotNil(t, stack.GetFrame(StackFrameSize))
}
