package sbpf

import (
	"errors"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

func makeSlot(op uint8, dst uint8, src uint8, off int16, imm int32) Slot {
	return Slot(uint64(op) |
		uint64(dst&0xF)<<8 |
		uint64(src&0xF)<<12 |
		uint64(uint16(off))<<16 |
		uint64(uint32(imm))<<32)
}

func lddw(dst uint8, val uint64) (Slot, Slot) {
	lo := int32(val)
	hi := int32(val >> 32)
	return makeSlot(OpLddw, dst, 0, 0, lo), Slot(uint64(uint32(hi)) << 32)
}

func noSyscalls(_ uint32) (Syscall, bool) { return nil, false }

func newTestInterpreter(text []Slot, funcs map[uint32]int64, ver sbpfver.SbpfVersion) *Interpreter {
	p := &Program{
		Text:        text,
		TextVA:      VaddrProgram,
		Entrypoint:  0,
		Funcs:       funcs,
		SbpfVersion: ver,
	}
	meter := cu.NewComputeMeter(1000)
	opts := &VMOpts{
		HeapMax:      256 * 1024,
		Syscalls:     noSyscalls,
		ComputeMeter: &meter,
	}
	return NewInterpreter(p, opts)
}

func getException(t *testing.T, err error) *Exception {
	t.Helper()
	var exc *Exception
	require.True(t, errors.As(err, &exc), "expected *Exception, got %T: %v", err, err)
	return exc
}

func TestCallErrors(t *testing.T) {
	callxAddr := VaddrProgram + 2*8
	callxLo, callxHi := lddw(9, callxAddr)
	badLo, badHi := lddw(9, 0xDEAD_0000_0000)

	tests := []struct {
		name    string
		text    []Slot
		funcs   map[uint32]int64
		ver     sbpfver.SbpfVersion
		wantErr error
		wantPC  int64
	}{
		{
			name: "OpCall/CallDepthExceeded",
			text: []Slot{
				makeSlot(OpCall, 0, 0, 0, 1),
				makeSlot(OpCall, 0, 0, 0, 1),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			funcs:   map[uint32]int64{1: 1},
			ver:     sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0},
			wantErr: ExcCallDepth,
			wantPC:  1,
		},
		{
			name: "OpCallx/CallDepthExceeded",
			text: []Slot{
				callxLo,
				callxHi,
				makeSlot(OpCallx, 0, 0, 0, 9),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			funcs:   map[uint32]int64{},
			ver:     sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0},
			wantErr: ExcCallDepth,
			wantPC:  2,
		},
		{
			name: "OpCallx/OutOfBoundsTarget",
			text: []Slot{
				badLo,
				badHi,
				makeSlot(OpCallx, 0, 0, 0, 9),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			funcs:   map[uint32]int64{},
			ver:     sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0},
			wantErr: ExcCallOutsideTextSegment,
			wantPC:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip := newTestInterpreter(tc.text, tc.funcs, tc.ver)
			defer ip.Finish()

			_, _, err := ip.Run()
			exc := getException(t, err)
			require.ErrorIs(t, exc.Detail, tc.wantErr)
			require.Equal(t, tc.wantPC, exc.PC)
		})
	}
}

func TestSignedDivOverflow(t *testing.T) {
	v2 := sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV2}

	tests := []struct {
		name   string
		text   []Slot
		wantR0 uint64
	}{
		{
			name: "OpSdiv32Imm",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, math.MinInt32),
				makeSlot(OpSdiv32Imm, 0, 0, 0, -1),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x80000000,
		},
		{
			name: "OpSdiv32Reg",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, math.MinInt32),
				makeSlot(OpMov32Imm, 1, 0, 0, -1),
				makeSlot(OpSdiv32Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x80000000,
		},
		{
			name: "OpSdiv64Imm",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 1),
				makeSlot(OpLsh64Imm, 0, 0, 0, 63),
				makeSlot(OpSdiv64Imm, 0, 0, 0, -1),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x8000000000000000,
		},
		{
			name: "OpSdiv64Reg",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 1),
				makeSlot(OpLsh64Imm, 0, 0, 0, 63),
				makeSlot(OpMov64Imm, 1, 0, 0, -1),
				makeSlot(OpSdiv64Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x8000000000000000,
		},
		{
			name: "OpSrem32Imm",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, math.MinInt32),
				makeSlot(OpSrem32Imm, 0, 0, 0, -1),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x80000000,
		},
		{
			name: "OpSrem32Reg",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, math.MinInt32),
				makeSlot(OpMov32Imm, 1, 0, 0, -1),
				makeSlot(OpSrem32Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x80000000,
		},
		{
			name: "OpSrem64Imm",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 1),
				makeSlot(OpLsh64Imm, 0, 0, 0, 63),
				makeSlot(OpSrem64Imm, 0, 0, 0, -1),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x8000000000000000,
		},
		{
			name: "OpSrem64Reg",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 1),
				makeSlot(OpLsh64Imm, 0, 0, 0, 63),
				makeSlot(OpMov64Imm, 1, 0, 0, -1),
				makeSlot(OpSrem64Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
			wantR0: 0x8000000000000000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip := newTestInterpreter(tc.text, map[uint32]int64{}, v2)
			defer ip.Finish()

			_, _, err := ip.Run()
			exc := getException(t, err)
			require.ErrorIs(t, exc.Detail, ExcDivideOverflow)
			require.Equal(t, tc.wantR0, exc.R[0], "r0 should not be modified on overflow")
		})
	}
}

func TestSignedDivByZero(t *testing.T) {
	v2 := sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV2}

	tests := []struct {
		name string
		text []Slot
	}{
		{
			name: "OpSdiv32Reg",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, 42),
				makeSlot(OpMov32Imm, 1, 0, 0, 0),
				makeSlot(OpSdiv32Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
		},
		{
			name: "OpSdiv64Reg",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 42),
				makeSlot(OpMov64Imm, 1, 0, 0, 0),
				makeSlot(OpSdiv64Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
		},
		{
			name: "OpSrem32Reg",
			text: []Slot{
				makeSlot(OpMov32Imm, 0, 0, 0, 42),
				makeSlot(OpMov32Imm, 1, 0, 0, 0),
				makeSlot(OpSrem32Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
		},
		{
			name: "OpSrem64Reg",
			text: []Slot{
				makeSlot(OpMov64Imm, 0, 0, 0, 42),
				makeSlot(OpMov64Imm, 1, 0, 0, 0),
				makeSlot(OpSrem64Reg, 0, 1, 0, 0),
				makeSlot(OpExit, 0, 0, 0, 0),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip := newTestInterpreter(tc.text, map[uint32]int64{}, v2)
			defer ip.Finish()

			_, _, err := ip.Run()
			exc := getException(t, err)
			require.ErrorIs(t, exc.Detail, ExcDivideByZero)
			require.Equal(t, uint64(42), exc.R[0], "r0 should not be modified on divide by zero")
		})
	}
}
