package sbpf

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

// These tests guard the invariant Stack.Finish's partial clear rests on: a
// buffer handed back to the pool is entirely zero, so the next program cannot
// observe bytes written by the previous one.
//
// Under-clearing is a silent data leak across programs and a consensus
// divergence -- a program reading stack it never wrote would see another
// program's values instead of the zeros mainnet gives it. Nothing else in the
// suite would notice, because every existing test writes stack before reading
// it. That is what these are for.

// residuePattern is deliberately not zero-like, so a missed clear shows up.
var residuePattern = [8]byte{0xde, 0xad, 0xbe, 0xef, 0x5a, 0x5a, 0x5a, 0x5a}

func requireStackBufferZero(t *testing.T, buf []byte, context string) {
	t.Helper()
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("%s: residue at byte %d of %d: %#x", context, i, len(buf), b)
		}
	}
}

// TestStackFinishClearsEveryMarkedWrite drives Stack directly, so it pins
// Finish against markWritten without depending on how the interpreter computes
// the mark.
func TestStackFinishClearsEveryMarkedWrite(t *testing.T) {
	versions := []struct {
		name    string
		version uint32
		gaps    bool
	}{
		{"v3-nogaps", sbpfver.SbpfVersionV3, false},
		{"v0-gaps", sbpfver.SbpfVersionV0, false},
		{"v0-gaps-disabled", sbpfver.SbpfVersionV0, true},
		{"v1-dynamic", sbpfver.SbpfVersionV1, false},
	}

	// Offsets spanning the first frame, a frame boundary, deep memory, and the
	// very last addressable bytes.
	offsets := []uint64{0, 8, StackFrameSize - 8, StackFrameSize, 100_000, StackMax - 8}

	for _, v := range versions {
		for _, off := range offsets {
			t.Run(v.name, func(t *testing.T) {
				s := NewStack(sbpfver.SbpfVersion{Version: v.version}, v.gaps)
				buf := s.mem[:StackMax]
				requireStackBufferZero(t, buf, "buffer was not zero when handed out")

				mem := s.GetFrame(uint32(off))
				if mem == nil || len(mem) < len(residuePattern) {
					t.Skipf("offset %d not addressable for %s", off, v.name)
				}
				copy(mem, residuePattern[:])
				s.markWritten(StackMax - uint64(len(mem)) + uint64(len(residuePattern)))

				s.Finish()
				requireStackBufferZero(t, buf, "residue survived Finish")
			})
		}
	}
}

// TestInterpreterLeavesNoStackResidue is the end-to-end half: it runs real
// programs whose stores land at chosen depths, so it also covers whether
// translateInternal computes the mark correctly, including gap remapping.
func TestInterpreterLeavesNoStackResidue(t *testing.T) {
	// storeAtDepth writes eight bytes `delta` above the initial frame pointer.
	// r10 is the frame pointer, so r1 = r10 + delta addresses further up the
	// stack than the entry frame, which is what makes the deep cases deep.
	storeAtDepth := func(delta uint32) []Slot {
		return []Slot{
			testSlot(OpMov64Imm, 1, 0, 0, delta),
			testSlot(OpAdd64Reg, 1, 10, 0, 0),
			testSlot(OpMov64Imm, 2, 0, 0, 0x5a5a5a5a),
			testSlot(OpStxdw, 1, 2, 0, 0),
			testSlot(OpExit, 0, 0, 0, 0),
		}
	}

	cases := []struct {
		name string
		text []Slot
	}{
		{"no-stack-write", []Slot{testSlot(OpExit, 0, 0, 0, 0)}},
		{"below-frame-pointer", []Slot{
			testSlot(OpMov64Imm, 1, 0, 0, 0x5a5a5a5a),
			testSlot(OpStxdw, 10, 1, -8, 0),
			testSlot(OpExit, 0, 0, 0, 0),
		}},
		{"one-frame-up", storeAtDepth(StackFrameSize)},
		{"deep", storeAtDepth(90_000)},
		{"near-top", storeAtDepth(StackMax - StackFrameSize - 16)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			program := testV3Program(tc.text, nil)
			require.NoError(t, program.Verify())

			meter := cu.NewComputeMeter(1000)
			ip := NewInterpreter(program, &VMOpts{
				HeapMax:      1024,
				Syscalls:     noSyscalls(),
				ComputeMeter: &meter,
			})
			buf := ip.stack.mem[:StackMax]
			requireStackBufferZero(t, buf, "buffer was not zero when handed out")

			_, _, err := ip.Run()
			require.NoError(t, err, "program must run to completion")

			// The store really has to have landed, or this test proves nothing.
			if tc.name != "no-stack-write" {
				require.NotZero(t, ip.stack.written, "no stack write was recorded")
			}

			ip.Finish()
			requireStackBufferZero(t, buf, "residue survived Finish")
		})
	}
}

// TestStackWrittenTracksOnlyWrites checks that reads do not inflate the mark.
// If they did the clear would still be correct, just needlessly large, and the
// whole point of the change would quietly erode.
func TestStackWrittenTracksOnlyWrites(t *testing.T) {
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, 90_000),
		testSlot(OpAdd64Reg, 1, 10, 0, 0),
		testSlot(OpLdxdw, 2, 1, 0, 0), // deep read, no write
		testSlot(OpMov64Imm, 3, 0, 0, 0x11111111),
		testSlot(OpStxdw, 10, 3, -8, 0), // shallow write
		testSlot(OpExit, 0, 0, 0, 0),
	}
	program := testV3Program(text, nil)
	require.NoError(t, program.Verify())

	meter := cu.NewComputeMeter(1000)
	ip := NewInterpreter(program, &VMOpts{
		HeapMax:      1024,
		Syscalls:     noSyscalls(),
		ComputeMeter: &meter,
	})
	buf := ip.stack.mem[:StackMax]
	_, _, err := ip.Run()
	require.NoError(t, err)

	require.LessOrEqual(t, ip.stack.written, uint64(StackFrameSize),
		"a deep read inflated the written high-water mark")

	ip.Finish()
	requireStackBufferZero(t, buf, "residue survived Finish")
}
