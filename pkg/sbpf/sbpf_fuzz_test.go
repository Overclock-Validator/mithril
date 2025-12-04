package sbpf

import (
	"errors"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// FuzzSlotOperations tests Slot field extraction operations for correctness
func FuzzSlotOperations(f *testing.F) {
	// Seed corpus with interesting instruction patterns
	f.Add(uint64(0x0000000000000000)) // zero instruction
	f.Add(uint64(0xFFFFFFFFFFFFFFFF)) // all bits set
	f.Add(uint64(0x1234567890ABCDEF)) // mixed pattern
	f.Add(uint64(OpLddw))             // lddw opcode
	f.Add(uint64(OpExit))             // exit opcode
	f.Add(uint64(OpCall))             // call opcode

	f.Fuzz(func(t *testing.T, slotVal uint64) {
		s := Slot(slotVal)

		// Extract fields
		op := s.Op()
		dst := s.Dst()
		src := s.Src()
		off := s.Off()
		imm := s.Imm()
		uimm := s.Uimm()

		// Verify field extraction constraints
		if dst > 0xF {
			t.Errorf("Dst() returned invalid value %d, expected <= 15", dst)
		}
		if src > 0xF {
			t.Errorf("Src() returned invalid value %d, expected <= 15", src)
		}

		// Verify opcode is just the lower 8 bits
		if op != uint8(slotVal&0xFF) {
			t.Errorf("Op() extraction mismatch: got %d, expected %d", op, uint8(slotVal&0xFF))
		}

		// Verify imm/uimm consistency
		if uint32(imm) != uimm {
			t.Errorf("Imm/Uimm mismatch: imm=%d, uimm=%d", imm, uimm)
		}

		// Verify signed/unsigned offset relationship
		if int16(uint16(off)) != off {
			t.Errorf("Offset sign extension failed: %d", off)
		}
	})
}

// FuzzIsLongIns tests the IsLongIns opcode classification
func FuzzIsLongIns(f *testing.F) {
	// Seed with all standard opcodes
	f.Add(uint8(OpLddw))
	f.Add(uint8(OpExit))
	f.Add(uint8(OpCall))
	f.Add(uint8(OpAdd64Imm))
	f.Add(uint8(0x00))
	f.Add(uint8(0xFF))

	f.Fuzz(func(t *testing.T, op uint8) {
		result := IsLongIns(op)

		// Only OpLddw should return true
		if result && op != OpLddw {
			t.Errorf("IsLongIns(%#x) returned true, but only OpLddw (%#x) should return true", op, OpLddw)
		}
		if !result && op == OpLddw {
			t.Errorf("IsLongIns(OpLddw) returned false, expected true")
		}
	})
}

// FuzzVMTranslate tests memory translation and bounds checking
func FuzzVMTranslate(f *testing.F) {
	// Seed with memory region boundaries
	f.Add(VaddrProgram, uint64(1), false)
	f.Add(VaddrStack, uint64(1), false)
	f.Add(VaddrHeap, uint64(1), true)
	f.Add(VaddrInput, uint64(1), false)
	f.Add(uint64(0), uint64(1), false)                // invalid region
	f.Add(VaddrProgram, uint64(0), false)             // zero size
	f.Add(VaddrProgram+0x1000, uint64(0x1000), false) // large read

	f.Fuzz(func(t *testing.T, addr uint64, size uint64, write bool) {
		// Limit size to prevent OOM
		if size > 1024*1024 {
			size = size % (1024 * 1024)
		}

		// Create a minimal interpreter for testing
		prog := &Program{
			RO:          make([]byte, 4096),
			Text:        make([]Slot, 512),
			TextVA:      VaddrProgram,
			Entrypoint:  0,
			SbpfVersion: sbpfver.SbpfVersion{},
		}

		meter := cu.NewComputeMeter(1000000)
		opts := &VMOpts{
			HeapMax:      32 * 1024,
			ComputeMeter: &meter,
			Input:        make([]byte, 4096),
		}

		ip := NewInterpreter(prog, opts)
		defer ip.Finish()

		mem, err := ip.Translate(addr, size, write)

		// Verify error conditions
		hi := addr >> 32
		switch hi {
		case VaddrProgram >> 32:
			if write {
				if err == nil {
					t.Error("Expected error when writing to program memory")
				}
				var excBadAccess ExcBadAccess
				if !errors.As(err, &excBadAccess) {
					t.Errorf("Expected ExcBadAccess for program write, got %T", err)
				}
			} else {
				lo := addr & math.MaxUint32
				if lo+size > uint64(len(prog.RO)) {
					if err == nil {
						t.Error("Expected error for out-of-bounds program read")
					}
				} else if size > 0 && err != nil {
					t.Errorf("Unexpected error for valid program read: %v", err)
				}
			}
		case VaddrStack >> 32:
			// Stack access should succeed if in bounds
			if size > 0 && err != nil {
				// Verify it's genuinely out of bounds
				frame := ip.stack.GetFrame(uint32(addr))
				if uint64(len(frame)) >= size && err != nil {
					t.Errorf("Valid stack access failed: addr=%#x size=%d err=%v", addr, size, err)
				}
			}
		case VaddrHeap >> 32:
			lo := addr & math.MaxUint32
			if lo+size > uint64(len(ip.heap)) {
				if err == nil {
					t.Error("Expected error for out-of-bounds heap access")
				}
			} else if size > 0 && err != nil {
				t.Errorf("Unexpected error for valid heap access: %v", err)
			}
		case VaddrInput >> 32:
			lo := addr & math.MaxUint32
			if write {
				if err == nil {
					t.Error("Expected error when writing to input memory")
				}
			} else if lo+size > uint64(len(opts.Input)) {
				if err == nil {
					t.Error("Expected error for out-of-bounds input read")
				}
			} else if size > 0 && err != nil {
				t.Errorf("Unexpected error for valid input read: %v", err)
			}
		default:
			// Invalid memory region
			if size > 0 && err == nil {
				t.Errorf("Expected error for invalid memory region %#x", addr)
			}
		}

		// If translation succeeded, verify memory slice
		if err == nil && size > 0 {
			if len(mem) != int(size) {
				t.Errorf("Translate returned wrong size: got %d, expected %d", len(mem), size)
			}
		}

		// If translation succeeded and size is 0, mem should be nil or empty
		if err == nil && size == 0 {
			if len(mem) != 0 {
				t.Errorf("Translate with size=0 should return nil or empty slice, got %v", mem)
			}
		}
	})
}

// FuzzVMReadWrite tests memory read/write operations
func FuzzVMReadWrite(f *testing.F) {
	// Seed with valid heap addresses and values
	f.Add(VaddrHeap, uint64(0x1234567890ABCDEF))
	f.Add(VaddrHeap+100, uint64(0))
	f.Add(VaddrHeap+1000, uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, baseAddr uint64, value uint64) {
		// Ensure we're in heap region
		addr := VaddrHeap + (baseAddr % (32 * 1024))

		prog := &Program{
			RO:          make([]byte, 4096),
			Text:        make([]Slot, 512),
			TextVA:      VaddrProgram,
			Entrypoint:  0,
			SbpfVersion: sbpfver.SbpfVersion{},
		}

		meter := cu.NewComputeMeter(1000000)
		opts := &VMOpts{
			HeapMax:      32 * 1024,
			ComputeMeter: &meter,
			Input:        make([]byte, 4096),
		}

		ip := NewInterpreter(prog, opts)
		defer ip.Finish()

		// Test Write64/Read64
		if addr+8 <= VaddrHeap+uint64(len(ip.heap)) {
			err := ip.Write64(addr, value)
			if err != nil {
				t.Errorf("Write64 failed: %v", err)
			}

			readVal, err := ip.Read64(addr)
			if err != nil {
				t.Errorf("Read64 failed: %v", err)
			}

			if readVal != value {
				t.Errorf("Read64/Write64 mismatch: wrote %#x, read %#x", value, readVal)
			}
		}

		// Test Write32/Read32
		val32 := uint32(value)
		if addr+4 <= VaddrHeap+uint64(len(ip.heap)) {
			err := ip.Write32(addr, val32)
			if err != nil {
				t.Errorf("Write32 failed: %v", err)
			}

			readVal, err := ip.Read32(addr)
			if err != nil {
				t.Errorf("Read32 failed: %v", err)
			}

			if readVal != val32 {
				t.Errorf("Read32/Write32 mismatch: wrote %#x, read %#x", val32, readVal)
			}
		}

		// Test Write16/Read16
		val16 := uint16(value)
		if addr+2 <= VaddrHeap+uint64(len(ip.heap)) {
			err := ip.Write16(addr, val16)
			if err != nil {
				t.Errorf("Write16 failed: %v", err)
			}

			readVal, err := ip.Read16(addr)
			if err != nil {
				t.Errorf("Read16 failed: %v", err)
			}

			if readVal != val16 {
				t.Errorf("Read16/Write16 mismatch: wrote %#x, read %#x", val16, readVal)
			}
		}

		// Test Write8/Read8
		val8 := uint8(value)
		if addr+1 <= VaddrHeap+uint64(len(ip.heap)) {
			err := ip.Write8(addr, val8)
			if err != nil {
				t.Errorf("Write8 failed: %v", err)
			}

			readVal, err := ip.Read8(addr)
			if err != nil {
				t.Errorf("Read8 failed: %v", err)
			}

			if readVal != val8 {
				t.Errorf("Read8/Write8 mismatch: wrote %#x, read %#x", val8, readVal)
			}
		}
	})
}

// FuzzStackPushPop tests stack frame management
func FuzzStackPushPop(f *testing.F) {
	// Seed with various return addresses and register values
	f.Add(int64(0), uint64(VaddrStack+StackFrameSize))
	f.Add(int64(100), uint64(VaddrStack+StackFrameSize*2))
	f.Add(int64(-1), uint64(VaddrStack))

	f.Fuzz(func(t *testing.T, retAddr int64, framePtr uint64) {
		stack := NewStack(sbpfver.SbpfVersion{})
		defer stack.Finish()

		// Initialize registers
		regs := make([]uint64, 11)
		for i := range regs {
			regs[i] = uint64(i * 0x1000)
		}
		regs[10] = framePtr // frame pointer

		// Save non-volatile regs for later comparison
		savedNV := [4]uint64{regs[6], regs[7], regs[8], regs[9]}

		// Test push
		ok := stack.Push(regs, retAddr)
		if !ok {
			t.Fatal("Push failed")
		}

		// Modify registers to verify restoration
		for i := range regs {
			regs[i] = 0xDEADBEEF
		}

		// Test pop
		restoredRet, ok := stack.Pop(regs)
		if !ok {
			t.Fatal("Pop failed")
		}

		// Verify return address
		if restoredRet != retAddr {
			t.Errorf("Return address mismatch: expected %d, got %d", retAddr, restoredRet)
		}

		// Verify non-volatile registers restored
		if regs[6] != savedNV[0] || regs[7] != savedNV[1] || regs[8] != savedNV[2] || regs[9] != savedNV[3] {
			t.Errorf("Non-volatile registers not restored correctly")
		}

		// Verify frame pointer restored
		if regs[10] != framePtr {
			t.Errorf("Frame pointer not restored: expected %#x, got %#x", framePtr, regs[10])
		}
	})
}

// FuzzStackOverflow tests stack depth limits
func FuzzStackOverflow(f *testing.F) {
	f.Add(uint8(StackDepth - 1))
	f.Add(uint8(StackDepth))
	f.Add(uint8(StackDepth + 1))
	f.Add(uint8(255))

	f.Fuzz(func(t *testing.T, pushCount uint8) {
		stack := NewStack(sbpfver.SbpfVersion{})
		defer stack.Finish()

		regs := make([]uint64, 11)
		successfulPushes := 0

		// Try to push pushCount frames
		for i := uint8(0); i < pushCount; i++ {
			ok := stack.Push(regs, int64(i))
			if ok {
				successfulPushes++
			} else {
				// Push should fail when stack is full
				if successfulPushes < StackDepth-1 {
					t.Errorf("Push failed prematurely at depth %d (expected max %d)", successfulPushes, StackDepth-1)
				}
				break
			}
		}

		// Verify we can't exceed StackDepth-1 (accounting for initial frame)
		if successfulPushes >= StackDepth {
			t.Errorf("Stack allowed %d pushes, exceeding limit of %d", successfulPushes, StackDepth-1)
		}

		// Pop all successfully pushed frames
		for i := 0; i < successfulPushes; i++ {
			_, ok := stack.Pop(regs)
			if !ok {
				t.Errorf("Pop failed after %d pops, expected to succeed for all %d pushed frames", i, successfulPushes)
			}
		}

		// Verify we can't pop the initial frame
		_, ok := stack.Pop(regs)
		if ok {
			t.Error("Pop succeeded when trying to pop initial frame, should have failed")
		}
	})
}

// FuzzStackFrameAccess tests GetFrame memory access
func FuzzStackFrameAccess(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(StackFrameSize))
	f.Add(uint32(StackFrameSize * 2))
	f.Add(uint32(math.MaxUint32))

	f.Fuzz(func(t *testing.T, addr uint32) {
		// Test static stack frames
		stackStatic := NewStack(sbpfver.SbpfVersion{})
		defer stackStatic.Finish()

		frame := stackStatic.GetFrame(addr)

		// For static frames, gaps (odd frame numbers) should return nil
		frameNum := addr / StackFrameSize
		if frameNum%2 == 1 && frame != nil {
			t.Error("GetFrame returned non-nil for gap frame in static mode")
		}

		// Test dynamic stack frames (SBPF V1+)
		ver := sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV1}
		stackDynamic := NewStack(ver)
		defer stackDynamic.Finish()

		frameDynamic := stackDynamic.GetFrame(addr)

		// Dynamic frames should not have gaps
		off := uint64(addr & math.MaxUint32)
		if off > StackMax {
			if frameDynamic != nil {
				t.Error("GetFrame returned non-nil for out-of-bounds address in dynamic mode")
			}
		}
	})
}

// FuzzExceptionWrapping tests Exception error handling
func FuzzExceptionWrapping(f *testing.F) {
	f.Add(int64(0), "division by zero")
	f.Add(int64(100), "out of bounds")
	f.Add(int64(-1), "invalid opcode")

	f.Fuzz(func(t *testing.T, pc int64, msg string) {
		detail := errors.New(msg)
		exc := &Exception{
			PC:     pc,
			Detail: detail,
		}

		// Test Error() formatting
		errStr := exc.Error()
		if errStr == "" {
			t.Error("Exception.Error() returned empty string")
		}

		// Test Unwrap()
		unwrapped := exc.Unwrap()
		if unwrapped != detail {
			t.Errorf("Exception.Unwrap() returned wrong error: expected %v, got %v", detail, unwrapped)
		}

		// Test errors.Is compatibility
		if !errors.Is(exc, detail) {
			t.Error("errors.Is failed for wrapped exception")
		}
	})
}

// FuzzExcBadAccessCreation tests bad access exception creation
func FuzzExcBadAccessCreation(f *testing.F) {
	f.Add(uint64(0), uint64(8), false, "unmapped")
	f.Add(VaddrHeap, uint64(1024*1024), true, "overflow")
	f.Add(VaddrProgram, uint64(1), true, "write to program")

	f.Fuzz(func(t *testing.T, addr uint64, size uint64, write bool, reason string) {
		exc := NewExcBadAccess(addr, size, write, reason)

		if exc.Addr != addr {
			t.Errorf("ExcBadAccess.Addr mismatch: expected %#x, got %#x", addr, exc.Addr)
		}
		if exc.Size != size {
			t.Errorf("ExcBadAccess.Size mismatch: expected %d, got %d", size, exc.Size)
		}
		if exc.Write != write {
			t.Errorf("ExcBadAccess.Write mismatch: expected %v, got %v", write, exc.Write)
		}
		if exc.Reason != reason {
			t.Errorf("ExcBadAccess.Reason mismatch: expected %s, got %s", reason, exc.Reason)
		}

		// Test Error() formatting
		errStr := exc.Error()
		if errStr == "" {
			t.Error("ExcBadAccess.Error() returned empty string")
		}
	})
}

// FuzzExcCallDestCreation tests call destination exception
func FuzzExcCallDestCreation(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(0xFFFFFFFF))
	f.Add(uint32(0x12345678))

	f.Fuzz(func(t *testing.T, imm uint32) {
		exc := ExcCallDest{Imm: imm}

		// Test Error() formatting
		errStr := exc.Error()
		if errStr == "" {
			t.Error("ExcCallDest.Error() returned empty string")
		}

		// Verify the imm value is preserved
		if exc.Imm != imm {
			t.Errorf("ExcCallDest.Imm mismatch: expected %#x, got %#x", imm, exc.Imm)
		}
	})
}

// FuzzComputeMeterIntegration tests compute meter integration in VM
func FuzzComputeMeterIntegration(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(100))
	f.Add(uint64(1000000))

	f.Fuzz(func(t *testing.T, initialCU uint64) {
		// Limit to reasonable values
		if initialCU > 10000000 {
			initialCU = initialCU % 10000000
		}

		prog := &Program{
			RO:          make([]byte, 4096),
			Text:        make([]Slot, 512),
			TextVA:      VaddrProgram,
			Entrypoint:  0,
			SbpfVersion: sbpfver.SbpfVersion{},
		}

		meter := cu.NewComputeMeter(initialCU)
		opts := &VMOpts{
			HeapMax:      32 * 1024,
			ComputeMeter: &meter,
			Input:        make([]byte, 4096),
		}

		ip := NewInterpreter(prog, opts)
		defer ip.Finish()

		// Verify compute meter is accessible
		if ip.ComputeMeter() != &meter {
			t.Error("ComputeMeter() returned wrong meter")
		}

		// Verify initial state
		if ip.PrevInstrMeter() != initialCU {
			t.Errorf("PrevInstrMeter mismatch: expected %d, got %d", initialCU, ip.PrevInstrMeter())
		}

		// Test SetPrevInstrMeter
		newVal := initialCU / 2
		ip.SetPrevInstrMeter(newVal)
		if ip.PrevInstrMeter() != newVal {
			t.Errorf("SetPrevInstrMeter failed: expected %d, got %d", newVal, ip.PrevInstrMeter())
		}
	})
}
