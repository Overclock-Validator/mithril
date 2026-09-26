package sbpf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

// These byte values alias arithmetic instructions outside v2. Exercise all
// relocated memory instructions through Run, not just the cold handler.
func TestInterpreterV2MemoryOpcodes(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		loads := map[int]uint8{1: OpLd1BReg, 2: OpLd2BReg, 4: OpLd4BReg, 8: OpLd8BReg}
		immediates := map[int]uint8{1: OpSt1BImm, 2: OpSt2BImm, 4: OpSt4BImm, 8: OpSt8BImm}
		registers := map[int]uint8{1: OpSt1BReg, 2: OpSt2BReg, 4: OpSt4BReg, 8: OpSt8BReg}
		for _, kind := range []string{"load", "store_imm", "store_reg"} {
			for _, region := range []string{"heap", "stack", "input", "rodata", "unmapped"} {
				t.Run(fmt.Sprintf("%s/%d/%s", kind, width, region), func(t *testing.T) {
					const value = uint64(0xfedcba9876543210)
					immediate := uint32(0xf123abcd)
					var addr uint64
					switch region {
					case "heap":
						addr = VaddrHeap + 9
					case "stack":
						addr = VaddrStack + 9
					case "input":
						addr = VaddrInput + 9
					case "rodata":
						addr = VaddrProgram + 9
					case "unmapped":
						addr = 0x600000009
					}
					// Negative offsets and unaligned addresses must behave identically to
					// the original interpreter, including the post-instruction exception PC.
					text := diffLoadImm64(5, addr+3, sbpfver.SbpfVersionV2)
					text = append(text, diffLoadImm64(6, value, sbpfver.SbpfVersionV2)...)
					var op Slot
					switch kind {
					case "load":
						op = slot(loads[width], 0, 5, -3, 0)
					case "store_imm":
						op = slot(immediates[width], 5, 0, -3, immediate)
					case "store_reg":
						op = slot(registers[width], 5, 6, -3, 0)
					}
					text = append(text, op, slot(OpExit, 0, 0, 0, 0))
					p := mkProgram(text, sbpfver.SbpfVersionV2)
					p.RO = bytes.Repeat([]byte{0xa5}, 32)
					require.NoError(t, p.Verify())
					cm := cu.NewComputeMeter(100)
					ip := NewInterpreter(p, &VMOpts{HeapMax: 32, Input: bytes.Repeat([]byte{0xa5}, 32), ComputeMeter: &cm, Syscalls: noSyscalls})
					defer ip.Finish()
					var memory []byte
					switch region {
					case "heap":
						memory = ip.heap
					case "stack":
						memory = ip.stack.mem
					case "input":
						memory = ip.input
					case "rodata":
						memory = p.RO
					}
					if memory != nil {
						// Use Write for writable VM storage so pooled-memory tracking is kept.
						if region != "rodata" {
							require.NoError(t, ip.Write(addr-9, bytes.Repeat([]byte{0xa5}, 32)))
						}
					}
					before := append([]byte(nil), memory...)
					ret, used, err := ip.Run()
					if region == "unmapped" || (region == "rodata" && kind != "load") {
						require.Error(t, err)
						var exc *Exception
						require.ErrorAs(t, err, &exc)
						require.Equal(t, int64(5), exc.PC)
						var access ExcBadAccess
						require.ErrorAs(t, err, &access)
						require.Equal(t, addr, access.Addr)
						require.Equal(t, uint64(width), access.Size)
						require.Equal(t, kind != "load", access.Write)
						require.Equal(t, uint64(95), cm.Remaining())
						require.Equal(t, before, memory)
						return
					}
					require.NoError(t, err)
					require.Equal(t, uint64(6), used)
					require.Equal(t, uint64(94), cm.Remaining())
					var encoded [8]byte
					if kind == "load" {
						copy(encoded[:], before[9:9+width])
						require.Equal(t, binary.LittleEndian.Uint64(encoded[:]), ret)
					} else {
						v := value
						if kind == "store_imm" {
							signed := int32(immediate)
							v = uint64(int64(signed))
						}
						binary.LittleEndian.PutUint64(encoded[:], v)
						copy(before[9:9+width], encoded[:width])
					}
					require.Equal(t, before, memory)
				})
			}
		}
	}
}
