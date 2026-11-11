//go:build ignore

// gen_interp_amd64 writes the amd64 assembly interpreter loop and the handler
// ID constants that index its handler address table:
//
//	go run gen_interp_amd64.go
//
// Every handler mirrors one case of runFast (interpreter_fast.go) and keeps
// runFast's contract: an instruction is either completed exactly (effect, then
// pc++ and one unit of budget) or left untouched, in which case the loop
// returns with pc and the remaining budget unchanged and Go executes it.
//
// Fixed registers while the loop runs:
//
//	SI  text base (*Slot)        R8   pc
//	R9  len(text)                R10  instructions left in this call
//	DI  register file (*[16]uint64)
//	R11 opcode handler table (*[256]uintptr)
//	AX  current instruction      BX CX DX R12 R13 R15  scratch
//
// SP, BP and R14 are never written. No handler touches the native stack, calls
// anything, or can fault: text is read only after an unsigned pc bound check,
// register indexes are 4-bit fields into a 16-entry array, guest memory goes
// through the same window checks as fastRead/fastWrite, the shadow stack is
// written only below its capacity, divisors are tested for zero, and shift
// counts Go would not mask are sent back to Go.
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
)

type handler struct {
	name string
	body []string
}

var handlers []handler

func add(name string, body ...string) {
	var lines []string
	for _, b := range body {
		for _, l := range strings.Split(b, "\n") {
			if l = strings.TrimSpace(l); l != "" {
				lines = append(lines, l)
			}
		}
	}
	handlers = append(handlers, handler{name, lines})
}

const (
	dst    = "MOVBLZX AH, BX\nANDL $15, BX"                           // BX = dst index
	dstSrc = "MOVBLZX AH, BX\nMOVL BX, DX\nSHRL $4, DX\nANDL $15, BX" // BX = dst, DX = src index
	rDst   = "(DI)(BX*8)"
	rSrc   = "(DI)(DX*8)"
	uimm   = "SHRQ $32, AX" // AX = imm, zero-extended (EAX = imm)
	simm   = "SARQ $32, AX" // AX = imm, sign-extended to 64 bits
)

// off leaves the sign-extended 16-bit offset field in reg.
func off(reg string) string {
	return fmt.Sprintf("MOVQ AX, %s\nSHLQ $32, %s\nSARQ $48, %s", reg, reg, reg)
}

// translate turns the guest address in R13 into a host address in R13, or
// jumps to out. It mirrors fastRead (lenField rlen) and fastWrite (lenField
// wlen, plus the dirty-page bits). Clobbers CX, R12, R15 only.
//
// The window index is compared, not clamped: the last table entry is
// permanently empty, so an address at or beyond it can only be declined.
// With gaps false the stack-gap arithmetic is left out. That is exact only
// when no window has gap fields set, which the table builder guarantees.
func translate(size int, write, gaps bool) string {
	lenField := "memRegion_rlen"
	if write {
		lenField = "memRegion_wlen"
	}
	s := `
		MOVQ R13, R15
		SHRQ $32, R15
		CMPQ R15, $(const_numFastRegions-1)
		JCC out
		SHLQ $6, R15
		ADDQ regions+24(FP), R15
	`
	if gaps {
		// off = uint32(lo - ((lo & gapMask) >> 1) - start), declined when
		// lo & gapBit != 0. The 32-bit subtractions read only the low half
		// of R13 and clear its upper half.
		s += `
		MOVL R13, CX
		TESTQ CX, memRegion_gapBit(R15)
		JNE out
		ANDQ memRegion_gapMask(R15), CX
		SHRQ $1, CX
		SUBL CX, R13
		`
	}
	s += fmt.Sprintf(`
		SUBL memRegion_start(R15), R13
		LEAQ %d(R13), CX
		CMPQ CX, %s(R15)
		JHI out
	`, size, lenField)
	if write {
		// reg.dirty |= 3 << (off >> 12). Go yields 0 for a count of 64 or
		// more; the hardware would mask the count, so skip the update then.
		s += `
		MOVQ R13, CX
		SHRQ $12, CX
		CMPQ CX, $64
		JCC nodirty
		MOVL $3, R12
		SHLQ CX, R12
		ORQ R12, memRegion_dirty(R15)
		nodirty:
		`
	}
	return s + "ADDQ memRegion_base(R15), R13"
}

func main() {
	add("Bail")

	// Memory handlers come in two variants: the plain name handles stack
	// frame gaps, the NoGap one is for interpreters whose windows have none.
	for _, v := range []struct {
		suffix string
		gaps   bool
	}{{"", true}, {"NoGap", false}} {
		// Loads: r[dst] = zero-extended *(r[src] + off).
		for _, m := range []struct {
			name, load string
			size       int
		}{
			{"Ldxb", "MOVBLZX (R13), CX", 1}, {"Ldxh", "MOVWLZX (R13), CX", 2},
			{"Ldxw", "MOVL (R13), CX", 4}, {"Ldxdw", "MOVQ (R13), CX", 8},
		} {
			add(m.name+v.suffix, dstSrc, off("R13"), "ADDQ "+rSrc+", R13", translate(m.size, false, v.gaps), m.load, "MOVQ CX, "+rDst)
		}
		// Stores of an immediate: *(r[dst] + off) = imm (sign-extended for 8 bytes).
		for _, m := range []struct {
			name, store string
			size        int
		}{
			{"Stb", "MOVB AL, (R13)", 1}, {"Sth", "MOVW AX, (R13)", 2},
			{"Stw", "MOVL AX, (R13)", 4}, {"Stdw", "MOVQ AX, (R13)", 8},
		} {
			add(m.name+v.suffix, dst, off("R13"), "ADDQ "+rDst+", R13", simm, translate(m.size, true, v.gaps), m.store)
		}
		// Stores of a register: *(r[dst] + off) = r[src].
		for _, m := range []struct {
			name, store string
			size        int
		}{
			{"Stxb", "MOVB DL, (R13)", 1}, {"Stxh", "MOVW DX, (R13)", 2},
			{"Stxw", "MOVL DX, (R13)", 4}, {"Stxdw", "MOVQ DX, (R13)", 8},
		} {
			add(m.name+v.suffix, dstSrc, off("R13"), "ADDQ "+rDst+", R13", translate(m.size, true, v.gaps), "MOVQ "+rSrc+", DX", m.store)
		}
	}

	// 32-bit add/sub. SX sign-extends the 32-bit result, ZX zero-extends it
	// (a 32-bit operation already clears the upper half of its register).
	for _, ext := range []struct{ suffix, op string }{{"SX", "MOVLQSX"}, {"ZX", ""}} {
		fix := func(reg string) string {
			if ext.op == "" {
				return ""
			}
			return ext.op + " " + reg + ", " + reg
		}
		add("Add32Imm"+ext.suffix, dst, uimm, "ADDL "+rDst+", AX", fix("AX"), "MOVQ AX, "+rDst)
		add("Add32Reg"+ext.suffix, dstSrc, "MOVL "+rSrc+", DX", "ADDL "+rDst+", DX", fix("DX"), "MOVQ DX, "+rDst)
		add("Sub32Imm"+ext.suffix, dst, uimm, "MOVL "+rDst+", DX", "SUBL AX, DX", fix("DX"), "MOVQ DX, "+rDst)
		add("Sub32ImmSwap"+ext.suffix, dst, uimm, "SUBL "+rDst+", AX", fix("AX"), "MOVQ AX, "+rDst)
		add("Sub32Reg"+ext.suffix, dstSrc, "MOVL "+rSrc+", DX", "MOVL "+rDst+", CX", "SUBL DX, CX", fix("CX"), "MOVQ CX, "+rDst)
	}
	// 32-bit multiply always sign-extends its result.
	add("Mul32Imm", dst, uimm, "IMULL "+rDst+", AX", "MOVLQSX AX, AX", "MOVQ AX, "+rDst)
	add("Mul32Reg", dstSrc, "MOVL "+rSrc+", DX", "IMULL "+rDst+", DX", "MOVLQSX DX, DX", "MOVQ DX, "+rDst)
	for _, o := range []struct{ name, op string }{{"Or", "ORL"}, {"And", "ANDL"}, {"Xor", "XORL"}} {
		add(o.name+"32Imm", dst, uimm, o.op+" "+rDst+", AX", "MOVQ AX, "+rDst)
		add(o.name+"32Reg", dstSrc, "MOVL "+rSrc+", DX", o.op+" "+rDst+", DX", "MOVQ DX, "+rDst)
	}
	// 32-bit shifts by CL use the low five bits of the count, as Go's & 0x1f.
	for _, o := range []struct{ name, op string }{{"Lsh", "SHLL"}, {"Rsh", "SHRL"}} {
		add(o.name+"32Imm", dst, uimm, "MOVL AX, CX", "MOVL "+rDst+", DX", o.op+" CX, DX", "MOVQ DX, "+rDst)
		add(o.name+"32Reg", dstSrc, "MOVQ "+rSrc+", CX", "MOVL "+rDst+", DX", o.op+" CX, DX", "MOVQ DX, "+rDst)
	}
	// Go does not mask arithmetic shift counts. Counts past the width (and a
	// negative immediate) go back to Go rather than being emulated here.
	add("Arsh32Imm", dst, uimm, "CMPL AX, $31", "JHI out", "MOVL AX, CX", "MOVL "+rDst+", DX", "SARL CX, DX", "MOVQ DX, "+rDst)
	add("Arsh32Reg", dstSrc, "MOVL "+rSrc+", CX", "CMPL CX, $31", "JHI out", "MOVL "+rDst+", DX", "SARL CX, DX", "MOVQ DX, "+rDst)
	add("Mov32Imm", dst, uimm, "MOVQ AX, "+rDst)
	add("Mov32RegSX", dstSrc, "MOVLQSX "+rSrc+", DX", "MOVQ DX, "+rDst)
	add("Mov32RegZX", dstSrc, "MOVL "+rSrc+", DX", "MOVQ DX, "+rDst)
	// Unsigned 32-bit divide: EDX:EAX / ECX with EDX = 0 cannot overflow.
	add("Div32Reg", dstSrc, "MOVL "+rSrc+", CX", "TESTL CX, CX", "JEQ out", "MOVL "+rDst+", AX", "XORL DX, DX", "DIVL CX", "MOVQ AX, "+rDst)
	add("Mod32Reg", dstSrc, "MOVL "+rSrc+", CX", "TESTL CX, CX", "JEQ out", "MOVL "+rDst+", AX", "XORL DX, DX", "DIVL CX", "MOVQ DX, "+rDst)

	// 64-bit arithmetic. Immediates are sign-extended.
	for _, o := range []struct{ name, op string }{{"Add", "ADDQ"}, {"Or", "ORQ"}, {"And", "ANDQ"}, {"Xor", "XORQ"}, {"Sub", "SUBQ"}} {
		add(o.name+"64Imm", dst, simm, o.op+" AX, "+rDst)
		add(o.name+"64Reg", dstSrc, "MOVQ "+rSrc+", DX", o.op+" DX, "+rDst)
	}
	add("Sub64ImmSwap", dst, simm, "SUBQ "+rDst+", AX", "MOVQ AX, "+rDst)
	add("Mul64Imm", dst, simm, "IMULQ "+rDst+", AX", "MOVQ AX, "+rDst)
	add("Mul64Reg", dstSrc, "MOVQ "+rSrc+", DX", "IMULQ "+rDst+", DX", "MOVQ DX, "+rDst)
	// 64-bit shifts by CL use the low six bits of the count, as Go's & 0x3f.
	for _, o := range []struct{ name, op string }{{"Lsh", "SHLQ"}, {"Rsh", "SHRQ"}} {
		add(o.name+"64Imm", dst, uimm, "MOVL AX, CX", o.op+" CX, "+rDst)
		add(o.name+"64Reg", dstSrc, "MOVQ "+rSrc+", CX", o.op+" CX, "+rDst)
	}
	add("Arsh64Imm", dst, uimm, "CMPL AX, $63", "JHI out", "MOVL AX, CX", "SARQ CX, "+rDst)
	add("Arsh64Reg", dstSrc, "MOVQ "+rSrc+", CX", "CMPQ CX, $63", "JHI out", "SARQ CX, "+rDst)
	add("Mov64Imm", dst, simm, "MOVQ AX, "+rDst)
	add("Mov64Reg", dstSrc, "MOVQ "+rSrc+", DX", "MOVQ DX, "+rDst)
	// Unsigned 64-bit divide: RDX:RAX / RCX with RDX = 0 cannot overflow.
	add("Div64Imm", dst, simm, "TESTQ AX, AX", "JEQ out", "MOVQ AX, CX", "MOVQ "+rDst+", AX", "XORQ DX, DX", "DIVQ CX", "MOVQ AX, "+rDst)
	add("Mod64Imm", dst, simm, "TESTQ AX, AX", "JEQ out", "MOVQ AX, CX", "MOVQ "+rDst+", AX", "XORQ DX, DX", "DIVQ CX", "MOVQ DX, "+rDst)
	add("Div64Reg", dstSrc, "MOVQ "+rSrc+", CX", "TESTQ CX, CX", "JEQ out", "MOVQ "+rDst+", AX", "XORQ DX, DX", "DIVQ CX", "MOVQ AX, "+rDst)
	add("Mod64Reg", dstSrc, "MOVQ "+rSrc+", CX", "TESTQ CX, CX", "JEQ out", "MOVQ "+rDst+", AX", "XORQ DX, DX", "DIVQ CX", "MOVQ DX, "+rDst)

	// lddw: two slots, one unit of budget. The second slot must exist.
	add("Lddw", dst,
		"LEAQ 1(R8), CX", "CMPQ CX, R9", "JCC out",
		uimm, "MOVQ 8(SI)(R8*8), DX", "SHRQ $32, DX", "SHLQ $32, DX", "ORQ DX, AX",
		"MOVQ AX, "+rDst, "INCQ R8")

	// Jumps add the offset without validating the target; the pc bound check
	// before the next fetch rejects a target outside the text.
	add("Ja", off("DX"), "ADDQ DX, R8")
	for _, j := range []struct{ name, cmov string }{
		{"Jeq", "CMOVQEQ"}, {"Jgt", "CMOVQHI"}, {"Jge", "CMOVQCC"}, {"Jlt", "CMOVQCS"}, {"Jle", "CMOVQLS"},
		{"Jset", "CMOVQNE"}, {"Jne", "CMOVQNE"},
		{"Jsgt", "CMOVQGT"}, {"Jsge", "CMOVQGE"}, {"Jslt", "CMOVQLT"}, {"Jsle", "CMOVQLE"},
	} {
		cmp := "CMPQ " + rDst + ", %s" // flags of r[dst] - operand
		if j.name == "Jset" {
			cmp = "TESTQ %s, " + rDst // flags of r[dst] & operand
		}
		take := "XORL CX, CX\n" + cmp + "\n" + j.cmov + " R13, CX\nADDQ CX, R8"
		add(j.name+"Imm", dst, off("R13"), simm, fmt.Sprintf(take, "AX"))
		add(j.name+"Reg", dstSrc, off("R13"), "MOVQ "+rSrc+", DX", fmt.Sprintf(take, "DX"))
	}

	// Internal call of the static-syscall versions (src field 1): push a
	// shadow frame exactly as runFast does, then continue at pc + imm + 1.
	// The target and the shadow stack's capacity are checked before any write.
	add("CallStatic",
		"MOVBLZX AH, DX", "SHRL $4, DX", "CMPL DX, $1", "JNE out",
		simm, "LEAQ 1(R8)(AX*1), R13", "CMPQ R13, R9", "JCC out",
		"MOVQ stack+40(FP), R15",
		"MOVQ (Stack_shadow+8)(R15), CX", "CMPQ CX, (Stack_shadow+16)(R15)", "JGE out",
		"IMUL3Q $Frame__size, CX, R12", "ADDQ Stack_shadow(R15), R12",
		"INCQ CX", "MOVQ CX, (Stack_shadow+8)(R15)",
		"LEAQ 1(R8), DX", "MOVQ DX, Frame_RetAddr(R12)",
		"MOVQ 48(DI), DX", "MOVQ DX, (Frame_NVRegs+0)(R12)",
		"MOVQ 56(DI), DX", "MOVQ DX, (Frame_NVRegs+8)(R12)",
		"MOVQ 64(DI), DX", "MOVQ DX, (Frame_NVRegs+16)(R12)",
		"MOVQ 72(DI), DX", "MOVQ DX, (Frame_NVRegs+24)(R12)",
		"MOVQ 80(DI), DX", "MOVQ DX, Frame_FramePtr(R12)",
		"CMPQ CX, Stack_maxDepth(R15)", "JLE nomax", "MOVQ CX, Stack_maxDepth(R15)", "nomax:",
		"CMPB Stack_dynamicStackFrames(R15), $0", "JNE framed",
		"MOVQ $const_StackFrameSize, DX",
		"CMPB Stack_stackFrameGaps(R15), $0", "JEQ nogap", "ADDQ DX, DX", "nogap:",
		"ADDQ DX, 80(DI)", "framed:",
		"LEAQ -1(R13), R8")
	// Return from an inner frame: pop the shadow frame. Returning from the
	// entry frame (depth 1) ends the program and stays in Go.
	add("Exit",
		"MOVQ stack+40(FP), R15",
		"MOVQ (Stack_shadow+8)(R15), CX", "CMPQ CX, $1", "JLE out",
		"DECQ CX", "IMUL3Q $Frame__size, CX, R13", "ADDQ Stack_shadow(R15), R13",
		"MOVQ (Frame_NVRegs+0)(R13), DX", "MOVQ DX, 48(DI)",
		"MOVQ (Frame_NVRegs+8)(R13), DX", "MOVQ DX, 56(DI)",
		"MOVQ (Frame_NVRegs+16)(R13), DX", "MOVQ DX, 64(DI)",
		"MOVQ (Frame_NVRegs+24)(R13), DX", "MOVQ DX, 72(DI)",
		"MOVQ Frame_FramePtr(R13), DX", "MOVQ DX, 80(DI)",
		"MOVQ CX, (Stack_shadow+8)(R15)",
		"MOVQ Frame_RetAddr(R13), R8", "DECQ R8")

	var s bytes.Buffer
	s.WriteString(`// Code generated by gen_interp_amd64.go. DO NOT EDIT.

//go:build amd64 && !purego

#include "textflag.h"
#include "go_asm.h"
#include "funcdata.h"

// NEXT finishes an instruction: pc++, one unit of budget, then fetch and
// dispatch the next one. It leaves through out when the budget for this call
// is spent or pc is outside the text; pc and the budget are then exact.
#define NEXT \
	INCQ R8; \
	DECQ R10; \
	JEQ  out; \
	CMPQ R8, R9; \
	JCC  out; \
	MOVQ (SI)(R8*8), AX; \
	MOVBLZX AL, BX; \
	JMP  (R11)(BX*8)

// LEAVE returns pc and the unused budget to Go. Every handler shares
// runAsmLoop's argument frame: none of them moves SP, so the offsets agree.
#define LEAVE \
	MOVQ R8, newPC+64(FP); \
	MOVQ R10, newRem+72(FP); \
	RET

// func runAsmLoop(text *Slot, textLen int64, r *[16]uint64, regions *[numFastRegions]memRegion, table *[256]uintptr, stack *Stack, pc int64, rem uint64) (newPC int64, newRem uint64)
TEXT ·runAsmLoop(SB), NOSPLIT|NOFRAME, $0-80
	MOVQ text+0(FP), SI
	MOVQ textLen+8(FP), R9
	MOVQ r+16(FP), DI
	MOVQ table+32(FP), R11
	MOVQ pc+48(FP), R8
	MOVQ rem+56(FP), R10
	TESTQ R10, R10
	JEQ  out
	CMPQ R8, R9
	JCC  out
	MOVQ (SI)(R8*8), AX
	MOVBLZX AL, BX
	JMP  (R11)(BX*8)
out:
	LEAVE
`)
	for _, h := range handlers {
		fmt.Fprintf(&s, "\nTEXT asmOp%s<>(SB), NOSPLIT|NOFRAME, $0-80\n", h.name)
		// The handlers run in runAsmLoop's frame, so they describe its
		// arguments the same way.
		s.WriteString("\tFUNCDATA $FUNCDATA_ArgsPointerMaps, ·runAsmLoop·args_stackmap(SB)\n")
		for _, l := range h.body {
			if strings.HasSuffix(l, ":") {
				fmt.Fprintf(&s, "%s\n", l)
			} else {
				fmt.Fprintf(&s, "\t%s\n", l)
			}
		}
		// Bail is the table entry for every opcode the loop does not handle:
		// it leaves at once, with pc and the budget untouched.
		if h.name == "Bail" {
			s.WriteString("\tLEAVE\n")
			continue
		}
		s.WriteString("\tNEXT\nout:\n\tLEAVE\n")
	}
	s.WriteString("\n// asmHandlers holds each handler's address, indexed by its ID.\n")
	for i, h := range handlers {
		fmt.Fprintf(&s, "DATA asmHandlers<>+%d(SB)/8, $asmOp%s<>(SB)\n", i*8, h.name)
	}
	fmt.Fprintf(&s, "GLOBL asmHandlers<>(SB), RODATA|NOPTR, $%d\n", len(handlers)*8)
	s.WriteString(`
// func asmHandlerTable() *[numAsmHandlers]uintptr
TEXT ·asmHandlerTable(SB), NOSPLIT, $0-8
	LEAQ asmHandlers<>(SB), AX
	MOVQ AX, ret+0(FP)
	RET
`)

	var g bytes.Buffer
	g.WriteString("// Code generated by gen_interp_amd64.go. DO NOT EDIT.\n\n//go:build amd64 && !purego\n\npackage sbpf\n\n// Handler IDs index the table returned by asmHandlerTable.\nconst (\n")
	for i, h := range handlers {
		if i == 0 {
			fmt.Fprintf(&g, "\tasm%s = iota\n", h.name)
		} else {
			fmt.Fprintf(&g, "\tasm%s\n", h.name)
		}
	}
	g.WriteString("\tnumAsmHandlers\n)\n")

	for name, data := range map[string][]byte{
		"interpreter_asm_amd64.s":      s.Bytes(),
		"interpreter_asm_ids_amd64.go": g.Bytes(),
	} {
		if err := os.WriteFile(name, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
