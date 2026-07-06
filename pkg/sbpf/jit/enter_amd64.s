#include "textflag.h"

// func enter(ctx *ExecContext)
//
// Switches to the context's native stack, loads the SBF registers,
// CALLs the compiled code at ctx.Resume, and unwinds when it RETs.
// Compiled code clobbers everything including g (R14); all host state
// lives in the context while native code runs.
TEXT ·enter(SB), NOSPLIT|NOFRAME, $0-8
	MOVQ ctx+0(FP), AX

	// Save host registers.
	MOVQ SP,  0x58(AX)
	MOVQ BP,  0x60(AX)
	MOVQ BX,  0x68(AX)
	MOVQ R12, 0x70(AX)
	MOVQ R13, 0x78(AX)
	MOVQ R14, 0x80(AX)
	MOVQ R15, 0x88(AX)

	// Switch stacks and make BP the context pointer. EnterSP is the
	// native stack top on a fresh entry and the yield-time RSP on a
	// syscall resume.
	MOVQ 0x118(AX), SP
	MOVQ AX, BP

	// Load SBF r0..r10.
	MOVQ 0x00(BP), BX
	MOVQ 0x08(BP), CX
	MOVQ 0x10(BP), SI
	MOVQ 0x18(BP), DI
	MOVQ 0x20(BP), R8
	MOVQ 0x28(BP), R9
	MOVQ 0x30(BP), R10
	MOVQ 0x38(BP), R11
	MOVQ 0x40(BP), R12
	MOVQ 0x48(BP), R13
	MOVQ 0x50(BP), R14

	MOVQ 0x98(BP), AX
	CALL AX

	// Store SBF r0..r10 back.
	MOVQ BX,  0x00(BP)
	MOVQ CX,  0x08(BP)
	MOVQ SI,  0x10(BP)
	MOVQ DI,  0x18(BP)
	MOVQ R8,  0x20(BP)
	MOVQ R9,  0x28(BP)
	MOVQ R10, 0x30(BP)
	MOVQ R11, 0x38(BP)
	MOVQ R12, 0x40(BP)
	MOVQ R13, 0x48(BP)
	MOVQ R14, 0x50(BP)

	// Restore host registers.
	MOVQ BP, AX
	MOVQ 0x58(AX), SP
	MOVQ 0x60(AX), BP
	MOVQ 0x68(AX), BX
	MOVQ 0x70(AX), R12
	MOVQ 0x78(AX), R13
	MOVQ 0x80(AX), R14
	MOVQ 0x88(AX), R15
	RET
