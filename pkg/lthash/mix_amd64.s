//go:build amd64 && !purego

#include "textflag.h"

// The LtHash value is 1024 uint16 lanes = 2048 bytes = 16 iterations of
// four 32-byte YMM vectors. VPADDW/VPSUBW operate on 16-bit lanes modulo
// 2^16, exactly like the generic Go loop. Loads and stores are unaligned
// (VMOVDQU): LtHash values live inside Go structs with 2-byte alignment.

// func mixInAVX2(dst, src *[1024]uint16)
TEXT ·mixInAVX2(SB), NOSPLIT, $0-16
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	XORQ AX, AX
mixin_loop:
	VMOVDQU (DI)(AX*1), Y0
	VMOVDQU 32(DI)(AX*1), Y1
	VMOVDQU 64(DI)(AX*1), Y2
	VMOVDQU 96(DI)(AX*1), Y3
	VPADDW  (SI)(AX*1), Y0, Y0
	VPADDW  32(SI)(AX*1), Y1, Y1
	VPADDW  64(SI)(AX*1), Y2, Y2
	VPADDW  96(SI)(AX*1), Y3, Y3
	VMOVDQU Y0, (DI)(AX*1)
	VMOVDQU Y1, 32(DI)(AX*1)
	VMOVDQU Y2, 64(DI)(AX*1)
	VMOVDQU Y3, 96(DI)(AX*1)
	ADDQ    $128, AX
	CMPQ    AX, $2048
	JB      mixin_loop
	VZEROUPPER
	RET

// func mixOutAVX2(dst, src *[1024]uint16)
TEXT ·mixOutAVX2(SB), NOSPLIT, $0-16
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	XORQ AX, AX
mixout_loop:
	VMOVDQU (DI)(AX*1), Y0
	VMOVDQU 32(DI)(AX*1), Y1
	VMOVDQU 64(DI)(AX*1), Y2
	VMOVDQU 96(DI)(AX*1), Y3
	VPSUBW  (SI)(AX*1), Y0, Y0
	VPSUBW  32(SI)(AX*1), Y1, Y1
	VPSUBW  64(SI)(AX*1), Y2, Y2
	VPSUBW  96(SI)(AX*1), Y3, Y3
	VMOVDQU Y0, (DI)(AX*1)
	VMOVDQU Y1, 32(DI)(AX*1)
	VMOVDQU Y2, 64(DI)(AX*1)
	VMOVDQU Y3, 96(DI)(AX*1)
	ADDQ    $128, AX
	CMPQ    AX, $2048
	JB      mixout_loop
	VZEROUPPER
	RET
