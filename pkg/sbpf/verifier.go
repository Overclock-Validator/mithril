package sbpf

import "fmt"

type Verifier struct {
	Program       *Program
	validationMap [256]int
}

func NewVerifier(p *Program) *Verifier {
	return &Verifier{Program: p}
}

const (
	verifyValid = iota
	verifyCheckJmpV3
	verifyCheckEnd
	verifyCheckSt
	verifyCheckLDQ
	verifyCheckDiv
	verifyCheckSh32
	verifyCheckSh64
	verifyInvalid
	verifyCheckCallReg
	verifyCheckCallRegDepr
	verifyCheckCallImm
	verifyCheckSyscall
	verifyCheckJmpV0
)

func (v *Verifier) VerifyProgram() error {
	v.buildValidationMap()

	// TODO: static syscalls logic
	functionStart := uint64(0)
	functionNext := uint64(0)

	text := v.Program.Text
	if len(text) == 0 {
		return fmt.Errorf("empty text")
	}

	for pc := 0; pc < len(text); pc++ {
		ins := text[pc]

		if ins.Src() > 10 {
			return fmt.Errorf("invalid src register")
		}

		validationCode := v.validationMap[ins.Op()]
		switch validationCode {
		case verifyValid:
			// nothing to do

		case verifyCheckSt:
			// nothing to do

		case verifyCheckJmpV0:
			{
				dst := int64(pc) + int64(ins.Off()) + 1
				if (dst < 0) || (int(dst) >= len(text)) {
					return fmt.Errorf("jump out of code")
				}
				dstIns := text[dst]
				if dstIns.Op() == 0 {
					return fmt.Errorf("jump into middle of instruction")
				}
			}

		case verifyCheckJmpV3:
			{
				dst := int64(pc) + int64(ins.Off()) + 1
				if dst < int64(functionStart) || dst >= int64(functionNext) {
					return fmt.Errorf("jump out of code")
				}
			}

		case verifyCheckEnd:
			{
				if ins.Imm() != 16 && ins.Imm() != 32 && ins.Imm() != 64 {
					return fmt.Errorf("invalid end immediate")
				}
			}

		case verifyCheckLDQ:
			{
				if (pc + 1) >= len(text) {
					return fmt.Errorf("incomplete LDQ")
				}
				addlImm := text[pc+1]
				if addlImm.Op() != 0 {
					return fmt.Errorf("LDQ no addl imm")
				}
				pc++
			}

		case verifyCheckDiv:
			{
				if ins.Imm() == 0 {
					return fmt.Errorf("div by zero")
				}
			}

		case verifyCheckSh32:
			{
				if ins.Imm() >= 32 {
					return fmt.Errorf("sh overflow")
				}
			}

		case verifyCheckSh64:
			{
				if ins.Imm() >= 64 {
					return fmt.Errorf("sh overflow")
				}
			}

		case verifyCheckCallReg:
			{
				if ins.Src() > 9 {
					return fmt.Errorf("invalid register")
				}
			}

		case verifyCheckCallRegDepr:
			{
				if ins.Imm() > 9 {
					return fmt.Errorf("invalid register")
				}
			}

		case verifyCheckCallImm:
			{
				if _, ok := v.Program.Funcs[ins.Uimm()]; !ok {
					return fmt.Errorf("invalid function")
				}
			}

		case verifyCheckSyscall:
			// nothing to do - already verified by the loader

		case verifyInvalid:
			fallthrough
		default:
			{
				return fmt.Errorf("invalid opcode 0x%x", ins.Op())
			}
		}

		if ins.Src() > 10 {
			return fmt.Errorf("invalid src register")
		}

		if ins.Dst() == 10 && v.Program.SbpfVersion.DynamicStackFrames() &&
			ins.Op() == 0x07 && (ins.Imm()%DynamicStackFramesAlign) == 0 {
			continue
		}

		if ins.Dst() == 10 && validationCode != verifyCheckSt {
			return fmt.Errorf("invalid dst register")
		}

		if ins.Dst() > 10 {
			return fmt.Errorf("invalid dst register")
		}
	}

	return nil
}

func (v *Verifier) buildValidationMap() {
	checkJmp := verifyCheckJmpV0
	if v.Program.SbpfVersion.EnableStaticSyscalls() {
		checkJmp = verifyCheckJmpV3
	}

	v.validationMap = [256]int{
		/* 0x00 */ verifyInvalid /* 0x01 */, verifyInvalid /* 0x02 */, verifyInvalid /* 0x03 */, verifyInvalid,
		/* 0x04 */ verifyValid /* 0x05 */, checkJmp /* 0x06 */, verifyInvalid /* 0x07 */, verifyValid,
		/* 0x08 */ verifyInvalid /* 0x09 */, verifyInvalid /* 0x0a */, verifyInvalid /* 0x0b */, verifyInvalid,
		/* 0x0c */ verifyValid /* 0x0d */, verifyInvalid /* 0x0e */, verifyInvalid /* 0x0f */, verifyValid,
		/* 0x10 */ verifyInvalid /* 0x11 */, verifyInvalid /* 0x12 */, verifyInvalid /* 0x13 */, verifyInvalid,
		/* 0x14 */ verifyValid /* 0x15 */, checkJmp /* 0x16 */, verifyInvalid /* 0x17 */, verifyValid,
		/* 0x18 */ verifyInvalid /* 0x19 */, verifyInvalid /* 0x1a */, verifyInvalid /* 0x1b */, verifyInvalid,
		/* 0x1c */ verifyValid /* 0x1d */, checkJmp /* 0x1e */, verifyInvalid /* 0x1f */, verifyValid,
		/* 0x20 */ verifyInvalid /* 0x21 */, verifyInvalid /* 0x22 */, verifyInvalid /* 0x23 */, verifyInvalid,
		/* 0x24 */ verifyInvalid /* 0x25 */, checkJmp /* 0x26 */, verifyInvalid /* 0x27 */, verifyCheckSt,
		/* 0x28 */ verifyInvalid /* 0x29 */, verifyInvalid /* 0x2a */, verifyInvalid /* 0x2b */, verifyInvalid,
		/* 0x2c */ verifyValid /* 0x2d */, checkJmp /* 0x2e */, verifyInvalid /* 0x2f */, verifyCheckSt,
		/* 0x30 */ verifyInvalid /* 0x31 */, verifyInvalid /* 0x32 */, verifyInvalid /* 0x33 */, verifyInvalid,
		/* 0x34 */ verifyInvalid /* 0x35 */, checkJmp /* 0x36 */, verifyValid /* 0x37 */, verifyCheckSt,
		/* 0x38 */ verifyInvalid /* 0x39 */, verifyInvalid /* 0x3a */, verifyInvalid /* 0x3b */, verifyInvalid,
		/* 0x3c */ verifyValid /* 0x3d */, checkJmp /* 0x3e */, verifyValid /* 0x3f */, verifyCheckSt,
		/* 0x40 */ verifyInvalid /* 0x41 */, verifyInvalid /* 0x42 */, verifyInvalid /* 0x43 */, verifyInvalid,
		/* 0x44 */ verifyValid /* 0x45 */, checkJmp /* 0x46 */, verifyCheckDiv /* 0x47 */, verifyValid,
		/* 0x48 */ verifyInvalid /* 0x49 */, verifyInvalid /* 0x4a */, verifyInvalid /* 0x4b */, verifyInvalid,
		/* 0x4c */ verifyValid /* 0x4d */, checkJmp /* 0x4e */, verifyValid /* 0x4f */, verifyValid,
		/* 0x50 */ verifyInvalid /* 0x51 */, verifyInvalid /* 0x52 */, verifyInvalid /* 0x53 */, verifyInvalid,
		/* 0x54 */ verifyValid /* 0x55 */, checkJmp /* 0x56 */, verifyCheckDiv /* 0x57 */, verifyValid,
		/* 0x58 */ verifyInvalid /* 0x59 */, verifyInvalid /* 0x5a */, verifyInvalid /* 0x5b */, verifyInvalid,
		/* 0x5c */ verifyValid /* 0x5d */, checkJmp /* 0x5e */, verifyValid /* 0x5f */, verifyValid,
		/* 0x60 */ verifyInvalid /* 0x61 */, verifyInvalid /* 0x62 */, verifyInvalid /* 0x63 */, verifyInvalid,
		/* 0x64 */ verifyCheckSh32 /* 0x65 */, checkJmp /* 0x66 */, verifyCheckDiv /* 0x67 */, verifyCheckSh64,
		/* 0x68 */ verifyInvalid /* 0x69 */, verifyInvalid /* 0x6a */, verifyInvalid /* 0x6b */, verifyInvalid,
		/* 0x6c */ verifyValid /* 0x6d */, checkJmp /* 0x6e */, verifyValid /* 0x6f */, verifyValid,
		/* 0x70 */ verifyInvalid /* 0x71 */, verifyInvalid /* 0x72 */, verifyInvalid /* 0x73 */, verifyInvalid,
		/* 0x74 */ verifyCheckSh32 /* 0x75 */, checkJmp /* 0x76 */, verifyCheckDiv /* 0x77 */, verifyCheckSh64,
		/* 0x78 */ verifyInvalid /* 0x79 */, verifyInvalid /* 0x7a */, verifyInvalid /* 0x7b */, verifyInvalid,
		/* 0x7c */ verifyValid /* 0x7d */, checkJmp /* 0x7e */, verifyValid /* 0x7f */, verifyValid,
		/* 0x80 */ verifyInvalid /* 0x81 */, verifyInvalid /* 0x82 */, verifyInvalid /* 0x83 */, verifyInvalid,
		/* 0x84 */ verifyInvalid /* 0x85 */, verifyCheckCallImm /*0x86*/, verifyValid /* 0x87 */, verifyCheckSt,
		/* 0x88 */ verifyInvalid /* 0x89 */, verifyInvalid /* 0x8a */, verifyInvalid /* 0x8b */, verifyInvalid,
		/* 0x8c */ verifyValid /* 0x8d */, verifyCheckCallReg /*0x8e*/, verifyValid /* 0x8f */, verifyCheckSt,
		/* 0x90 */ verifyInvalid /* 0x91 */, verifyInvalid /* 0x92 */, verifyInvalid /* 0x93 */, verifyInvalid,
		/* 0x94 */ verifyInvalid /* 0x95 */, verifyCheckSyscall /*0x96*/, verifyValid /* 0x97 */, verifyCheckSt,
		/* 0x98 */ verifyInvalid /* 0x99 */, verifyInvalid /* 0x9a */, verifyInvalid /* 0x9b */, verifyInvalid,
		/* 0x9c */ verifyValid /* 0x9d */, verifyValid /* 0x9e */, verifyValid /* 0x9f */, verifyCheckSt,
		/* 0xa0 */ verifyInvalid /* 0xa1 */, verifyInvalid /* 0xa2 */, verifyInvalid /* 0xa3 */, verifyInvalid,
		/* 0xa4 */ verifyValid /* 0xa5 */, checkJmp /* 0xa6 */, verifyInvalid /* 0xa7 */, verifyValid,
		/* 0xa8 */ verifyInvalid /* 0xa9 */, verifyInvalid /* 0xaa */, verifyInvalid /* 0xab */, verifyInvalid,
		/* 0xac */ verifyValid /* 0xad */, checkJmp /* 0xae */, verifyInvalid /* 0xaf */, verifyValid,
		/* 0xb0 */ verifyInvalid /* 0xb1 */, verifyInvalid /* 0xb2 */, verifyInvalid /* 0xb3 */, verifyInvalid,
		/* 0xb4 */ verifyValid /* 0xb5 */, checkJmp /* 0xb6 */, verifyValid /* 0xb7 */, verifyValid,
		/* 0xb8 */ verifyInvalid /* 0xb9 */, verifyInvalid /* 0xba */, verifyInvalid /* 0xbb */, verifyInvalid,
		/* 0xbc */ verifyValid /* 0xbd */, checkJmp /* 0xbe */, verifyValid /* 0xbf */, verifyValid,
		/* 0xc0 */ verifyInvalid /* 0xc1 */, verifyInvalid /* 0xc2 */, verifyInvalid /* 0xc3 */, verifyInvalid,
		/* 0xc4 */ verifyCheckSh32 /* 0xc5 */, checkJmp /* 0xc6 */, verifyCheckDiv /* 0xc7 */, verifyCheckSh64,
		/* 0xc8 */ verifyInvalid /* 0xc9 */, verifyInvalid /* 0xca */, verifyInvalid /* 0xcb */, verifyInvalid,
		/* 0xcc */ verifyValid /* 0xcd */, checkJmp /* 0xce */, verifyValid /* 0xcf */, verifyValid,
		/* 0xd0 */ verifyInvalid /* 0xd1 */, verifyInvalid /* 0xd2 */, verifyInvalid /* 0xd3 */, verifyInvalid,
		/* 0xd4 */ verifyInvalid /* 0xd5 */, checkJmp /* 0xd6 */, verifyCheckDiv /* 0xd7 */, verifyInvalid,
		/* 0xd8 */ verifyInvalid /* 0xd9 */, verifyInvalid /* 0xda */, verifyInvalid /* 0xdb */, verifyInvalid,
		/* 0xdc */ verifyCheckEnd /* 0xdd */, checkJmp /* 0xde */, verifyValid /* 0xdf */, verifyInvalid,
		/* 0xe0 */ verifyInvalid /* 0xe1 */, verifyInvalid /* 0xe2 */, verifyInvalid /* 0xe3 */, verifyInvalid,
		/* 0xe4 */ verifyInvalid /* 0xe5 */, verifyInvalid /* 0xe6 */, verifyCheckDiv /* 0xe7 */, verifyInvalid,
		/* 0xe8 */ verifyInvalid /* 0xe9 */, verifyInvalid /* 0xea */, verifyInvalid /* 0xeb */, verifyInvalid,
		/* 0xec */ verifyInvalid /* 0xed */, verifyInvalid /* 0xee */, verifyValid /* 0xef */, verifyInvalid,
		/* 0xf0 */ verifyInvalid /* 0xf1 */, verifyInvalid /* 0xf2 */, verifyInvalid /* 0xf3 */, verifyInvalid,
		/* 0xf4 */ verifyInvalid /* 0xf5 */, verifyInvalid /* 0xf6 */, verifyCheckDiv /* 0xf7 */, verifyValid,
		/* 0xf8 */ verifyInvalid /* 0xf9 */, verifyInvalid /* 0xfa */, verifyInvalid /* 0xfb */, verifyInvalid,
		/* 0xfc */ verifyInvalid /* 0xfd */, verifyInvalid /* 0xfe */, verifyValid /* 0xff */, verifyInvalid,
	}

	/* SIMD-0173: LDDW */
	if v.Program.SbpfVersion.DisableLddw() {
		v.validationMap[0x18] = verifyInvalid
		v.validationMap[0xf7] = verifyValid
	} else {
		v.validationMap[0x18] = verifyCheckLDQ
		v.validationMap[0xf7] = verifyInvalid
	}

	/* SIMD-0173: LE */
	if v.Program.SbpfVersion.DisableLe() {
		v.validationMap[0xd4] = verifyInvalid
	} else {
		v.validationMap[0xd4] = verifyCheckEnd
	}

	/* SIMD-0173: LDXW, STW, STXW
	 * SIMD-0173: LDXH, STH, STXH
	 * SIMD-0173: LDXB, STB, STXB
	 * SIMD-0173: LDXDW, STDW, STXDW */
	if v.Program.SbpfVersion.MoveMemoryInstructionClasses() {
		v.validationMap[0x61] = verifyInvalid
		v.validationMap[0x62] = verifyInvalid
		v.validationMap[0x63] = verifyInvalid
		v.validationMap[0x8c] = verifyValid
		v.validationMap[0x87] = verifyCheckSt
		v.validationMap[0x8f] = verifyCheckSt

		v.validationMap[0x69] = verifyInvalid
		v.validationMap[0x6a] = verifyInvalid
		v.validationMap[0x6b] = verifyInvalid
		v.validationMap[0x3c] = verifyValid
		v.validationMap[0x37] = verifyCheckSt
		v.validationMap[0x3f] = verifyCheckSt

		v.validationMap[0x71] = verifyInvalid
		v.validationMap[0x72] = verifyInvalid
		v.validationMap[0x73] = verifyInvalid
		v.validationMap[0x2c] = verifyValid
		v.validationMap[0x27] = verifyCheckSt
		v.validationMap[0x2f] = verifyCheckSt

		v.validationMap[0x79] = verifyInvalid
		v.validationMap[0x7a] = verifyInvalid
		v.validationMap[0x7b] = verifyInvalid
		v.validationMap[0x9c] = verifyValid
		v.validationMap[0x97] = verifyCheckSt
		v.validationMap[0x9f] = verifyCheckSt
	} else {
		v.validationMap[0x61] = verifyValid
		v.validationMap[0x62] = verifyCheckSt
		v.validationMap[0x63] = verifyCheckSt
		v.validationMap[0x8c] = verifyInvalid
		v.validationMap[0x87] = verifyValid
		v.validationMap[0x8f] = verifyInvalid

		v.validationMap[0x69] = verifyValid
		v.validationMap[0x6a] = verifyCheckSt
		v.validationMap[0x6b] = verifyCheckSt
		v.validationMap[0x3c] = verifyValid
		v.validationMap[0x37] = verifyCheckDiv
		v.validationMap[0x3f] = verifyValid

		v.validationMap[0x71] = verifyValid
		v.validationMap[0x72] = verifyCheckSt
		v.validationMap[0x73] = verifyCheckSt
		v.validationMap[0x2c] = verifyValid
		v.validationMap[0x27] = verifyValid
		v.validationMap[0x2f] = verifyValid

		v.validationMap[0x79] = verifyValid
		v.validationMap[0x7a] = verifyCheckSt
		v.validationMap[0x7b] = verifyCheckSt
		v.validationMap[0x9c] = verifyValid
		v.validationMap[0x97] = verifyCheckDiv
		v.validationMap[0x9f] = verifyValid
	}

	/* SIMD-0173: CALLX */
	if v.Program.SbpfVersion.CallXUsesSrcReg() {
		v.validationMap[0x8d] = verifyCheckCallReg
	} else {
		v.validationMap[0x8d] = verifyCheckCallRegDepr
	}

	/* SIMD-0174: MUL, DIV, MOD */
	if v.Program.SbpfVersion.EnablePqr() {
		v.validationMap[0x24] = verifyInvalid
		v.validationMap[0x34] = verifyInvalid
		v.validationMap[0x94] = verifyInvalid
	} else {
		v.validationMap[0x24] = verifyValid
		v.validationMap[0x34] = verifyCheckDiv
		v.validationMap[0x94] = verifyCheckDiv
	}

	/* SIMD-0174: NEG */
	if v.Program.SbpfVersion.DisableNeg() {
		v.validationMap[0x84] = verifyInvalid
	} else {
		v.validationMap[0x84] = verifyValid
	}

	/* SIMD-0174: MUL, DIV, MOD */
	if v.Program.SbpfVersion.EnablePqr() {
		v.validationMap[0x36] = verifyValid
		v.validationMap[0x3e] = verifyValid
		v.validationMap[0x46] = verifyCheckDiv
		v.validationMap[0x4e] = verifyValid
		v.validationMap[0x56] = verifyCheckDiv
		v.validationMap[0x5e] = verifyValid
		v.validationMap[0x66] = verifyCheckDiv
		v.validationMap[0x6e] = verifyValid
		v.validationMap[0x76] = verifyCheckDiv
		v.validationMap[0x7e] = verifyValid
		v.validationMap[0x86] = verifyValid
		v.validationMap[0x8e] = verifyValid
		v.validationMap[0x96] = verifyValid
		v.validationMap[0x9e] = verifyValid
		v.validationMap[0xb6] = verifyValid
		v.validationMap[0xbe] = verifyValid
		v.validationMap[0xc6] = verifyCheckDiv
		v.validationMap[0xce] = verifyValid
		v.validationMap[0xd6] = verifyCheckDiv
		v.validationMap[0xde] = verifyValid
		v.validationMap[0xe6] = verifyCheckDiv
		v.validationMap[0xee] = verifyValid
		v.validationMap[0xf6] = verifyCheckDiv
		v.validationMap[0xfe] = verifyValid
	} else {
		v.validationMap[0x36] = verifyInvalid
		v.validationMap[0x3e] = verifyInvalid
		v.validationMap[0x46] = verifyInvalid
		v.validationMap[0x4e] = verifyInvalid
		v.validationMap[0x56] = verifyInvalid
		v.validationMap[0x5e] = verifyInvalid
		v.validationMap[0x66] = verifyInvalid
		v.validationMap[0x6e] = verifyInvalid
		v.validationMap[0x76] = verifyInvalid
		v.validationMap[0x7e] = verifyInvalid
		v.validationMap[0x86] = verifyInvalid
		v.validationMap[0x8e] = verifyInvalid
		v.validationMap[0x96] = verifyInvalid
		v.validationMap[0x9e] = verifyInvalid
		v.validationMap[0xb6] = verifyInvalid
		v.validationMap[0xbe] = verifyInvalid
		v.validationMap[0xc6] = verifyInvalid
		v.validationMap[0xce] = verifyInvalid
		v.validationMap[0xd6] = verifyInvalid
		v.validationMap[0xde] = verifyInvalid
		v.validationMap[0xe6] = verifyInvalid
		v.validationMap[0xee] = verifyInvalid
		v.validationMap[0xf6] = verifyInvalid
		v.validationMap[0xfe] = verifyInvalid
	}

	/* SIMD-0178: static syscalls */
	if v.Program.SbpfVersion.EnableStaticSyscalls() {
		v.validationMap[0x85] = verifyCheckCallImm
		v.validationMap[0x95] = verifyCheckSyscall
		v.validationMap[0x9d] = verifyValid
	} else {
		v.validationMap[0x85] = verifyValid
		v.validationMap[0x95] = verifyValid
		v.validationMap[0x9d] = verifyInvalid
	}
}
