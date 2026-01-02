package sbpf

import "fmt"

var mnemonicTable = [0x100]string{
	OpLddw:      "lddw",
	OpLdxb:      "ldxb",
	OpLdxh:      "ldxh",
	OpLdxw:      "ldxw",
	OpLdxdw:     "ldxdw",
	OpStb:       "stb",
	OpSth:       "sth",
	OpStw:       "stw",
	OpStdw:      "stdw",
	OpStxb:      "stxb",
	OpStxh:      "stxh",
	OpStxw:      "stxw",
	OpStxdw:     "stxdw",
	OpAdd32Imm:  "add32",
	OpAdd32Reg:  "add32",
	OpSub32Imm:  "sub32",
	OpSub32Reg:  "sub32",
	OpMul32Imm:  "mul32",
	OpMul32Reg:  "mul32",
	OpDiv32Imm:  "div32",
	OpDiv32Reg:  "div32",
	OpOr32Imm:   "or32",
	OpOr32Reg:   "or32",
	OpAnd32Imm:  "and32",
	OpAnd32Reg:  "and32",
	OpLsh32Imm:  "lsh32",
	OpLsh32Reg:  "lsh32",
	OpRsh32Imm:  "rsh32",
	OpRsh32Reg:  "rsh32",
	OpNeg32:     "neg32",
	OpMod32Imm:  "mod32imm",
	OpMod32Reg:  "mod32reg",
	OpXor32Imm:  "xor32",
	OpXor32Reg:  "xor32",
	OpMov32Imm:  "mov32",
	OpMov32Reg:  "mov32",
	OpArsh32Imm: "arsh32",
	OpArsh32Reg: "arsh32",
	OpSdiv32Imm: "sdiv32",
	OpSdiv32Reg: "sdiv32",
	OpLe:        "le",
	OpBe:        "be",
	OpAdd64Imm:  "add64",
	OpAdd64Reg:  "add64",
	OpSub64Imm:  "sub64",
	OpSub64Reg:  "sub64",
	OpMul64Imm:  "mul64",
	OpMul64Reg:  "mul64",
	OpDiv64Imm:  "div64",
	OpDiv64Reg:  "div64",
	OpOr64Imm:   "or64",
	OpOr64Reg:   "or64",
	OpAnd64Imm:  "and64",
	OpAnd64Reg:  "and64",
	OpLsh64Imm:  "lsh64",
	OpLsh64Reg:  "lsh64",
	OpRsh64Imm:  "rsh64",
	OpRsh64Reg:  "rsh64",
	OpNeg64:     "neg64",
	OpMod64Imm:  "mod64",
	OpMod64Reg:  "mod64",
	OpXor64Imm:  "xor64",
	OpXor64Reg:  "xor64",
	OpMov64Imm:  "mov64",
	OpMov64Reg:  "mov64",
	OpArsh64Imm: "arsh64",
	OpArsh64Reg: "arsh64",
	OpSdiv64Imm: "sdiv64",
	OpSdiv64Reg: "sdiv64",
	OpJa:        "ja",
	OpJeqImm:    "jeq",
	OpJeqReg:    "jeq",
	OpJgtImm:    "jgt",
	OpJgtReg:    "jgt",
	OpJgeImm:    "jge",
	OpJgeReg:    "jge",
	OpJltImm:    "jlt",
	OpJltReg:    "jlt",
	OpJleImm:    "jle",
	OpJleReg:    "jle",
	OpJsetImm:   "jset",
	OpJsetReg:   "jset",
	OpJneImm:    "jne",
	OpJneReg:    "jne",
	OpJsgtImm:   "jsgt",
	OpJsgtReg:   "jsgt",
	OpJsgeImm:   "jsge",
	OpJsgeReg:   "jsge",
	OpJsltImm:   "jslt",
	OpJsltReg:   "jslt",
	OpJsleImm:   "jsle",
	OpJsleReg:   "jsle",
	OpCall:      "call",
	OpCallx:     "callx",
	OpExit:      "exit",
	OpSt4BReg:   "OpSt4BReg",
}

func (ip *Interpreter) GetOpcodeName(opc uint8) string {
	return mnemonicTable[opc]
}

func (ip *Interpreter) disassemble(slot Slot, slot2 Slot) string {
	opc := slot.Op()
	mnemonic := ip.GetOpcodeName(opc)
	switch opc {
	case OpLddw:
		return fmt.Sprintf("lddw r%d", slot.Dst())
	case OpLdxb, OpLdxh, OpLdxw, OpLdxdw:
		return fmt.Sprintf("%s r%d, [r%d%#+x]", mnemonic, slot.Dst(), slot.Src(), slot.Off())
	case OpStb:
		return fmt.Sprintf("stb [r%d%#+x], %#x", slot.Src(), slot.Off(), int8(slot.Imm()))
	case OpSth:
		return fmt.Sprintf("sth [r%d%#+x], %#x", slot.Src(), slot.Off(), int16(slot.Imm()))
	case OpStw:
		return fmt.Sprintf("stw [r%d%#+x], %#x", slot.Src(), slot.Off(), slot.Imm())
	case OpStdw:
		return fmt.Sprintf("stdw [r%d%#+x], %#x", slot.Src(), slot.Off(), int64(slot.Imm()))
	case OpStxb, OpStxh, OpStxw, OpStxdw:
		return fmt.Sprintf("%s [r%d%#+x], r%d", mnemonic, slot.Dst(), slot.Off(), slot.Src())
	case OpAdd32Imm, OpSub32Imm, OpAdd64Imm, OpSub64Imm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Imm())
	case OpOr32Imm, OpAnd32Imm, OpXor32Imm, OpMov32Imm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Uimm())
	case OpDiv32Imm, OpMod32Imm, OpLsh32Imm, OpRsh32Imm, OpArsh32Imm,
		OpDiv64Imm, OpMod64Imm, OpLsh64Imm, OpRsh64Imm, OpArsh64Imm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Uimm())
	case OpMul32Imm, OpSdiv32Imm, OpMul64Imm, OpSdiv64Imm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Imm())
	case OpOr64Imm, OpAnd64Imm, OpXor64Imm, OpMov64Imm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Imm())
	case OpAdd32Reg, OpSub32Reg, OpMul32Reg, OpDiv32Reg, OpOr32Reg, OpAnd32Reg, OpLsh32Reg, OpRsh32Reg, OpMod32Reg, OpXor32Reg, OpMov32Reg, OpArsh32Reg, OpSdiv32Reg,
		OpAdd64Reg, OpSub64Reg, OpMul64Reg, OpDiv64Reg, OpOr64Reg, OpAnd64Reg, OpLsh64Reg, OpRsh64Reg, OpMod64Reg, OpXor64Reg, OpMov64Reg, OpArsh64Reg, OpSdiv64Reg:
		return fmt.Sprintf("%s r%d, r%d", mnemonic, slot.Dst(), slot.Src())
	case OpNeg32, OpNeg64:
		if ip.sbpfVersion.MoveMemoryInstructionClasses() {
			return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), slot.Uimm())
		} else {
			return fmt.Sprintf("%s r%d", mnemonic, slot.Dst())
		}
	case OpSt4BReg:
		return fmt.Sprintf("%s r%d, r%d", mnemonic, slot.Dst(), slot.Src())
	case OpLe, OpBe:
		return fmt.Sprintf("%s%d r%d", mnemonic, slot.Uimm(), slot.Dst())
	case OpJa:
		return "ja"
	case OpJeqImm, OpJgtImm, OpJgeImm, OpJltImm, OpJleImm, OpJsetImm, OpJneImm, OpJsgtImm, OpJsgeImm, OpJsltImm, OpJsleImm:
		return fmt.Sprintf("%s r%d, %d", mnemonic, slot.Dst(), int64(slot.Imm()))
	case OpJeqReg, OpJgtReg, OpJgeReg, OpJltReg, OpJleReg, OpJsetReg, OpJneReg, OpJsgtReg, OpJsgeReg, OpJsltReg, OpJsleReg:
		return fmt.Sprintf("%s r%d, r%d", mnemonic, slot.Dst(), slot.Src())
	case OpCall:
		return "call"
	case OpCallx:
		return "callx"
	case OpExit:
		return "exit"
	default:
		return fmt.Sprintf("invalid (%x)", opc)
	}
}
