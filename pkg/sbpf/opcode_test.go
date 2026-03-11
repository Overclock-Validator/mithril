package sbpf

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpcodes(t *testing.T) {
	type testCase struct {
		want uint8
		have uint8
	}
	cases := []testCase{
		{0x18, OpLddw},

		{0x71, OpLdxb},
		{0x69, OpLdxh},
		{0x61, OpLdxw},
		{0x79, OpLdxdw},
		{0x72, OpStb},
		{0x6a, OpSth},
		{0x62, OpStw},
		{0x7a, OpStdw},
		{0x73, OpStxb},
		{0x6b, OpStxh},
		{0x63, OpStxw},
		{0x7b, OpStxdw},

		{0x04, OpAdd32Imm},
		{0x07, OpAdd64Imm},
		{0x0c, OpAdd32Reg},
		{0x0f, OpAdd64Reg},
		{0x14, OpSub32Imm},
		{0x17, OpSub64Imm},
		{0x1c, OpSub32Reg},
		{0x1f, OpSub64Reg},
		{0x24, OpMul32Imm},
		{0x27, OpMul64Imm},
		{0x2c, OpMul32Reg},
		{0x2f, OpMul64Reg},
		{0x34, OpDiv32Imm},
		{0x37, OpDiv64Imm},
		{0x3c, OpDiv32Reg},
		{0x3f, OpDiv64Reg},
		{0x44, OpOr32Imm},
		{0x47, OpOr64Imm},
		{0x4c, OpOr32Reg},
		{0x4f, OpOr64Reg},
		{0x54, OpAnd32Imm},
		{0x57, OpAnd64Imm},
		{0x5c, OpAnd32Reg},
		{0x5f, OpAnd64Reg},
		{0x64, OpLsh32Imm},
		{0x67, OpLsh64Imm},
		{0x6c, OpLsh32Reg},
		{0x6f, OpLsh64Reg},
		{0x74, OpRsh32Imm},
		{0x77, OpRsh64Imm},
		{0x7c, OpRsh32Reg},
		{0x7f, OpRsh64Reg},
		{0x84, OpNeg32},
		{0x87, OpNeg64},
		{0x94, OpMod32Imm},
		{0x97, OpMod64Imm},
		{0x9c, OpMod32Reg},
		{0x9f, OpMod64Reg},
		{0xa4, OpXor32Imm},
		{0xa7, OpXor64Imm},
		{0xac, OpXor32Reg},
		{0xaf, OpXor64Reg},
		{0xb4, OpMov32Imm},
		{0xb7, OpMov64Imm},
		{0xbc, OpMov32Reg},
		{0xbf, OpMov64Reg},
		{0xc4, OpArsh32Imm},
		{0xc7, OpArsh64Imm},
		{0xcc, OpArsh32Reg},
		{0xcf, OpArsh64Reg},
		{0xd4, OpLe},
		{0xdc, OpBe},
		{0xc6, OpSdiv32Imm},
		{0xd6, OpSdiv64Imm},
		{0xce, OpSdiv32Reg},
		{0xde, OpSdiv64Reg},

		{0x05, OpJa},
		{0x15, OpJeqImm},
		{0x1d, OpJeqReg},
		{0x25, OpJgtImm},
		{0x2d, OpJgtReg},
		{0x35, OpJgeImm},
		{0x3d, OpJgeReg},
		{0x45, OpJsetImm},
		{0x4d, OpJsetReg},
		{0x55, OpJneImm},
		{0x5d, OpJneReg},
		{0x65, OpJsgtImm},
		{0x6d, OpJsgtReg},
		{0x75, OpJsgeImm},
		{0x7d, OpJsgeReg},
		{0xa5, OpJltImm},
		{0xad, OpJltReg},
		{0xb5, OpJleImm},
		{0xbd, OpJleReg},
		{0xc5, OpJsltImm},
		{0xcd, OpJsltReg},
		{0xd5, OpJsleImm},
		{0xdd, OpJsleReg},

		{0x85, OpCall},
		{0x8d, OpCallx},
		{0x95, OpExit},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("Op_%#02x", c.want), func(t *testing.T) {
			require.Equal(t, c.want, c.have)
		})
	}
}
