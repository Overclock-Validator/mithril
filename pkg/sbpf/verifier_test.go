package sbpf

import (
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

// TestVerifier_ImmShiftBounds pins the immediate shift check to Agave's
// check_imm_shift (anza-xyz/sbpf src/verifier.rs), which rejects
// `imm < 0 || imm >= width` for every immediate shift opcode on every SBPF
// version. Before this check had a lower bound, `arsh64 r0, -1` passed
// verification and then panicked the interpreter with a negative shift count.
func TestVerifier_ImmShiftBounds(t *testing.T) {
	shifts := []struct {
		name  string
		op    uint8
		width int64
	}{
		{"lsh32", OpLsh32Imm, 32},
		{"rsh32", OpRsh32Imm, 32},
		{"arsh32", OpArsh32Imm, 32},
		{"lsh64", OpLsh64Imm, 64},
		{"rsh64", OpRsh64Imm, 64},
		{"arsh64", OpArsh64Imm, 64},
	}
	versions := []uint32{
		sbpfver.SbpfVersionV0,
		sbpfver.SbpfVersionV1,
		sbpfver.SbpfVersionV2,
		sbpfver.SbpfVersionV3,
	}

	for _, ver := range versions {
		for _, sh := range shifts {
			cases := []struct {
				imm   int64
				valid bool
			}{
				{math.MinInt32, false},
				{-1, false},
				{0, true},
				{sh.width - 1, true},
				{sh.width, false},
				{math.MaxInt32, false},
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("v%d/%s/imm=%d", ver, sh.name, tc.imm), func(t *testing.T) {
					text := []Slot{
						slot(sh.op, 0, 0, 0, uint32(int32(tc.imm))),
						slot(OpExit, 0, 0, 0, 0),
					}
					err := mkProgram(text, ver).Verify()
					if tc.valid {
						require.NoError(t, err)
					} else {
						require.EqualError(t, err, "sh overflow")
					}
				})
			}
		}
	}
}

// TestVerifier_NegativeArsh64ImmDoesNotReachInterpreter is the reproducer from
// issue #248: the program must be rejected by Verify, so the interpreter's
// signed shift is never evaluated with a negative count.
func TestVerifier_NegativeArsh64ImmDoesNotReachInterpreter(t *testing.T) {
	text := []Slot{
		slot(OpMov64Imm, 0, 0, 0, 1),
		slot(OpArsh64Imm, 0, 0, 0, 0xffffffff), // arsh64 r0, -1
		slot(OpExit, 0, 0, 0, 0),
	}
	require.EqualError(t, mkProgram(text, sbpfver.SbpfVersionV0).Verify(), "sh overflow")
}
