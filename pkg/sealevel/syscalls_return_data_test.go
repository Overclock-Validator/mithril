package sealevel

import (
	"bytes"
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSyscallGetReturnDataPrefix(t *testing.T) {
	for _, n := range []uint64{0, 1, 7, 32, 64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			input := bytes.Repeat([]byte{0xCC}, 128)
			vm, ctx := newMemSyscallVM(t, input, nil)
			ctx.TransactionContext = &TransactionCtx{}
			data := bytes.Repeat([]byte{0x5A}, 32)
			pk := solana.PublicKey{0x72}
			ctx.TransactionContext.SetReturnData(pk, data)
			before := ctx.ComputeMeter.Remaining()
			got, err := SyscallGetReturnDataImpl(vm, sbpf.VaddrInput, n, sbpf.VaddrInput+64)
			require.NoError(t, err)
			require.Equal(t, uint64(len(data)), got)
			copied := min(n, uint64(len(data)))
			actual, err := vm.Translate(sbpf.VaddrInput, 128, false)
			require.NoError(t, err)
			require.Equal(t, data[:copied], actual[:copied])
			require.Equal(t, bytes.Repeat([]byte{0xCC}, int(64-copied)), actual[copied:64])
			charge := uint64(cu.CUSyscallBaseCost)
			if copied != 0 {
				require.Equal(t, pk[:], actual[64:96])
				charge += (copied + 32) / cu.CUCpiBytesPerUnit
			} else {
				require.Equal(t, bytes.Repeat([]byte{0xCC}, 32), actual[64:96])
			}
			require.Equal(t, charge, before-ctx.ComputeMeter.Remaining())
			_, retained := ctx.TransactionContext.ReturnData()
			require.Equal(t, data, retained)
		})
	}
}
