package sealevel

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// Measures replacement after invocation/CPI. Fixture creation and input decoding
// are outside the clock; each iteration restores identical transaction state.
func BenchmarkAccountDataReplacement(b *testing.B) {
	for _, size := range []int{165, 4096, 65536} {
		for _, mode := range []string{"shared", "private", "repeated_cpi"} {
			b.Run(fmt.Sprintf("%s/%d", mode, size), func(b *testing.B) {
				ctx := serializationTestContext(b, 1)
				tx := ctx.TransactionContext
				ix, err := tx.CurrentInstructionCtx()
				require.NoError(b, err)
				acct, err := ix.BorrowInstructionAccount(tx, 0)
				require.NoError(b, err)
				defer acct.Drop()
				original := acct.Account
				original.Data = bytes.Repeat([]byte{7}, size)
				input := bytes.Repeat([]byte{42}, size)
				input2 := bytes.Repeat([]byte{43}, size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx.Accounts.Accounts[1] = original
					acct.Account = original
					tx.Accounts.Shared[1] = mode != "private"
					tx.Accounts.Touched[1] = false
					tx.AccountsResizeDelta = 0
					if err := acct.SetData(ctx.Features, input); err != nil {
						b.Fatal(err)
					}
					if mode == "repeated_cpi" {
						for j := 0; j < 8; j++ {
							if err := acct.SetData(ctx.Features, input2); err != nil {
								b.Fatal(err)
							}
							if err := acct.SetData(ctx.Features, input); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
				b.StopTimer()
				require.Equal(b, input, acct.Data())
				require.Zero(b, tx.AccountsResizeDelta)
				require.True(b, tx.Accounts.Touched[1])
				require.False(b, tx.Accounts.Shared[1])
				if mode != "private" {
					require.Equal(b, bytes.Repeat([]byte{7}, size), original.Data)
				}
			})
		}
	}
}
