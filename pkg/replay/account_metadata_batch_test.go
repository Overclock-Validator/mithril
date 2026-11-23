package replay

import (
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func metadataBatchFixture(n int) (*sealevel.SlotCtx, *solana.Transaction, []*solana.AccountMeta) {
	ctx := newSimd186SlotCtx()
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: make([]solana.PublicKey, n)}}
	metas := make([]*solana.AccountMeta, n)
	for i := 0; i < n; i++ {
		key := solana.PublicKey{byte(i + 1)}
		tx.Message.AccountKeys[i] = key
		metas[i] = &solana.AccountMeta{PublicKey: key, IsSigner: i%3 == 0, IsWritable: i%2 == 0}
		_ = ctx.Accounts.SetAccountWithoutLock(key, &accounts.Account{Key: key, Owner: addresses.SystemProgramAddr, Lamports: 100, Data: []byte{1}})
	}
	return ctx, tx, metas
}
func TestAccountMetadataBatchOwnership(t *testing.T) {
	for _, n := range []int{0, 1, 8, 32, 128} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			ctx, tx, metas := metadataBatchFixture(n)
			if n > 1 {
				metas[1] = &solana.AccountMeta{PublicKey: metas[0].PublicKey, IsSigner: false, IsWritable: false}
			}
			load := func() *sealevel.TransactionAccounts {
				out, returned, _, err := loadAndValidateTxAcctsSimd186(ctx, metas, tx, nil, nil, math.MaxUint32, nil)
				require.NoError(t, err)
				require.Equal(t, metas, returned)
				return out
			}
			first := load()
			want := make([]sealevel.AccountMeta, n)
			seen := map[*sealevel.AccountMeta]bool{}
			for i, v := range first.AcctMetas {
				want[i] = sealevel.AccountMeta{Pubkey: metas[i].PublicKey, IsSigner: metas[i].IsSigner, IsWritable: metas[i].IsWritable}
				require.Equal(t, want[i], *v)
				require.False(t, seen[v])
				seen[v] = true
			}
			second := load()
			for i, v := range second.AcctMetas {
				require.NotSame(t, first.AcctMetas[i], v)
				v.IsSigner = !v.IsSigner
				metas[i].IsWritable = !metas[i].IsWritable
			}
			for i, v := range first.AcctMetas {
				require.Equal(t, want[i], *v, "metadata retained by an older transaction must remain independent")
			}
		})
	}
}
func BenchmarkAccountMetadataBatch(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			ctx, tx, metas := metadataBatchFixture(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, _, _, err := loadAndValidateTxAcctsSimd186(ctx, metas, tx, nil, nil, math.MaxUint32, nil)
				if err != nil || len(out.AcctMetas) != n {
					b.Fatal("loader changed", err)
				}
			}
		})
	}
}
