package accountsdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Both revisions run this identical benchmark. All reads use a real Pebble
// index; setup and periodic publication-epoch changes are outside timing.
func BenchmarkBatchRepeatedAbsences(b *testing.B) {
	for _, missing := range []int{0, 256, 512} {
		for _, changeEpoch := range []bool{false, true} {
			b.Run(fmt.Sprintf("missing=%d/epochChange=%t", missing, changeEpoch), func(b *testing.B) {
				db, _ := newFoldTestDb(b)
				defer db.CloseDb()
				keys := make([]solana.PublicKey, 512)
				delta := make([]*accounts.Account, 0, 512-missing)
				for i := range keys {
					binary.LittleEndian.PutUint64(keys[i][:], uint64(i+1))
					if i >= missing {
						delta = append(delta, &accounts.Account{Key: keys[i], Lamports: 1, Data: []byte{1}})
					}
				}
				if len(delta) > 0 {
					_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
					require.NoError(b, err)
				}
				_, err := db.GetAccountsBatch(context.Background(), 100, keys)
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if changeEpoch && i%32 == 0 {
						b.StopTimer()
						db.refreshReadCaches(nil)
						b.StartTimer()
					}
					out, err := db.GetAccountsBatch(context.Background(), 100, keys)
					if err != nil || len(out) != len(keys) {
						b.Fatalf("read: %v", err)
					}
					for j, acct := range out {
						want := uint64(1)
						if j < missing {
							want = 0
						}
						if acct.Key != keys[j] || acct.Lamports != want {
							b.Fatal("incorrect batch result")
						}
					}
				}
			})
		}
	}
}
