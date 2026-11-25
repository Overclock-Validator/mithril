package accounts

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// CloneLarge must return exactly what Clone returns, for data below, at and
// above the size where it starts copying in pieces, and with piece counts that
// do not divide the length.
func TestCloneLargeMatchesClone(t *testing.T) {
	for _, size := range []int{0, 1, largeCloneChunk - 1, largeCloneChunk, 2*largeCloneChunk - 1, 2 * largeCloneChunk, 2*largeCloneChunk + 1, 7*largeCloneChunk + 13} {
		src := &Account{Key: solana.PublicKey{9}, Lamports: 77, Owner: [32]byte{3}, Executable: true, RentEpoch: 5, Data: make([]byte, size)}
		for i := range src.Data {
			src.Data[i] = byte(i*31 + i>>8)
		}
		want := src.Clone()
		for _, workers := range []int{1, 2, 3, 8} {
			got := src.CloneLarge(workers)
			require.Equal(t, want, got, "size %d workers %d", size, workers)
			require.Equal(t, len(want.Data), cap(got.Data))
			if size > 0 {
				require.NotSame(t, &src.Data[0], &got.Data[0])
			}
		}
	}
}
