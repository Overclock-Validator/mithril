package accountsdb

import (
	"bytes"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestScanIndexEntriesFromAppendVecsBoundsBatches(t *testing.T) {
	var encoded bytes.Buffer
	for i := byte(1); i <= 5; i++ {
		var key solana.PublicKey
		key[0] = i
		require.NoError(t, (&AppendVecAccount{
			Pubkey:   key,
			Lamports: 1,
		}).Marshal(&encoded))
	}

	wantKeys, wantEntries, wantStakes, err := BuildIndexEntriesFromAppendVecs(
		encoded.Bytes(), uint64(encoded.Len()), 10, 11,
	)
	require.NoError(t, err)
	var gotKeys []solana.PublicKey
	var gotEntries []AccountIndexEntry
	var gotStakes []StakeIndexEntry
	var batchLengths []int
	err = ScanIndexEntriesFromAppendVecs(
		encoded.Bytes(), uint64(encoded.Len()), 10, 11, 2,
		func(keys []solana.PublicKey, entries []AccountIndexEntry, stakes []StakeIndexEntry) error {
			batchLengths = append(batchLengths, len(keys))
			gotKeys = append(gotKeys, keys...)
			gotEntries = append(gotEntries, entries...)
			gotStakes = append(gotStakes, stakes...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int{2, 2, 1}, batchLengths)
	require.Equal(t, wantKeys, gotKeys)
	require.Equal(t, wantEntries, gotEntries)
	require.ElementsMatch(t, wantStakes, gotStakes)
}
