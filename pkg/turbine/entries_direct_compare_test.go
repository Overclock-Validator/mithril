package turbine

import (
	"bytes"
	"context"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestDataShredBatchMatchesChecksLengthsAndEverySlice(t *testing.T) {
	raw := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	shreds := []*Shred{
		{Type: ShredTypeData, Data: raw[:10]},
		nil,
		{Type: ShredTypeCode, Data: []byte("coding bytes are not component data")},
		{Type: ShredTypeData},
		{Type: ShredTypeData, Data: raw[10:20]},
		{Type: ShredTypeData, Data: raw[20:]},
	}
	require.True(t, dataShredBatchMatches(shreds, bytes.Clone(raw)))
	for i := range raw {
		changed := bytes.Clone(raw)
		changed[i] ^= 1
		require.False(t, dataShredBatchMatches(shreds, changed), "changed byte %d", i)
	}
	for i := 0; i < len(raw); i++ {
		require.False(t, dataShredBatchMatches(shreds, raw[:i]), "truncated at %d", i)
	}
	require.False(t, dataShredBatchMatches(shreds, append(bytes.Clone(raw), 0)))
	require.True(t, dataShredBatchMatches(nil, nil))
	require.False(t, dataShredBatchMatches(nil, raw))
}

func TestEntryDecodeDirectComparisonCannotReuseChangedSignatureVerdict(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	txs := verifierSignedTransactions(t, 1)
	raw := prefetchTestPayload(t, txs)
	cache := decodedPrefetchForTest(paddedComponentShreds(t, raw, 0, 0xa5))
	cached := cache[0]
	var err error
	cached.verification, err = v.submitTransactions(context.Background(), entryBatchTransactions(cached.entries))
	require.NoError(t, err)
	_, err = cached.verification.wait()
	require.NoError(t, err)

	bad := *txs[0]
	bad.Signatures = append([]solana.Signature(nil), bad.Signatures...)
	bad.Signatures[0][0] ^= 1
	changed := prefetchTestPayload(t, []*solana.Transaction{&bad})
	require.Len(t, changed, len(raw))
	timings := entryDecodeTimings{prefetched: cache}
	entries, _, _, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(paddedComponentShreds(t, changed, 0, 0xa5), &timings)
	require.NoError(t, err)
	require.Len(t, timings.retained, 1)
	require.NotSame(t, cached, timings.retained[0])
	require.Nil(t, timings.retained[0].verification)
	blk := BlockFromEntries(100, 99, entries)
	require.Equal(t, bad.Signatures[0], blk.Transactions[0].Signatures[0])
	require.ErrorContains(t, verifyDecodedEntryBatches(context.Background(), blk, timings.retained, v), "failed signature verification")
	require.False(t, blk.TransactionSignaturesVerified())
}

func TestEntryDecodeDirectComparisonRejectsChangedCachedLength(t *testing.T) {
	raw := prefetchTestPayload(t, verifierSignedTransactions(t, 1))
	for _, delta := range []int{-1, 1} {
		shreds := paddedComponentShreds(t, raw, 0, 0xa5)
		cache := decodedPrefetchForTest(shreds)
		cached := cache[0]
		if delta < 0 {
			cached.raw = cached.raw[:len(cached.raw)-1]
		} else {
			cached.raw = append(cached.raw, 0)
		}
		timings := entryDecodeTimings{prefetched: cache}
		entries, _, _, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.NotSame(t, cached, timings.retained[0])
	}
}

func FuzzDataShredBatchMatchesConcatenatedBytes(f *testing.F) {
	f.Add([]byte("a component spanning several shreds"), []byte("a component spanning several shreds"), uint8(3))
	f.Add([]byte("truncated"), []byte("truncate"), uint8(1))
	f.Add([]byte{}, []byte{}, uint8(0))
	f.Fuzz(func(t *testing.T, data, candidate []byte, split uint8) {
		if len(data) > 64*1024 || len(candidate) > 64*1024 {
			t.Skip()
		}
		shreds := []*Shred{nil, {Type: ShredTypeCode, Data: []byte{1, 2, 3}}}
		width := int(split) + 1
		for start := 0; start < len(data); start += width {
			shreds = append(shreds, &Shred{Type: ShredTypeData, Data: data[start:min(start+width, len(data))]})
		}
		require.Equal(t, bytes.Equal(data, candidate), dataShredBatchMatches(shreds, candidate))
	})
}
