package turbine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func decodedPrefetchForTest(shreds []*Shred) map[uint32]*prefetchedShredBatch {
	cache := make(map[uint32]*prefetchedShredBatch)
	var raw []byte
	var start uint32
	for _, shred := range shreds {
		if raw == nil {
			start = shred.Index
		}
		raw = append(raw, shred.Data...)
		if shred.DataComplete() {
			batch := decodeClosedShredBatch(raw, start, shred.Index)
			batch.ready = make(chan struct{})
			close(batch.ready)
			cache[start] = batch
			raw = nil
		}
	}
	return cache
}

func TestEntryDecodeReusesOnlyExactPrefetchedBytesAndBounds(t *testing.T) {
	component, err := NewEntryBatch([]Entry{{Hash: solana.Hash{9}, Txns: []solana.Transaction{mustParseTransferTx(t, 21)}}})
	require.NoError(t, err)
	raw, err := MarshalBlockComponent(component)
	require.NoError(t, err)
	for _, mode := range []string{"match", "different_bytes", "different_end", "different_start"} {
		t.Run(mode, func(t *testing.T) {
			shreds := paddedComponentShreds(t, raw, 0, 0xa5)
			cache := decodedPrefetchForTest(shreds)
			cached := cache[0]
			require.NoError(t, cached.err)
			cached.parseDuration = time.Hour // must not enter final parse accounting
			if mode != "match" {
				cached.err = errors.New("stale cached failure must be ignored")
			}
			switch mode {
			case "different_bytes":
				cached.raw[len(cached.raw)-1] ^= 1 // even differing padding invalidates reuse
			case "different_end":
				cached.end++
				cached.ready = make(chan struct{}) // mismatched bounds must never wait
			case "different_start":
				cached.start++
				cached.ready = make(chan struct{})
			}
			timings := entryDecodeTimings{prefetched: cache}
			entries, _, _, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Len(t, timings.all, 1)
			require.Len(t, timings.retained, 1)
			if mode == "match" {
				require.Same(t, cached, timings.retained[0])
				require.Same(t, &cached.entries[0].Txns[0], &entries[0].Txns[0])
				require.Zero(t, timings.transactionParse)
			} else {
				require.NotSame(t, cached, timings.retained[0])
				require.Less(t, timings.transactionParse, time.Hour)
			}
		})
	}
}

func TestEntryDecodePrefetchPreparationWaitHonorsCancellation(t *testing.T) {
	raw, err := marshalEntryBatch([]Entry{{Hash: solana.Hash{3}}})
	require.NoError(t, err)
	shreds := paddedComponentShreds(t, raw, 0, 0)
	cache := decodedPrefetchForTest(shreds)
	cache[0].ready = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	timings := entryDecodeTimings{ctx: ctx, prefetched: cache}
	_, _, _, err = decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, timings.retained)
	require.Zero(t, timings.transactionParse)
}

func TestEntryDecodePrefetchPreservesUpdateParentAndIgnoresVerificationErrors(t *testing.T) {
	prefix, err := NewEntryBatch([]Entry{{Hash: solana.Hash{1}, Txns: []solana.Transaction{mustParseTransferTx(t, 20)}}})
	require.NoError(t, err)
	suffix, err := NewEntryBatch([]Entry{{Hash: solana.Hash{2}, Txns: []solana.Transaction{mustParseTransferTx(t, 21)}}})
	require.NoError(t, err)
	components := []BlockComponent{NewBlockHeader(99, solana.Hash{9}), prefix, NewUpdateParent(98, solana.Hash{8}), suffix, NewBlockFooter(BlockFooter{BankHash: solana.Hash{7}})}
	var shreds []*Shred
	for _, component := range components {
		raw, err := MarshalBlockComponent(component)
		require.NoError(t, err)
		shreds = append(shreds, paddedComponentShreds(t, raw, uint32(len(shreds)), 0xa5)...)
	}
	cache := decodedPrefetchForTest(shreds)
	for _, start := range []uint32{32, 96} {
		// Decoding must not make a signature decision, even for retained entries.
		cache[start].verification = &transactionVerification{err: errors.New("signature result belongs to completion")}
		cache[start].submitErr = errors.New("submission result belongs to completion")
	}
	timings := entryDecodeTimings{prefetched: cache}
	entries, parent, footer, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, solana.Hash{2}, entries[0].Hash)
	require.Equal(t, uint32(64), parent.ReplayFECSetIndex)
	require.Equal(t, solana.Hash{7}, footer.BankHash)
	require.Len(t, timings.all, 5)
	require.Len(t, timings.retained, 1)
	require.Same(t, cache[96], timings.retained[0])

	// Parsing the discarded prefix remains mandatory, unlike its signatures.
	cache[32].err = errors.New("malformed optimistic prefix")
	timings = entryDecodeTimings{prefetched: cache}
	_, _, _, err = decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
	require.ErrorContains(t, err, "malformed optimistic prefix")
}

func TestEntryDecodePrefetchedAgavePaddedCaptureMatchesFullDecode(t *testing.T) {
	packets := agavePaddedSlot1752420Packets(t)
	shreds := make([]*Shred, len(packets))
	for i, packet := range packets {
		var err error
		shreds[i], err = ParseShred(packet)
		require.NoError(t, err)
	}
	wantEntries, wantParent, wantFooter, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
	require.NoError(t, err)
	timings := entryDecodeTimings{prefetched: decodedPrefetchForTest(shreds)}
	entries, parent, footer, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, &timings)
	require.NoError(t, err)
	require.Equal(t, wantEntries, entries)
	require.Equal(t, wantParent, parent)
	require.Equal(t, wantFooter, footer)
	require.Len(t, timings.all, 4)
	require.Len(t, timings.retained, 2)
	require.Zero(t, timings.transactionParse)
}
