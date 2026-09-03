package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendVecReadGroupSharesDescriptorUntilLastChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendvec")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))

	group := &appendVecReadGroup{}
	group.remaining.Store(2)
	first, err := group.open(path)
	require.NoError(t, err)
	second, err := group.open(path)
	require.NoError(t, err)
	assert.Same(t, first, second)

	group.chunkDone()
	var one [1]byte
	_, err = first.ReadAt(one[:], 0)
	require.NoError(t, err, "the first completed chunk must not close a shared descriptor")

	group.chunkDone()
	_, err = first.ReadAt(one[:], 0)
	assert.ErrorIs(t, err, os.ErrClosed, "the final chunk must release the descriptor")
}

func TestAppendVecReadGroupRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "appendvec")
	require.NoError(t, os.WriteFile(target, []byte("account bytes"), 0o600))
	require.NoError(t, os.Symlink(target, link))

	group := &appendVecReadGroup{}
	file, err := group.open(link)
	require.Error(t, err)
	assert.Nil(t, file)
}

func TestGetAccountsBatchSharesAppendVecAcrossReadChunks(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	const accountCount = appendVecReadChunkSize*2 + 7
	delta := make([]*accounts.Account, accountCount)
	keys := make([]solana.PublicKey, accountCount)
	for i := range delta {
		binary.LittleEndian.PutUint64(keys[i][:8], uint64(i+1))
		delta[i] = &accounts.Account{
			Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{7}, Data: []byte{byte(i)},
		}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(t, err)
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
	}

	request := make([]solana.PublicKey, accountCount)
	for i := range request {
		request[i] = keys[len(keys)-1-i]
	}
	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, request)
	require.NoError(t, err)
	require.Len(t, out, accountCount)
	for i, acct := range out {
		assert.Equal(t, request[i], acct.Key)
		assert.Equal(t, uint64(accountCount-i), acct.Lamports)
	}
	assert.Equal(t, uint64(1), stats.UniqueAppendVecs)
	assert.Equal(t, uint64(3), stats.AppendVecChunks, "one descriptor must still support parallel chunk scheduling")
	assert.Equal(t, uint64(accountCount), stats.AppendVecAccounts)
}

func TestUnmarshalAppendVecExpectedAtPreservesVerificationAndBounds(t *testing.T) {
	stored := solana.PublicKey{1, 2, 3}
	appendVecAcct := AppendVecAccount{
		DataLen: 4, Pubkey: stored, Lamports: 99, RentEpoch: 7,
		Owner: solana.PublicKey{9}, Executable: true, Data: []byte{4, 5, 6, 7},
	}
	var encoded bytes.Buffer
	require.NoError(t, appendVecAcct.Marshal(&encoded))
	openReader := func(data []byte) *os.File {
		path := filepath.Join(t.TempDir(), "appendvec")
		require.NoError(t, os.WriteFile(path, data, 0o600))
		file, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		return file
	}

	acct, exact, err := unmarshalAcctFromAppendVecAcctHeaderExpectedAt(openReader(encoded.Bytes()), 0, stored)
	require.NoError(t, err)
	require.True(t, exact)
	assert.Equal(t, stored, acct.Key)
	assert.Equal(t, appendVecAcct.Data, acct.Data)
	assert.Equal(t, appendVecAcct.Lamports, acct.Lamports)

	other := solana.PublicKey{8}
	acct, exact, err = unmarshalAcctFromAppendVecAcctHeaderExpectedAt(openReader(encoded.Bytes()), 0, other)
	require.NoError(t, err)
	assert.False(t, exact)
	assert.Equal(t, stored, acct.Key)
	assert.Nil(t, acct.Data, "a false StreamHash candidate must not allocate or read account data")

	_, _, err = unmarshalAcctFromAppendVecAcctHeaderExpectedAt(
		openReader(encoded.Bytes()[:hdrLen+len(appendVecAcct.Data)-1]), 0, stored,
	)
	assert.ErrorIs(t, err, io.EOF)

	malformed := append([]byte(nil), encoded.Bytes()...)
	binary.LittleEndian.PutUint64(malformed[dataLenOffset:dataLenOffset+8], ^uint64(0))
	_, _, err = unmarshalAcctFromAppendVecAcctHeaderExpectedAt(openReader(malformed), 0, stored)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "overflows addressable range")

	// A corrupt but addressable length must be rejected against the protocol
	// maximum before attempting the allocation.
	malformed = append([]byte(nil), encoded.Bytes()...)
	binary.LittleEndian.PutUint64(malformed[dataLenOffset:dataLenOffset+8], 1<<30)
	_, _, err = unmarshalAcctFromAppendVecAcctHeaderExpectedAt(openReader(malformed), 0, stored)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "exceeds maximum")

	// The unused-storage marker is EOF even if its ignored data length is
	// corrupt. It must never be returned as the default-pubkey account.
	terminator := make([]byte, hdrLen)
	binary.LittleEndian.PutUint64(terminator[dataLenOffset:dataLenOffset+8], ^uint64(0))
	_, exact, err = unmarshalAcctFromAppendVecAcctHeaderExpectedAt(
		openReader(terminator), 0, solana.PublicKey{},
	)
	assert.ErrorIs(t, err, io.EOF)
	assert.False(t, exact)
}

func TestUnmarshalAppendVecExpectedReaderPreservesTerminatorAndAllocationBounds(t *testing.T) {
	stored := solana.PublicKey{3, 2, 1}
	header := make([]byte, hdrLen)
	copy(header[pubkeyOffset:pubkeyOffset+32], stored[:])
	binary.LittleEndian.PutUint64(header[lamportsOffset:lamportsOffset+8], 1)
	binary.LittleEndian.PutUint64(header[dataLenOffset:dataLenOffset+8], 1024)

	_, _, err := unmarshalAcctFromAppendVecAcctHeaderExpected(bytes.NewReader(header), stored)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "available bytes")

	terminator := make([]byte, hdrLen)
	binary.LittleEndian.PutUint64(terminator[dataLenOffset:dataLenOffset+8], ^uint64(0))
	_, exact, err := unmarshalAcctFromAppendVecAcctHeaderExpected(
		bytes.NewReader(terminator), solana.PublicKey{},
	)
	assert.ErrorIs(t, err, io.EOF)
	assert.False(t, exact)
}

func TestGetAppendVecDataLenRejectsOffsetOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendvec")
	require.NoError(t, os.WriteFile(path, make([]byte, hdrLen), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	_, err = GetAppendVecDataLen(file, ^uint64(0))
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "overflows int64")
}

func readBatchAccountAtWithSectionReader(
	file *os.File,
	path string,
	location batchAccountLocation,
) (*accounts.Account, error) {
	if location.entry.Offset > math.MaxInt64 {
		return nil, fmt.Errorf("account offset %d overflows int64", location.entry.Offset)
	}
	offset := int64(location.entry.Offset)
	acct, exact, err := unmarshalAcctFromAppendVecAcctHeaderExpected(
		io.NewSectionReader(file, offset, math.MaxInt64-offset), location.pubkey,
	)
	if err != nil {
		return nil, fmt.Errorf("unmarshal account at %s@%d: %w", path, location.entry.Offset, err)
	}
	if !exact {
		if location.source == accountIndexSourceBase {
			return nil, ErrNoAccount
		}
		return nil, fmt.Errorf("record at %s@%d holds %s (stale index entry)", path, location.entry.Offset, acct.Key)
	}
	acct.Slot = location.entry.Slot
	return acct, nil
}

func BenchmarkReadBatchAccountAt(b *testing.B) {
	for _, dataSize := range []int{0, 128, 1024} {
		b.Run(fmt.Sprintf("data-%04d", dataSize), func(b *testing.B) {
			key := solana.PublicKey{1, 2, 3}
			appendVecAcct := AppendVecAccount{
				DataLen: uint64(dataSize), Pubkey: key, Lamports: 99,
				Owner: solana.PublicKey{9}, Data: make([]byte, dataSize),
			}
			path := filepath.Join(b.TempDir(), "appendvec")
			file, err := os.Create(path)
			require.NoError(b, err)
			require.NoError(b, appendVecAcct.Marshal(file))
			require.NoError(b, file.Sync())
			require.NoError(b, file.Close())
			file, err = os.Open(path)
			require.NoError(b, err)
			defer file.Close()

			location := batchAccountLocation{
				pubkey: key,
				entry:  AccountIndexEntry{Slot: 100, FileId: 1, Offset: 0},
				source: accountIndexSourceDelta,
			}
			b.Run("reader-at", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(hdrLen + dataSize))
				var sink *accounts.Account
				for range b.N {
					sink, err = readBatchAccountAt(file, path, location)
					if err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(sink)
			})
			b.Run("section-reader", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(hdrLen + dataSize))
				var sink *accounts.Account
				for range b.N {
					sink, err = readBatchAccountAtWithSectionReader(file, path, location)
					if err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(sink)
			})
		})
	}
}

func BenchmarkAppendVecChunkDescriptor(b *testing.B) {
	path := filepath.Join(b.TempDir(), "appendvec")
	require.NoError(b, os.WriteFile(path, []byte("x"), 0o600))

	b.Run("shared", func(b *testing.B) {
		group := &appendVecReadGroup{}
		defer group.close()
		b.ReportAllocs()
		for range b.N {
			file, err := group.open(path)
			if err != nil || file == nil {
				b.Fatalf("open shared appendvec: %v", err)
			}
		}
	})
	b.Run("reopen", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			file, err := os.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			if err := file.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestAppendVecReadGroupCachesOpenFailure(t *testing.T) {
	group := &appendVecReadGroup{}
	missing := filepath.Join(t.TempDir(), "missing")
	first, firstErr := group.open(missing)
	second, secondErr := group.open(missing)
	assert.Nil(t, first)
	assert.Nil(t, second)
	assert.True(t, errors.Is(firstErr, os.ErrNotExist))
	assert.Same(t, firstErr, secondErr)
}

func TestGetAccountsBatchStableDedupeScattersAndReportsPhysicalReads(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	a := foldAcct(0x11, 11, []byte("aaaaaaaa"))
	b := foldAcct(0x22, 22, []byte("bbbbbbbb"))
	c := foldAcct(0x33, 33, []byte("cccccccc"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{a, b, c}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	for _, acct := range []*accounts.Account{a, b, c} {
		db.CommonAcctsCache.Delete(acct.Key)
		db.VoteAcctCache.Delete(acct.Key)
	}

	missing := solana.PublicKey{0x99}
	request := []solana.PublicKey{b.Key, a.Key, b.Key, missing, c.Key, a.Key, b.Key, missing}
	original := append([]solana.PublicKey(nil), request...)
	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, request)
	require.NoError(t, err)
	assert.Equal(t, original, request)
	require.Len(t, out, len(request))
	assert.Equal(t, []uint64{22, 11, 22, 0, 33, 11, 22, 0}, []uint64{
		out[0].Lamports, out[1].Lamports, out[2].Lamports, out[3].Lamports,
		out[4].Lamports, out[5].Lamports, out[6].Lamports, out[7].Lamports,
	})
	assert.Same(t, out[0], out[2])
	assert.Same(t, out[0], out[6])
	assert.Same(t, out[1], out[5])
	assert.Same(t, out[3], out[7])

	assert.Equal(t, uint64(len(request)), stats.RequestedKeys)
	assert.Equal(t, uint64(len(request)), stats.DurableKeys)
	assert.Equal(t, uint64(4), stats.UniqueKeys)
	assert.Equal(t, uint64(4), stats.UniqueDurableKeys)
	assert.Equal(t, uint64(4), stats.DuplicateKeys)
	assert.Equal(t, uint64(6), stats.IndexHits, "legacy outcome counters remain logical")
	assert.Equal(t, uint64(2), stats.IndexMisses)
	assert.Equal(t, uint64(3), stats.AppendVecAccounts, "each exact account is decoded once")
	assert.Equal(t, uint64(3), stats.DecodedAccountObjects)
	assert.Equal(t, uint64(1), stats.UniqueAppendVecs)
	assert.Equal(t, uint64(1), stats.AppendVecChunks)
	assert.Equal(t, uint64(2), stats.AppendVecReadRanges)
	assert.Equal(t, stats.AppendVecReadRanges, stats.AppendVecPreadCalls)
	assert.Equal(t, uint64(3*(hdrLen+8)), stats.AppendVecRequestedBytes)
	assert.Equal(t, stats.AppendVecRequestedBytes, stats.AppendVecPhysicalReadBytes,
		"dense aligned eight-byte records need no range over-read")
}

func TestGetAccountsBatchCoalescesNearbyDataAfterSeparateHeaderReads(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	const dataLen = 8 << 10
	stored := make([]*accounts.Account, 3)
	keys := make([]solana.PublicKey, len(stored))
	for i := range stored {
		keys[i][0] = byte(0x40 + i)
		data := make([]byte, dataLen)
		for j := range data {
			data[j] = byte(i*31 + j)
		}
		stored[i] = &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{7}, Data: data}
	}
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: stored}}, 100, nil, nil,
	)
	require.NoError(t, err)
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
		db.VoteAcctCache.Delete(key)
	}

	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.Len(t, out, len(keys))
	for i := range out {
		assert.Equal(t, stored[i].Data, out[i].Data)
	}
	assert.Equal(t, uint64(4), stats.AppendVecPreadCalls,
		"three >4KiB-separated headers plus one bounded data range")
	assert.Equal(t, stats.AppendVecPreadCalls, stats.AppendVecReadRanges)
	assert.Equal(t, uint64(len(stored)*(hdrLen+dataLen)), stats.AppendVecRequestedBytes)
	assert.Greater(t, stats.AppendVecPhysicalReadBytes, stats.AppendVecRequestedBytes,
		"the coalesced data range deliberately over-reads the two intervening headers")
	assert.Less(t, stats.AppendVecPhysicalReadBytes, uint64(len(stored)*2*(hdrLen+dataLen)),
		"coalescing must still beat one header and data pread per account")
}

func TestGetAccountsBatchLargeAccountUsesDirectBoundedFallback(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	data := make([]byte, appendVecDataReadRangeMaxBytes+257)
	for i := range data {
		data[i] = byte(i * 31)
	}
	large := foldAcct(0x44, 44, data)
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{large}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(large.Key)

	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, []solana.PublicKey{large.Key})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, data, out[0].Data)
	assert.Equal(t, uint64(2), stats.AppendVecReadRanges, "one header range plus one direct data read")
	assert.Equal(t, uint64(2), stats.AppendVecPreadCalls)
	assert.Equal(t, uint64(hdrLen+len(data)), stats.AppendVecRequestedBytes)
	assert.Equal(t, stats.AppendVecRequestedBytes, stats.AppendVecPhysicalReadBytes)
}

func TestGetAccountsBatchRejectsOversizedAccountBeforeDataAllocation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	acct := foldAcct(0x54, 54, []byte("small-on-disk"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{acct}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(acct.Key)
	db.VoteAcctCache.Delete(acct.Key)
	entry, _, found, err := db.lookupExactAccountIndexEntry(acct.Key)
	require.NoError(t, err)
	require.True(t, found)

	path := filepath.Join(db.AcctsDir, fmt.Sprintf("%d.%d", entry.Slot, entry.FileId))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	var encodedLen [8]byte
	binary.LittleEndian.PutUint64(encodedLen[:], maxAppendVecAccountDataLen+1)
	_, err = file.WriteAt(encodedLen[:], int64(entry.Offset)+dataLenOffset)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	out, stats, err := db.GetAccountsBatchSharedWithStats(
		context.Background(), 100, []solana.PublicKey{acct.Key},
	)
	require.Error(t, err)
	assert.Nil(t, out)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "exceeds maximum")
	assert.Equal(t, uint64(1), stats.AppendVecPreadCalls)
	assert.Equal(t, uint64(hdrLen), stats.AppendVecRequestedBytes)
	assert.Equal(t, uint64(hdrLen), stats.AppendVecPhysicalReadBytes)
}

func TestGetAccountsBatchCancellationStopsBetweenCoalescedReads(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	acct := foldAcct(0x55, 55, []byte("data-after-header"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{acct}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(acct.Key)

	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	db.batchHooks.afterAppendVecPread = func() { once.Do(cancel) }
	out, stats, err := db.GetAccountsBatchSharedWithStats(ctx, 100, []solana.PublicKey{acct.Key})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, out)
	assert.Equal(t, uint64(1), stats.AppendVecPreadCalls)
	assert.Equal(t, uint64(1), stats.AppendVecReadRanges)
	assert.Equal(t, uint64(hdrLen), stats.AppendVecPhysicalReadBytes)
	assert.Zero(t, stats.ReadFailures, "caller cancellation is not an appendvec integrity failure")
}

func TestGetAccountsBatchCoalescedHeaderStillRejectsStaleDeltaPubkey(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	stored := foldAcct(0x66, 66, []byte("stored"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{stored}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(stored.Key)
	entry, _, found, err := db.lookupExactAccountIndexEntry(stored.Key)
	require.NoError(t, err)
	require.True(t, found)

	wrong := solana.PublicKey{0x67}
	require.NoError(t, db.Index.Apply(
		[]deltaIndexMutation{liveDeltaMutation(wrong, entry)}, nil, true,
	))
	_, stats, err := db.GetAccountsBatchSharedWithStats(
		context.Background(), 100, []solana.PublicKey{stored.Key, wrong},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stale index entry")
	assert.Positive(t, stats.AppendVecPreadCalls)
}
