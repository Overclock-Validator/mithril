package accountsdb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexEntriesDoesNotReadPastSliceLen(t *testing.T) {
	realKey := appendVecTestPubkey(1)
	phantomKey := appendVecTestPubkey(2)
	real := marshalAppendVecTestAccount(t, realKey, 11, []byte("real"))
	phantom := marshalAppendVecTestAccount(t, phantomKey, 22, []byte("outside-valid-slice"))

	backing := make([]byte, len(real)+len(phantom))
	copy(backing, real)
	copy(backing[len(real):], phantom)
	data := backing[:len(real)] // len excludes phantom; cap and manifest include it.

	pubkeys, entries, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(cap(data)), 7, 9)
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{realKey}, pubkeys)
	require.Len(t, entries, 1)
	assert.Equal(t, AccountIndexEntry{Slot: 7, FileId: 9, Offset: 0}, entries[0])
}

func TestBuildIndexEntriesBoundsOversizedManifestByDataLen(t *testing.T) {
	key := appendVecTestPubkey(3)
	encoded := marshalAppendVecTestAccount(t, key, 33, []byte("account"))
	data := make([]byte, len(encoded)) // Force cap(data) == len(data).
	copy(data, encoded)

	var pubkeys []solana.PublicKey
	require.NotPanics(t, func() {
		var err error
		pubkeys, _, _, err = BuildIndexEntriesFromAppendVecs(data, uint64(len(data)+hdrLen), 8, 10)
		require.NoError(t, err)
	})
	require.Equal(t, []solana.PublicKey{key}, pubkeys)
}

func TestBuildIndexEntriesStopsAtDefaultZeroLamportTerminator(t *testing.T) {
	firstKey := appendVecTestPubkey(4)
	afterTerminatorKey := appendVecTestPubkey(5)
	first := marshalAppendVecTestAccount(t, firstKey, 44, []byte("first"))
	afterTerminator := marshalAppendVecTestAccount(t, afterTerminatorKey, 55, []byte("must-not-index"))
	terminator := make([]byte, hdrLen)
	// A bogus data length confirms the terminator is recognized before account
	// data bounds are validated.
	binary.LittleEndian.PutUint64(terminator[dataLenOffset:dataLenOffset+8], ^uint64(0))
	data := append(append(append([]byte{}, first...), terminator...), afterTerminator...)

	pubkeys, entries, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(data)), 12, 13)
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{firstKey}, pubkeys)
	require.Len(t, entries, 1)
}

func TestBuildIndexEntriesTerminatorRequiresDefaultKeyAndZeroLamports(t *testing.T) {
	defaultKey := solana.PublicKey{}
	zeroLamportKey := appendVecTestPubkey(8)
	defaultKeyAccount := marshalAppendVecTestAccount(t, defaultKey, 1, []byte("funded-default-key"))
	zeroLamportAccount := marshalAppendVecTestAccount(t, zeroLamportKey, 0, []byte("tombstone"))
	data := append(defaultKeyAccount, zeroLamportAccount...)

	pubkeys, entries, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(data)), 18, 19)
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{defaultKey, zeroLamportKey}, pubkeys)
	require.Len(t, entries, 2)
}

func TestBuildIndexEntriesRejectsTruncatedAccountData(t *testing.T) {
	key := appendVecTestPubkey(6)
	data := make([]byte, hdrLen+3)
	binary.LittleEndian.PutUint64(data[dataLenOffset:dataLenOffset+8], 4)
	copy(data[pubkeyOffset:pubkeyOffset+32], key[:])
	binary.LittleEndian.PutUint64(data[lamportsOffset:lamportsOffset+8], 1)

	pubkeys, entries, stakeEntries, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(data)), 14, 15)
	require.ErrorContains(t, err, "truncated appendvec account data")
	assert.Nil(t, pubkeys)
	assert.Nil(t, entries)
	assert.Nil(t, stakeEntries)
}

func TestBuildIndexEntriesAcceptsFinalAccountWithoutAlignmentPadding(t *testing.T) {
	key := appendVecTestPubkey(7)
	encoded := marshalAppendVecTestAccount(t, key, 77, []byte{1, 2, 3})
	data := encoded[: hdrLen+3 : hdrLen+3]

	pubkeys, entries, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(encoded)), 16, 17)
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{key}, pubkeys)
	require.Len(t, entries, 1)
}

func appendVecTestPubkey(firstByte byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = firstByte
	return key
}

func marshalAppendVecTestAccount(t *testing.T, key solana.PublicKey, lamports uint64, data []byte) []byte {
	t.Helper()
	account := AppendVecAccount{
		DataLen:  uint64(len(data)),
		Pubkey:   key,
		Lamports: lamports,
		Data:     data,
	}
	var out bytes.Buffer
	require.NoError(t, account.Marshal(&out))
	return out.Bytes()
}
