package accountsdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetAccountWithDataLimitDisk(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	acct := foldAcct(1, 100, bytes.Repeat([]byte{7}, 1<<20))
	commitTestBatch(t, db, 100, acct)
	db.CommonAcctsCache.Delete(acct.Key)

	got, err := db.GetAccountWithDataLimit(100, acct.Key, 64)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, got)
	require.False(t, db.CommonAcctsCache.Has(acct.Key), "a rejected read must not populate the cache")
	got, err = db.GetAccountWithDataLimit(100, acct.Key, uint64(len(acct.Data)))
	require.NoError(t, err)
	require.Equal(t, acct.Data, got.Data)
	require.Equal(t, acct.Lamports, got.Lamports)
	ordinary, err := db.GetAccount(100, acct.Key)
	require.NoError(t, err)
	require.Equal(t, ordinary, got)

	got, err = db.GetAccountWithDataLimit(100, solana.PublicKey{99}, 0)
	require.ErrorIs(t, err, ErrNoAccount)
	require.Nil(t, got)
	var absent *AccountsDb
	_, err = absent.GetAccountWithDataLimit(100, acct.Key, 0)
	require.ErrorIs(t, err, ErrNoAccount)
}

func TestGetAccountWithDataLimitResidentPrecedence(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{1}
	large := foldAcct(1, 100, bytes.Repeat([]byte{7}, 1<<20))
	for _, vote := range []bool{false, true} {
		if vote {
			large.Owner = addresses.VoteProgramAddr
			require.True(t, db.VoteAcctCache.Set(key, large))
		} else {
			require.True(t, db.CommonAcctsCache.Set(key, large))
		}
		got, err := db.GetAccountWithDataLimit(100, key, 64)
		require.ErrorIs(t, err, ErrAccountDataLimit)
		require.Nil(t, got)
		db.CommonAcctsCache.Delete(key)
		db.VoteAcctCache.Delete(key)
	}
	older := foldAcct(1, 9, []byte{1})
	require.True(t, db.CommonAcctsCache.Set(key, older))
	db.pendingFold = map[[32]byte]dedupedVersion{key: {acct: large, ownerSlot: 100}}
	_, err := db.GetAccountWithDataLimit(100, key, 64)
	require.ErrorIs(t, err, ErrAccountDataLimit, "oversized pending state must not fall back to the old cache")

	// Newest in-progress state takes precedence over the pending fold and cache.
	db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{key: large}})
	db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{key: older}})
	got, err := db.GetAccountWithDataLimit(100, key, 1)
	require.NoError(t, err)
	require.Equal(t, older, got)
	got.Data[0] = 8
	require.Equal(t, byte(1), older.Data[0], "in-progress reads retain owned-copy behavior")
	db.inProgressStoreRequests.Remove(db.inProgressStoreRequests.Back())
	_, err = db.GetAccountWithDataLimit(100, key, 64)
	require.ErrorIs(t, err, ErrAccountDataLimit)

	// A zero-lamport entry remains authoritative, including at a zero-byte limit.
	tombstone := foldAcct(1, 0, nil)
	db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{key: tombstone}})
	got, err = db.GetAccountWithDataLimit(100, key, 0)
	require.NoError(t, err)
	require.Equal(t, tombstone.Clone(), got)
	db.inProgressStoreRequests.Init()
	db.pendingFold[key] = dedupedVersion{acct: tombstone, ownerSlot: 101}
	got, err = db.GetAccountWithDataLimit(101, key, 0)
	require.NoError(t, err)
	require.Equal(t, tombstone, got)
	db.pendingFold = nil
}

func TestGetAccountWithDataLimitCorruption(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{9}
	var entry [24]byte
	(&AccountIndexEntry{Slot: 100, FileId: 424242}).Marshal(&entry)
	require.NoError(t, db.Index.Set(key[:], entry[:], nil))
	_, err := db.GetAccountWithDataLimit(100, key, 64)
	require.ErrorContains(t, err, "after retry")
	require.NotErrorIs(t, err, ErrNoAccount)

	var header [hdrLen]byte
	copy(header[16:48], key[:])
	binary.LittleEndian.PutUint64(header[8:16], 4)
	path := filepath.Join(db.AcctsDir, "100.424242")
	require.NoError(t, os.WriteFile(path, append(header[:], 1), 0o600))
	_, err = db.GetAccountWithDataLimit(100, key, 64)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.ErrorContains(t, err, "after retry")
	require.False(t, db.CommonAcctsCache.Has(key))

	binary.LittleEndian.PutUint64(header[8:16], math.MaxUint64)
	require.NoError(t, os.WriteFile(path, header[:], 0o600))
	_, err = db.GetAccountWithDataLimit(100, key, 64)
	require.ErrorIs(t, err, ErrAccountDataLimit, "reject the length before allocating or reading a body")
	require.NotErrorIs(t, err, ErrNoAccount)

	result := commitTestBatch(t, db, 110, foldAcct(1, 100, []byte{1}))
	(&AccountIndexEntry{Slot: 110, FileId: result.FileId}).Marshal(&entry)
	require.NoError(t, db.Index.Set(key[:], entry[:], nil))
	_, err = db.GetAccountWithDataLimit(110, key, 64)
	require.ErrorContains(t, err, "stale index entry")
	require.ErrorContains(t, err, "after retry")
}

func TestAppendVecUnmarshalDataLimit(t *testing.T) {
	var header [hdrLen]byte
	binary.LittleEndian.PutUint64(header[8:16], 1000)
	var decoded AppendVecAccount
	err := decoded.unmarshal(bytes.NewReader(header[:]), 10)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, decoded.Data)

	binary.LittleEndian.PutUint64(header[8:16], 3)
	for _, limit := range []uint64{3, math.MaxUint64} {
		err = decoded.unmarshal(bytes.NewReader(append(header[:], 1)), limit)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		err = decoded.unmarshal(bytes.NewReader(header[:10]), limit)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		err = decoded.unmarshal(bytes.NewReader(append(header[:], 1, 2, 3)), limit)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, decoded.Data)
	}
}
