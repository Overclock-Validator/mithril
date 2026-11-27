package rpcdb

import (
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func sig(b byte) solana.Signature {
	var s solana.Signature
	s[0], s[63] = b, b
	return s
}

func openTest(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, solana.Hash{1})
	require.NoError(t, err)
	return s
}

func TestSignaturesForAddressNewestFirstWithBounds(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	wallet, other := solana.PublicKey{1}, solana.PublicKey{2}
	b := s.NewBatch()
	for i, at := range []struct {
		slot uint64
		pos  uint32
	}{{100, 0}, {100, 5}, {101, 2}, {105, 1}, {105, 3}} {
		addrs := []solana.PublicKey{wallet}
		if i == 2 {
			addrs = append(addrs, other)
		}
		require.NoError(t, b.PutTransaction(&TxRecord{Signature: sig(byte(i + 1)), Slot: at.slot, Position: at.pos, Addresses: addrs, Wire: []byte{byte(i)}}))
	}
	require.NoError(t, b.SetWatermark(105))
	require.NoError(t, b.Commit())

	order := func(entries []SignatureEntry) []byte {
		var out []byte
		for _, e := range entries {
			out = append(out, e.Signature[0])
		}
		return out
	}
	all, err := s.SignaturesForAddress(wallet, nil, nil, 100)
	require.NoError(t, err)
	require.Equal(t, []byte{5, 4, 3, 2, 1}, order(all))
	require.EqualValues(t, 105, all[0].Slot)
	require.EqualValues(t, 3, all[0].Position)

	limited, err := s.SignaturesForAddress(wallet, nil, nil, 2)
	require.NoError(t, err)
	require.Equal(t, []byte{5, 4}, order(limited))

	before := sig(4)
	page, err := s.SignaturesForAddress(wallet, &before, nil, 100)
	require.NoError(t, err)
	require.Equal(t, []byte{3, 2, 1}, order(page))

	until := sig(2)
	bounded, err := s.SignaturesForAddress(wallet, &before, &until, 100)
	require.NoError(t, err)
	require.Equal(t, []byte{3}, order(bounded))

	others, err := s.SignaturesForAddress(other, nil, nil, 100)
	require.NoError(t, err)
	require.Equal(t, []byte{3}, order(others))

	missing := sig(99)
	_, err = s.SignaturesForAddress(wallet, &missing, nil, 10)
	require.Error(t, err)
}

func TestTransactionRecordRoundTripAndIdempotentRewrite(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	rec := &TxRecord{
		Signature: sig(7), Slot: 42, Position: 9, BlockTime: 1_700_000_000, BlockID: solana.Hash{4},
		Flags: Failed | Partial, Addresses: []solana.PublicKey{{1}, {2}}, Wire: []byte("wire"), Meta: []byte("meta"),
	}
	for i := 0; i < 2; i++ { // a replay after a crash writes the same slot again
		b := s.NewBatch()
		require.NoError(t, b.PutTransaction(rec))
		require.NoError(t, b.Commit())
	}
	got, ok, err := s.Transaction(sig(7))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, got)
	entries, err := s.SignaturesForAddress(solana.PublicKey{1}, nil, nil, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, Failed|Partial, entries[0].Flags)

	_, ok, err = s.Transaction(sig(8))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestTokenAccountsWatchesAndReopen(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	owner := solana.PublicKey{1}
	a := TokenAccount{Owner: owner, Program: 0, Mint: solana.PublicKey{5}, Account: solana.PublicKey{6}}
	c := TokenAccount{Owner: owner, Program: 1, Mint: solana.PublicKey{5}, Account: solana.PublicKey{7}}
	foreign := TokenAccount{Owner: solana.PublicKey{2}, Mint: solana.PublicKey{5}, Account: solana.PublicKey{8}}
	b := s.NewBatch()
	require.NoError(t, b.PutTokenAccount(a))
	require.NoError(t, b.PutTokenAccount(c))
	require.NoError(t, b.PutTokenAccount(foreign))
	require.NoError(t, b.PutWatch(0, owner, 1, 300))
	require.NoError(t, b.SetWatermark(300))
	require.NoError(t, b.Commit())
	require.NoError(t, s.Close())

	s = openTest(t, dir)
	defer s.Close()
	got, err := s.TokenAccounts(owner)
	require.NoError(t, err)
	require.Equal(t, []TokenAccount{a, c}, got)
	watches, err := s.Watches()
	require.NoError(t, err)
	require.Equal(t, []Watch{{Tenant: 0, Addr: owner, Kind: 1, Since: 300}}, watches)
	wm, err := s.Watermark()
	require.NoError(t, err)
	require.EqualValues(t, 300, wm)

	b = s.NewBatch()
	require.NoError(t, b.DeleteTokenAccount(a))
	require.NoError(t, b.Commit())
	got, err = s.TokenAccounts(owner)
	require.NoError(t, err)
	require.Equal(t, []TokenAccount{c}, got)
}

func TestOpenRefusesAnotherGenesis(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, openTest(t, dir).Close())
	_, err := Open(dir, solana.Hash{2})
	require.True(t, errors.Is(err, ErrGenesisMismatch), "got %v", err)
}
