package history

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testBase(slot uint64) *BaseBank {
	b := &BaseBank{
		Slot:  SlotBank{Slot: slot, BlockHeight: slot - 50, LamportsPerSignature: 5000, NumSignatures: 9},
		Chunk: ChunkBank{Slot: slot, AccountsLtHash: make([]byte, 2048), RecentBlockhashes: []BlockhashFee{{LamportsPerSignature: 5000}}},
	}
	b.Slot.Blockhash[0], b.Slot.Bankhash[0] = byte(slot), byte(slot+1)
	b.Chunk.AccountsLtHash[0] = byte(slot)
	b.Chunk.RecentBlockhashes[0].Blockhash[0] = byte(slot)
	b.Chunk.EvictedBlockhash[0] = 0xee
	return b
}

func testBankData(base *BaseBank, slots ...uint64) *BankData {
	d := &BankData{Base: base}
	for _, sl := range slots {
		r := SlotBank{Slot: sl, BlockHeight: sl - 50, LamportsPerSignature: 5000}
		r.Blockhash[0] = byte(sl)
		d.Slots = append(d.Slots, r)
	}
	last := slots[len(slots)-1]
	d.Top = &ChunkBank{Slot: last, AccountsLtHash: make([]byte, 2048), RecentBlockhashes: []BlockhashFee{{LamportsPerSignature: 5000}}}
	d.Top.AccountsLtHash[0] = byte(last)
	return d
}

func TestBaseBank(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentChunks: 2, KeepSegments: 100})
	require.NoError(t, err)

	// k1 existed before history and changes at 103; k2 is created at 105; k3
	// never changes.
	var k1, k2, k3 [32]byte
	k1[0], k2[0], k3[0] = 1, 2, 3
	v0 := &Version{Lamports: 7, Data: []byte{1, 2, 3}}
	v1 := &Version{Lamports: 8, Data: []byte{1, 2, 4}}
	v2 := &Version{Lamports: 9, Data: []byte{5}}
	absent := &Version{Absent: true}
	keys := []KeyHistory{
		{Key: k1, Prev: v0, Diffs: []Diff{{Slot: 103, Bytes: AppendXor(nil, v0, v1)}}},
		{Key: k2, Prev: absent, Diffs: []Diff{{Slot: 105, Bytes: AppendXor(nil, absent, v2)}}},
	}
	// A base record must come before the fold.
	require.Error(t, s.AppendKeys(101, 110, keys, testBankData(testBase(101), 101, 103, 110)))
	require.NoError(t, s.AppendKeys(101, 110, keys, testBankData(testBase(100), 101, 103, 105, 110)))

	check := func(s *Store) {
		t.Helper()
		r, err := s.BankAt(100)
		require.NoError(t, err)
		require.Equal(t, testBase(100).Slot, r)
		r, err = s.BankAt(101)
		require.NoError(t, err)
		require.EqualValues(t, 100, r.Parent, "the first block's parent is the base")
		_, err = s.BankAt(99)
		require.ErrorIs(t, err, ErrNoBank)

		c, err := s.ChunkBankAtOrBefore(100)
		require.NoError(t, err)
		require.Equal(t, testBase(100).Chunk, *c)
		c, err = s.ChunkBankAtOrBefore(109)
		require.NoError(t, err)
		require.EqualValues(t, 100, c.Slot, "inside the first chunk, the base")
		c, err = s.ChunkBankAtOrBefore(110)
		require.NoError(t, err)
		require.EqualValues(t, 110, c.Slot)
		_, err = s.ChunkBankAtOrBefore(99)
		require.ErrorIs(t, err, ErrNoBank)

		// Accounts as of the base block come from the first records' full copies.
		vs, errs, _ := s.AccountsAt([][32]byte{k1, k2, k3}, 100)
		require.NoError(t, errs[0])
		require.True(t, Same(v0, vs[0]))
		require.NoError(t, errs[1])
		require.True(t, vs[1].Absent)
		require.ErrorIs(t, errs[2], ErrUnchanged)
		vs, errs, _ = s.AccountsAt([][32]byte{k1}, 103)
		require.NoError(t, errs[0])
		require.True(t, Same(v1, vs[0]))
		_, errs, _ = s.AccountsAt([][32]byte{k1}, 99)
		require.ErrorIs(t, errs[0], ErrBeforeHistory)
	}
	check(s)

	// The next chunk of the segment does not keep one.
	require.NoError(t, s.AppendKeys(111, 120, nil, testBankData(testBase(110), 112, 120)))
	d, err := readBankFile(filepath.Join(s.segments[0].dir, bankFileName(111, 120)))
	require.NoError(t, err)
	require.Nil(t, d.Base)
	// A new segment's first chunk does.
	require.NoError(t, s.AppendKeys(121, 130, nil, testBankData(testBase(120), 125)))
	require.Len(t, s.segments, 2)
	require.NotNil(t, s.segments[1].base)
	r, err := s.BankAt(125)
	require.NoError(t, err)
	require.EqualValues(t, 120, r.Parent)
	check(s)

	// Reopened, the same.
	require.NoError(t, s.Close())
	s, err = Open(dir, Options{SegmentChunks: 2, KeepSegments: 100})
	require.NoError(t, err)
	check(s)
	require.NotNil(t, s.segments[1].base)

	// Pruning the first segment leaves the second's base as the oldest.
	require.NoError(t, s.Close())
	s, err = Open(dir, Options{SegmentChunks: 2, KeepSegments: 1})
	require.NoError(t, err)
	require.NoError(t, s.AppendKeys(131, 140, nil, testBankData(nil, 135)))
	require.NoError(t, s.AppendKeys(141, 150, nil, testBankData(testBase(140), 145)))
	_, err = s.BankAt(100)
	require.ErrorIs(t, err, ErrNoBank)
	r, err = s.BankAt(120)
	require.NoError(t, err)
	require.Equal(t, testBase(120).Slot, r)
	c, err := s.ChunkBankAtOrBefore(124)
	require.NoError(t, err)
	require.EqualValues(t, 120, c.Slot)
	_, errs, _ := s.AccountsAt([][32]byte{k1}, 120)
	require.ErrorIs(t, errs[0], ErrUnchanged)
	_, errs, _ = s.AccountsAt([][32]byte{k1}, 119)
	require.ErrorIs(t, errs[0], ErrBeforeHistory)
	require.NoError(t, s.Close())
}

func TestBankFileBase(t *testing.T) {
	// Without a base, the file is what older versions wrote.
	plain := testBankData(nil, 5, 7)
	d, err := decodeBank(encodeBank(plain))
	require.NoError(t, err)
	require.Equal(t, plain, d)
	noTop := &BankData{Slots: plain.Slots}
	d, err = decodeBank(encodeBank(noTop))
	require.NoError(t, err)
	require.Nil(t, d.Top)
	require.Nil(t, d.Base)

	for _, b := range []*BankData{testBankData(testBase(4), 5, 7), {Slots: plain.Slots, Base: testBase(4)}} {
		d, err = decodeBank(encodeBank(b))
		require.NoError(t, err)
		require.Equal(t, b, d)
	}
	enc := encodeBank(testBankData(testBase(4), 5, 7))
	_, err = decodeBank(enc[:len(enc)-40])
	require.Error(t, err)
}
