package history

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An account opened and closed inside every fold has no full copy to repeat;
// its records say it was absent before, so a read stops at the newest one.
func TestReadStopsAtAbsentBefore(t *testing.T) {
	s, err := Open(t.TempDir(), Options{SegmentChunks: 64})
	require.NoError(t, err)
	defer s.Close()
	var tmp, kept [32]byte
	tmp[0], kept[0] = 1, 2
	absent := &Version{Absent: true}
	keptV := &Version{Lamports: 1, Data: []byte{1}}
	for c := uint64(0); c < 20; c++ {
		from := 100 + 10*c
		open := &Version{Lamports: 2039280 + c, Data: []byte{byte(c), 7}}
		next := &Version{Lamports: keptV.Lamports + 1, Data: []byte{byte(c + 2)}}
		keys := []KeyHistory{
			{Key: tmp, Prev: absent, Diffs: []Diff{
				{Slot: from + 2, Bytes: AppendXor(nil, absent, open)},
				{Slot: from + 5, Bytes: AppendXor(nil, open, absent)},
			}},
			{Key: kept, Prev: keptV, Diffs: []Diff{{Slot: from + 3, Bytes: AppendXor(nil, keptV, next)}}},
		}
		keptV = next
		require.NoError(t, s.AppendKeys(from, from+9, keys, nil))
	}
	lookups := func(k [32]byte, slot uint64) (*Version, int) {
		h0, m0, _ := s.ReadCacheStats()
		v, errs, _ := s.AccountsAt([][32]byte{k}, slot)
		require.NoError(t, errs[0])
		h1, m1, _ := s.ReadCacheStats()
		return v[0], int(h1 - h0 + m1 - m0)
	}
	v, n := lookups(tmp, 293)
	require.Equal(t, 1, n)
	require.Equal(t, uint64(2039280+19), v.Lamports)
	v, n = lookups(tmp, 296)
	require.Equal(t, 1, n)
	require.True(t, v.Absent)
	v, _ = lookups(tmp, 104)
	require.Equal(t, uint64(2039280), v.Lamports)
	// An account that exists throughout repeats its full copy every
	// fullEveryChunks chunks.
	v, n = lookups(kept, 299)
	require.LessOrEqual(t, n, fullEveryChunks)
	require.Equal(t, keptV, v)
}

func TestSkipRecordMatchesReadRecord(t *testing.T) {
	recs := []*Record{
		{Key: [32]byte{1}, Diffs: []Diff{{Slot: 10, Bytes: []byte{1, 2, 3}}}},
		{Key: [32]byte{2}, Full: &Version{Lamports: 5, Data: make([]byte, 300)}, Diffs: []Diff{{Slot: 11, Bytes: []byte{9, 9}}, {Slot: 12, Bytes: []byte{0, 0}}}},
		{Key: [32]byte{3}, AbsentBefore: true, Diffs: []Diff{{Slot: 10, Bytes: []byte{4, 4}}}},
	}
	var raw []byte
	for _, r := range recs {
		raw = appendRecord(raw, r, 10)
	}
	for _, want := range recs {
		got, rest, err := readRecord(raw, 10)
		require.NoError(t, err)
		require.Equal(t, want.Key, got.Key)
		require.Equal(t, want.AbsentBefore, got.AbsentBefore)
		skipped, err := skipRecord(raw)
		require.NoError(t, err)
		require.Equal(t, len(rest), len(skipped))
		raw = rest
	}
	require.Empty(t, raw)
	_, err := skipRecord([]byte{1, 2, 3})
	require.Error(t, err)
}
