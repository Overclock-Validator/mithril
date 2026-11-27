package history

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func randomVersion(rng *rand.Rand) *Version {
	v := &Version{Lamports: rng.Uint64(), RentEpoch: uint64(rng.Intn(3)), Data: make([]byte, rng.Intn(300))}
	v.Owner[0] = byte(rng.Intn(3))
	rng.Read(v.Data[:len(v.Data)/2]) // half zeros, like real accounts
	return v
}

// mutate returns a changed copy: small edits most of the time, sometimes a
// resize, an owner change, or deletion.
func mutate(rng *rand.Rand, v *Version) *Version {
	if v.Absent {
		return randomVersion(rng)
	}
	n := v.Clone()
	switch rng.Intn(10) {
	case 0:
		return &Version{Absent: true}
	case 1:
		n.Data = append(n.Data, make([]byte, rng.Intn(50))...)
	case 2:
		if len(n.Data) > 0 {
			n.Data = n.Data[:rng.Intn(len(n.Data))]
		}
	case 3:
		n.Owner[5] ^= 1
		n.Executable = !n.Executable
	}
	n.Lamports += uint64(rng.Intn(1000))
	for j := rng.Intn(4); j > 0 && len(n.Data) > 0; j-- {
		n.Data[rng.Intn(len(n.Data))] ^= byte(1 + rng.Intn(255))
	}
	return n
}

func TestXorRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		prev := randomVersion(rng)
		if rng.Intn(8) == 0 {
			prev = &Version{Absent: true}
		}
		cur := mutate(rng, prev)
		d := AppendXor(nil, prev, cur)
		f := prev.Clone()
		require.NoError(t, ApplyXor(f, d, true), "case %d", i)
		require.True(t, Same(f, cur), "forward case %d", i)
		b := cur.Clone()
		require.NoError(t, ApplyXor(b, d, false), "case %d", i)
		require.True(t, Same(b, prev), "backward case %d", i)
	}
}

func TestXorManyRanges(t *testing.T) {
	// More than 127 ranges needs a two-byte count.
	prev := &Version{Lamports: 1, Data: make([]byte, 5000)}
	cur := prev.Clone()
	for i := 0; i < 4000; i += 20 {
		cur.Data[i] = 7
	}
	d := AppendXor(nil, prev, cur)
	f := prev.Clone()
	require.NoError(t, ApplyXor(f, d, true))
	require.True(t, Same(f, cur))
}

// simulate drives a store with random writes and checks AccountAt against the
// true version at every slot, through segment rolls, pruning and reopening.
func TestStoreMatchesTruth(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	dir := t.TempDir()
	opts := Options{SegmentChunks: 3, KeepSegments: 4}
	s, err := Open(dir, opts)
	require.NoError(t, err)

	const keys = 60
	var keyList [][32]byte
	for i := 0; i < keys; i++ {
		var k [32]byte
		rng.Read(k[:])
		keyList = append(keyList, k)
	}
	// Two keys sharing an 8-byte prefix exercise the closed-index collision path.
	keyList[1] = keyList[0]
	keyList[1][31] ^= 0xff

	current := map[[32]byte]*Version{}
	for _, k := range keyList {
		if rng.Intn(4) > 0 {
			current[k] = randomVersion(rng)
		} else {
			current[k] = &Version{Absent: true}
		}
	}
	truth := map[uint64]map[[32]byte]*Version{} // version at end of slot
	slot := uint64(1000)
	truth[slot-1] = map[[32]byte]*Version{}
	for k, v := range current {
		truth[slot-1][k] = v
	}
	prevFn := func(keys [][32]byte) ([]*Version, error) {
		out := make([]*Version, len(keys))
		for i, k := range keys {
			out[i] = current[k]
		}
		return out, nil
	}

	for batch := 0; batch < 30; batch++ {
		width := uint64(1 + rng.Intn(6))
		from, to := slot, slot+width-1
		atStart := map[[32]byte]*Version{}
		for k, v := range current {
			atStart[k] = v
		}
		working := map[[32]byte]*Version{}
		for k, v := range current {
			working[k] = v
		}
		var slots []SlotWrites
		for sl := from; sl <= to; sl++ {
			if rng.Intn(5) == 0 {
				// skipped slot
				truth[sl] = copyMap(working)
				continue
			}
			sw := SlotWrites{Slot: sl}
			for _, k := range keyList {
				switch r := rng.Intn(10); {
				case r < 3:
					working[k] = mutate(rng, working[k])
					sw.Keys = append(sw.Keys, k)
					sw.Versions = append(sw.Versions, working[k])
				case r == 3:
					// rewritten unchanged
					sw.Keys = append(sw.Keys, k)
					sw.Versions = append(sw.Versions, working[k])
				}
			}
			slots = append(slots, sw)
			truth[sl] = copyMap(working)
		}
		// prev must be the state before the fold
		current = atStart
		require.NoError(t, s.Append(from, to, slots, prevFn))
		current = working
		slot = to + 1

		if batch == 17 {
			require.NoError(t, s.Close())
			s, err = Open(dir, opts)
			require.NoError(t, err)
		}
	}

	first, through, ok := s.Range()
	require.True(t, ok)
	require.Equal(t, slot-1, through)
	checked := 0
	for sl := first; sl <= through; sl++ {
		for _, k := range keyList {
			want := truth[sl][k]
			got, th, err := s.AccountAt(k, sl)
			require.Equal(t, through, th)
			if errors.Is(err, ErrUnchanged) {
				// The store says it never changed after sl: it must equal the latest value.
				require.True(t, Same(want, current[k]), "slot %d key %x: unchanged claim", sl, k[:2])
				continue
			}
			require.NoError(t, err, "slot %d", sl)
			require.True(t, Same(got, want), "slot %d key %x", sl, k[:2])
			checked++
		}
	}
	require.Positive(t, checked)
	_, _, err = s.AccountAt(keyList[2], first-1)
	require.ErrorIs(t, err, ErrBeforeHistory)
	_, _, err = s.AccountAt(keyList[2], through+1)
	require.ErrorIs(t, err, ErrAfterHistory)
	require.NoError(t, s.Close())
}

func copyMap(m map[[32]byte]*Version) map[[32]byte]*Version {
	c := make(map[[32]byte]*Version, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func TestAppendRejectsGap(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	require.NoError(t, err)
	none := func(keys [][32]byte) ([]*Version, error) { return make([]*Version, len(keys)), nil }
	require.NoError(t, s.Append(10, 20, nil, none))
	require.Error(t, s.Append(22, 30, nil, none))
	require.NoError(t, s.Append(21, 30, nil, none))
	_ = bytes.MinRead
}

func TestBankRecords(t *testing.T) {
	s, err := Open(t.TempDir(), Options{SegmentChunks: 2})
	require.NoError(t, err)
	none := func(keys [][32]byte) ([]*Version, error) { return make([]*Version, len(keys)), nil }
	_ = none
	appendFold := func(from, to uint64, slots ...uint64) {
		b := &BankData{}
		for _, sl := range slots {
			r := SlotBank{Slot: sl, BlockHeight: sl - 100, LamportsPerSignature: 5000, NumSignatures: sl % 7}
			r.Blockhash[0], r.Bankhash[0] = byte(sl), byte(sl+1)
			b.Slots = append(b.Slots, r)
		}
		top := &ChunkBank{Slot: slots[len(slots)-1], AccountsLtHash: make([]byte, 2048), RecentBlockhashes: []BlockhashFee{{LamportsPerSignature: 5000}}}
		top.AccountsLtHash[0] = byte(to)
		b.Top = top
		require.NoError(t, s.AppendKeys(from, to, nil, b))
	}
	appendFold(201, 210, 201, 203, 210)
	appendFold(211, 220, 212, 219)
	appendFold(221, 230, 225)

	r, err := s.BankAt(203)
	require.NoError(t, err)
	require.EqualValues(t, 201, r.Parent)
	require.EqualValues(t, 103, r.BlockHeight)
	require.EqualValues(t, 203, r.Blockhash[0])
	r, err = s.BankAt(212)
	require.NoError(t, err)
	require.EqualValues(t, 210, r.Parent, "parent crosses chunks")
	_, err = s.BankAt(202)
	require.ErrorIs(t, err, ErrNoBank, "skipped slot")

	top, err := s.ChunkBankAtOrBefore(218)
	require.NoError(t, err)
	require.EqualValues(t, 210, top.Slot, "chunk 211..220 ends at 219, after 218")
	top, err = s.ChunkBankAtOrBefore(219)
	require.NoError(t, err)
	require.EqualValues(t, 219, top.Slot)
	require.EqualValues(t, 220, top.AccountsLtHash[0])

	// Truncation removes bank records with their chunks; parents continue.
	require.NoError(t, s.TruncateFrom(221))
	_, err = s.BankAt(225)
	require.ErrorIs(t, err, ErrNoBank)
	appendFold(221, 230, 226)
	r, err = s.BankAt(226)
	require.NoError(t, err)
	require.EqualValues(t, 219, r.Parent)

	// Reopen keeps them.
	require.NoError(t, s.Close())
}

func TestAccountsAtMatchesAccountAt(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	s, err := Open(t.TempDir(), Options{SegmentChunks: 2})
	require.NoError(t, err)
	keys := make([][32]byte, 40)
	for i := range keys {
		rng.Read(keys[i][:])
	}
	cur := map[[32]byte]*Version{}
	prev := func(ks [][32]byte) ([]*Version, error) {
		out := make([]*Version, len(ks))
		for i, k := range ks {
			out[i] = cur[k]
		}
		return out, nil
	}
	slot := uint64(10)
	for f := 0; f < 6; f++ {
		var sw []SlotWrites
		next := map[[32]byte]*Version{}
		for k, v := range cur {
			next[k] = v
		}
		for s2 := slot; s2 < slot+3; s2++ {
			w := SlotWrites{Slot: s2}
			for _, k := range keys[:rng.Intn(len(keys))] {
				v := randomVersion(rng)
				w.Keys = append(w.Keys, k)
				w.Versions = append(w.Versions, v)
				next[k] = v
			}
			sw = append(sw, w)
		}
		require.NoError(t, s.Append(slot, slot+2, sw, prev))
		cur = next
		slot += 3
	}
	for sl := uint64(10); sl < slot; sl++ {
		vs, errs, _ := s.AccountsAt(keys, sl)
		for i, k := range keys {
			v, _, err := s.AccountAt(k, sl)
			require.Equal(t, err, errs[i])
			if err == nil {
				require.True(t, Same(v, vs[i]))
			}
		}
	}
}
