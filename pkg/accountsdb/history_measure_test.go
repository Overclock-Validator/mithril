package accountsdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// hmApplyDiff rebuilds cur from prev and an hmEncodeDiff encoding.
func hmApplyDiff(t *testing.T, prev *hmVer, d []byte) *hmVer {
	t.Helper()
	flags := d[0]
	d = d[1:]
	if flags&hmFlagCreated != 0 {
		v := &hmVer{lamports: binary.LittleEndian.Uint64(d)}
		copy(v.owner[:], d[8:40])
		v.exec = d[40] == 1
		v.rentEpoch = binary.LittleEndian.Uint64(d[41:])
		n, k := binary.Uvarint(d[49:])
		v.data = bytes.Clone(d[49+k : 49+k+int(n)])
		return v
	}
	v := &hmVer{lamports: prev.lamports, owner: prev.owner, exec: prev.exec, rentEpoch: prev.rentEpoch, data: bytes.Clone(prev.data)}
	if flags&hmFlagLamports != 0 {
		v.lamports = binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&hmFlagOwner != 0 {
		copy(v.owner[:], d[:32])
		d = d[32:]
	}
	if flags&hmFlagExec != 0 {
		v.exec = !v.exec
	}
	if flags&hmFlagRent != 0 {
		v.rentEpoch = binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&hmFlagLength != 0 {
		n, k := binary.Uvarint(d)
		d = d[k:]
		if int(n) < len(v.data) {
			v.data = v.data[:n]
		} else {
			v.data = append(v.data, make([]byte, int(n)-len(v.data))...)
		}
	}
	ranges, k := binary.Uvarint(d)
	d = d[k:]
	pos := 0
	for r := uint64(0); r < ranges; r++ {
		gap, k := binary.Uvarint(d)
		d = d[k:]
		n, k2 := binary.Uvarint(d)
		d = d[k2:]
		pos += int(gap)
		copy(v.data[pos:], d[:n])
		d = d[n:]
		pos += int(n)
	}
	require.Empty(t, d)
	return v
}

func TestHistoryMeasureDiffRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		n := rng.Intn(600)
		prev := &hmVer{lamports: rng.Uint64(), rentEpoch: 5, data: make([]byte, n)}
		rng.Read(prev.data[:n/2])
		cur := &hmVer{lamports: prev.lamports, owner: prev.owner, rentEpoch: prev.rentEpoch, data: bytes.Clone(prev.data)}
		switch rng.Intn(5) {
		case 0:
			cur.data = append(cur.data, make([]byte, rng.Intn(40))...)
		case 1:
			if len(cur.data) > 0 {
				cur.data = cur.data[:rng.Intn(len(cur.data))]
			}
		case 2:
			cur.lamports++
			cur.owner[0] = 7
		}
		for j := rng.Intn(6); j > 0 && len(cur.data) > 0; j-- {
			cur.data[rng.Intn(len(cur.data))] ^= byte(1 + rng.Intn(255))
		}
		d, zr, _, _ := hmEncodeDiff(nil, prev, cur)
		require.Positive(t, zr)
		got := hmApplyDiff(t, prev, d)
		require.True(t, hmSame(got, cur), "case %d", i)
	}
	created := &hmVer{lamports: 3, data: []byte{1, 2, 3}}
	d, _, _, _ := hmEncodeDiff(nil, &hmVer{absent: true}, created)
	require.True(t, hmSame(hmApplyDiff(t, nil, d), created))
}

func TestHistoryMeasureZeroRuns(t *testing.T) {
	require.Equal(t, 0, hmZeroRunSize(nil))
	require.Equal(t, 1+3, hmZeroRunSize([]byte{1, 0, 0})) // short zero run stays literal
	require.Equal(t, 2, hmZeroRunSize(make([]byte, 100))) // one zero run of 100: a 2-byte length
	require.Equal(t, (1+1)+1+(1+1), hmZeroRunSize(append(append([]byte{9}, make([]byte, 10)...), 9)))
}

func TestHistoryMeasureWindows(t *testing.T) {
	db, _ := newFoldTestDb(t)
	pool := foldAcct(1, 10, make([]byte, 64))
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{pool}}}, 100, nil, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	m, err := newHistoryMeasurer(dir)
	require.NoError(t, err)
	// Slots 101..264: the pool changes one byte every slot; a new account
	// appears at slot 150.
	var deltas []accounts.SlotDelta
	for s := uint64(101); s <= 264; s++ {
		data := make([]byte, 64)
		data[0] = byte(s)
		delta := []*accounts.Account{{Key: solana.PublicKey{1}, Lamports: 10, Data: data}}
		if s == 150 {
			delta = append(delta, &accounts.Account{Key: solana.PublicKey{2}, Lamports: 5, Data: []byte{1, 2}})
		}
		deltas = append(deltas, accounts.SlotDelta{Slot: s, Delta: delta})
	}
	m.observe(db, deltas[:128])
	m.observe(db, deltas[128:])

	raw, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	require.NoError(t, err)
	var sum struct {
		Base     uint64 `json:"base_slot"`
		Spacings []hmSpacingSummary
	}
	require.NoError(t, json.Unmarshal(raw, &sum))
	require.EqualValues(t, 100, sum.Base)
	byN := map[uint64]hmSpacingSummary{}
	for _, s := range sum.Spacings {
		byN[s.N] = s
	}
	// Every slot through 263 is in a closed window at N=1: 163 pool versions plus one creation.
	require.EqualValues(t, 163, byN[1].SlotsCovered)
	require.EqualValues(t, 164, byN[1].Other.Versions)
	require.EqualValues(t, 1, byN[1].Other.Created)
	require.EqualValues(t, 163, byN[1].Other.ChangedBytes-2) // one byte per pool version, two for the new account
	// N=128: one closed window (101..228), one pool version and one creation.
	require.EqualValues(t, 128, byN[128].SlotsCovered)
	require.EqualValues(t, 2, byN[128].Other.Versions)
	require.NotNil(t, byN[128].Compress)
	require.Positive(t, byN[128].Compress.ZstdDiff)
	// N=1024 has not closed a window.
	require.EqualValues(t, 0, byN[1024].Other.Versions)
}
