package accountsdb

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestHistoryMeasureXorRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 3000; i++ {
		n := rng.Intn(600)
		prev := &hmVer{lamports: rng.Uint64(), rentEpoch: 5, data: make([]byte, n)}
		rng.Read(prev.data[:n/2])
		cur := &hmVer{lamports: prev.lamports, owner: prev.owner, rentEpoch: prev.rentEpoch, data: bytes.Clone(prev.data)}
		switch rng.Intn(5) {
		case 0:
			cur.data = append(cur.data, byte(1+rng.Intn(9)), 0, 3)
		case 1:
			if len(cur.data) > 0 {
				cur.data = cur.data[:rng.Intn(len(cur.data))]
			}
		case 2:
			cur.lamports ^= 77
			cur.owner[3] = 9
			cur.exec = true
			cur.rentEpoch = 6
		}
		for j := rng.Intn(6); j > 0 && len(cur.data) > 0; j-- {
			cur.data[rng.Intn(len(cur.data))] ^= byte(1 + rng.Intn(255))
		}
		x := hmEncodeXor(nil, prev, cur)
		fwd, err := hmApplyXor(prev, x, true)
		require.NoError(t, err, "case %d", i)
		require.True(t, hmSame(fwd, cur), "forward case %d", i)
		back, err := hmApplyXor(cur, x, false)
		require.NoError(t, err, "case %d", i)
		require.True(t, hmSame(back, prev), "backward case %d", i)
	}
	created := &hmVer{lamports: 3, data: []byte{1, 2, 3}}
	x := hmEncodeXor(nil, &hmVer{absent: true}, created)
	v, err := hmApplyXor(nil, x, true)
	require.NoError(t, err)
	require.True(t, hmSame(v, created))
	v, err = hmApplyXor(created, x, false)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestHistoryMeasureXorSegments(t *testing.T) {
	db, _ := newFoldTestDb(t)
	pool := foldAcct(1, 10, make([]byte, 64))
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{pool}}}, 100, nil, nil)
	require.NoError(t, err)
	dir := t.TempDir()
	m, err := newHistoryMeasurer(dir)
	require.NoError(t, err)
	m.xor = newHMXorState()
	var deltas []accounts.SlotDelta
	for s := uint64(101); s <= 400; s++ {
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
	raw, err := os.ReadFile(filepath.Join(dir, "xor.json"))
	require.NoError(t, err)
	var got []HMXStats
	require.NoError(t, json.Unmarshal(raw, &got))
	for _, s := range got {
		require.Zero(t, s.RebuildBad, "p=%d depth=%d", s.P, s.Depth)
		require.Zero(t, s.ReverseBad, "p=%d depth=%d", s.P, s.Depth)
		if s.P == 128 && s.Depth == 0 {
			require.EqualValues(t, 2, s.Segments)
			require.EqualValues(t, 128, s.RunMax) // the pool changes every slot
			require.Zero(t, s.DepthCopies)
		}
		if s.P == 256 && s.Depth == 64 {
			require.EqualValues(t, 1, s.Segments)
			require.EqualValues(t, 64, s.RunMax)
			require.EqualValues(t, 3, s.DepthCopies) // 256 changes: one start copy, three more after 64 each
		}
	}
}
