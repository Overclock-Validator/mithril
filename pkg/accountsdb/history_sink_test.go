package accountsdb

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Folds feed history; every account read back at every slot matches what was
// folded, including after a rewind re-records slots.
func TestFoldsRecordHistory(t *testing.T) {
	db, _ := newFoldTestDb(t)
	rng := rand.New(rand.NewSource(5))
	keys := make([]solana.PublicKey, 20)
	for i := range keys {
		rng.Read(keys[i][:])
	}
	// Seed state at slot 100, before history is attached.
	var seed []*accounts.Account
	for i, k := range keys[:15] {
		seed = append(seed, &accounts.Account{Key: k, Lamports: uint64(100 + i), Data: []byte{byte(i), 1, 2, 3}})
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: seed}}, 100, nil, nil)
	require.NoError(t, err)

	hs, err := history.Open(t.TempDir(), history.Options{SegmentChunks: 2, KeepSegments: 10})
	require.NoError(t, err)
	db.SetHistory(hs)

	truth := map[uint64]map[solana.PublicKey]*accounts.Account{}
	live := map[solana.PublicKey]*accounts.Account{}
	for _, a := range seed {
		live[a.Key] = a
	}
	snapshot := func(slot uint64) {
		m := make(map[solana.PublicKey]*accounts.Account, len(live))
		for k, v := range live {
			m[k] = v
		}
		truth[slot] = m
	}
	snapshot(100)
	slot := uint64(101)
	for batch := 0; batch < 8; batch++ {
		var deltas []accounts.SlotDelta
		for i := 0; i < 4; i++ {
			sd := accounts.SlotDelta{Slot: slot}
			for _, k := range keys {
				if rng.Intn(3) != 0 {
					continue
				}
				a := &accounts.Account{Key: k, Lamports: uint64(rng.Intn(5)), Data: make([]byte, 4+rng.Intn(4))}
				rng.Read(a.Data)
				sd.Delta = append(sd.Delta, a)
				live[k] = a
			}
			deltas = append(deltas, sd)
			snapshot(slot)
			slot++
		}
		_, err := db.CommitBatch(deltas, slot-1, nil, nil)
		require.NoError(t, err)
	}
	require.EqualValues(t, 8, db.HistoryStats().Chunks)

	check := func(through uint64) {
		first, last, ok := hs.Range()
		require.True(t, ok)
		require.Equal(t, through, last)
		for s := first; s <= last; s++ {
			for _, k := range keys {
				want := HistoryVersion(truth[s][k])
				got, _, err := hs.AccountAt([32]byte(k), s)
				if errors.Is(err, history.ErrUnchanged) {
					require.True(t, history.Same(want, HistoryVersion(live[k])), "slot %d unchanged claim", s)
					continue
				}
				require.NoError(t, err)
				require.True(t, history.Same(got, want), "slot %d key %s", s, k)
			}
		}
	}
	check(slot - 1)
}

// With working-set compaction, folds commit the same store state and record
// the same history as without it.
func TestCompactedFoldsRecordExactHistory(t *testing.T) {
	db, _ := newFoldTestDb(t)
	hs, err := history.Open(t.TempDir(), history.Options{SegmentChunks: 3})
	require.NoError(t, err)
	db.SetHistory(hs)
	rng := rand.New(rand.NewSource(9))
	keys := make([]solana.PublicKey, 30)
	for i := range keys {
		rng.Read(keys[i][:])
	}
	ws := accounts.NewWorkingSet()
	truth := map[uint64]map[solana.PublicKey]*accounts.Account{}
	live := map[solana.PublicKey]*accounts.Account{}
	slot := uint64(101)
	for round := 0; round < 6; round++ {
		for i := 0; i < 10; i++ {
			var delta []*accounts.Account
			for _, k := range keys {
				if rng.Intn(2) != 0 {
					continue
				}
				a := &accounts.Account{Key: k, Lamports: uint64(rng.Intn(5)), Data: make([]byte, 40)}
				if prev := live[k]; prev != nil {
					copy(a.Data, prev.Data)
				}
				a.Data[rng.Intn(40)] = byte(rng.Intn(256))
				delta = append(delta, a)
				live[k] = a
			}
			ws.Add(slot, delta)
			m := map[solana.PublicKey]*accounts.Account{}
			for k, v := range live {
				m[k] = v
			}
			truth[slot] = m
			slot++
		}
		ws.CompactThrough(slot - 2) // everything but the newest slot is final
		chunk := ws.PromotionChunk(slot-3, 8, true)
		through := chunk[len(chunk)-1].Slot
		_, err := db.CommitBatch(chunk, through, nil, nil)
		require.NoError(t, err)
		ws.PromotePrefix(through)
	}
	first, last, ok := hs.Range()
	require.True(t, ok)
	for s := first; s <= last; s++ {
		for _, k := range keys {
			want := HistoryVersion(truth[s][k])
			got, _, err := hs.AccountAt([32]byte(k), s)
			if errors.Is(err, history.ErrUnchanged) {
				continue
			}
			require.NoError(t, err)
			require.True(t, history.Same(got, want), "slot %d", s)
		}
	}
	// The durable store holds each key's version as of the last fold.
	for _, k := range keys {
		accts, err := db.GetAccountsBatch(t.Context(), last, []solana.PublicKey{k})
		require.NoError(t, err)
		require.True(t, history.Same(HistoryVersion(accts[0]), HistoryVersion(truth[last][k])), "durable %s", k)
	}
}

// After the store rewinds to an earlier fold, the next folds re-record those
// slots: history drops the overlap and matches the new chain.
func TestHistoryFollowsRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	k := solana.PublicKey{7}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{{Key: k, Lamports: 1}}}}, 100, nil, nil)
	require.NoError(t, err)
	hs, err := history.Open(t.TempDir(), history.Options{SegmentChunks: 2})
	require.NoError(t, err)
	db.SetHistory(hs)
	fold := func(slot, lamports uint64) {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: k, Lamports: lamports}}}}, slot, nil, nil)
		require.NoError(t, err)
	}
	fold(110, 2)
	fold(120, 3)
	fold(130, 4)
	_, err = db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	fold(125, 30) // a different chain after 110
	_, through, ok := hs.Range()
	require.True(t, ok)
	require.EqualValues(t, 125, through)
	for _, tc := range []struct{ slot, want uint64 }{{105, 1}, {110, 2}, {124, 2}, {125, 30}} {
		v, _, err := hs.AccountAt([32]byte(k), tc.slot)
		require.NoError(t, err, "slot %d", tc.slot)
		require.EqualValues(t, tc.want, v.Lamports, "slot %d", tc.slot)
	}
	require.EqualValues(t, 1, db.HistoryStats().Truncations)
}

// Reads run while folds append history (run with -race).
func TestHistoryReadsDuringFolds(t *testing.T) {
	db, _ := newFoldTestDb(t)
	hs, err := history.Open(t.TempDir(), history.Options{SegmentChunks: 3})
	require.NoError(t, err)
	db.SetHistory(hs)
	keys := []solana.PublicKey{{1}, {2}, {3}}
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 1, Delta: []*accounts.Account{{Key: keys[0], Lamports: 1}}}}, 1, nil, nil)
	require.NoError(t, err)
	done := make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		defer close(readErr)
		for {
			select {
			case <-done:
				return
			default:
			}
			first, last, ok := hs.Range()
			if !ok {
				continue
			}
			for s := first; s <= last; s++ {
				for _, k := range keys {
					v, _, err := hs.AccountAt([32]byte(k), s)
					if err != nil && !errors.Is(err, history.ErrUnchanged) && !errors.Is(err, history.ErrBeforeHistory) && !errors.Is(err, history.ErrAfterHistory) {
						readErr <- err
						return
					}
					if err == nil && !v.Absent && v.Lamports != s && v.Lamports > s {
						readErr <- errors.New("read a version from the future")
						return
					}
				}
			}
		}
	}()
	for slot := uint64(2); slot < 200; slot++ {
		var delta []*accounts.Account
		for _, k := range keys {
			delta = append(delta, &accounts.Account{Key: k, Lamports: slot})
		}
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: delta}}, slot, nil, nil)
		require.NoError(t, err)
	}
	close(done)
	require.NoError(t, <-readErr)
}

// The fold job's staged bank context lands with the batch's history chunk.
func TestStagedBankReachesHistory(t *testing.T) {
	db, _ := newFoldTestDb(t)
	hs, err := history.Open(t.TempDir(), history.Options{})
	require.NoError(t, err)
	db.SetHistory(hs)
	require.True(t, db.HistoryEnabled())
	k := solana.PublicKey{4}
	bank := &history.BankData{Slots: []history.SlotBank{{Slot: 5, BlockHeight: 3, LamportsPerSignature: 5000}}, Top: &history.ChunkBank{Slot: 5}}
	db.StageHistoryBank(bank)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 5, Delta: []*accounts.Account{{Key: k, Lamports: 9}}}}, 5, nil, nil)
	require.NoError(t, err)
	r, err := hs.BankAt(5)
	require.NoError(t, err)
	require.EqualValues(t, 3, r.BlockHeight)
	// A later fold without a staged bank records none for its slots.
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 6, Delta: []*accounts.Account{{Key: k, Lamports: 10}}}}, 6, nil, nil)
	require.NoError(t, err)
	_, err = hs.BankAt(6)
	require.ErrorIs(t, err, history.ErrNoBank)
}
