package replay

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type rebuildTestReader struct {
	mu       sync.Mutex
	accounts map[solana.PublicKey]*accounts.Account
	batchErr bool
	gets     []solana.PublicKey
	batches  [][]solana.PublicKey
	slots    []uint64
}

func (r *rebuildTestReader) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gets = append(r.gets, key)
	r.slots = append(r.slots, slot)
	if acct := r.accounts[key]; acct != nil {
		return acct, nil
	}
	return nil, errors.New("missing account")
}

func (r *rebuildTestReader) GetAccountsBatch(_ context.Context, slot uint64, keys []solana.PublicKey) ([]*accounts.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, append([]solana.PublicKey(nil), keys...))
	r.slots = append(r.slots, slot)
	if r.batchErr {
		return nil, errors.New("one appendvec could not be read")
	}
	out := make([]*accounts.Account, len(keys))
	for i, key := range keys {
		out[i] = r.accounts[key]
		if out[i] == nil || out[i].Lamports == 0 {
			out[i] = &accounts.Account{Key: key}
		}
	}
	return out, nil
}

func rebuildTestStake(t *testing.T, key, voter solana.PublicKey, amount uint64) *accounts.Account {
	t.Helper()
	s := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
	s.Stake.Stake.Delegation = sealevel.Delegation{VoterPubkey: voter, StakeLamports: amount}
	data, err := sealevel.MarshalStakeStake(s)
	require.NoError(t, err)
	return &accounts.Account{Key: key, Lamports: 1, Data: data}
}

func TestStakeRebuildPreservesIndividualReadResults(t *testing.T) {
	voter := solana.PublicKey{9}
	for _, failBatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "fallback"}[failBatch], func(t *testing.T) {
			r := &rebuildTestReader{accounts: make(map[solana.PublicKey]*accounts.Account), batchErr: failBatch}
			var entries []accountsdb.StakeIndexEntry
			for i := 1; i <= 7; i++ {
				key := solana.PublicKey{byte(i)}
				entries = append(entries, accountsdb.StakeIndexEntry{Pubkey: key})
				r.accounts[key] = rebuildTestStake(t, key, voter, uint64(i))
			}
			// Missing, malformed, initialized but not delegated, and zero-balance
			// records exercise the old reader's exact inclusion rules.
			delete(r.accounts, solana.PublicKey{2})
			r.accounts[solana.PublicKey{3}].Data = []byte{255}
			data, err := sealevel.MarshalStakeStake(&sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusInitialized})
			require.NoError(t, err)
			r.accounts[solana.PublicKey{4}].Data = data
			r.accounts[solana.PublicKey{5}].Lamports = 0
			entries = append(entries, entries[0]) // Preserve duplicate-key behavior.
			want := map[solana.PublicKey]uint64{voter: 1 + 5 + 6 + 7 + 1}
			require.Equal(t, want, stakesOf(loadStakeAccountTotals(r, 100, entries)))
			if !failBatch {
				require.Equal(t, []solana.PublicKey{{2}, {5}}, r.gets)
			} else {
				require.Len(t, r.gets, len(entries))
			}
			for _, slot := range r.slots {
				require.Equal(t, uint64(100), slot)
			}
			// Rebuild after a rewind must reread the current account version,
			// even with the same snapshot-era index entries.
			r.accounts[solana.PublicKey{6}] = rebuildTestStake(t, solana.PublicKey{6}, solana.PublicKey{8}, 20)
			require.Equal(t, map[solana.PublicKey]uint64{voter: 14, {8}: 20}, stakesOf(loadStakeAccountTotals(r, 90, entries)))
		})
	}
}

func TestStakeRebuildBatchesAreBoundedAndKeepFileLocality(t *testing.T) {
	r := &rebuildTestReader{accounts: make(map[solana.PublicKey]*accounts.Account)}
	var entries []accountsdb.StakeIndexEntry
	for i := 0; i < 4005; i++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint32(key[:], uint32(i+1))
		fileID := uint64(1)
		if i >= 4001 {
			fileID = 2
		}
		entries = append(entries, accountsdb.StakeIndexEntry{Pubkey: key, FileId: fileID})
		r.accounts[key] = rebuildTestStake(t, key, solana.PublicKey{9}, 1)
	}
	require.Equal(t, map[solana.PublicKey]uint64{{9}: 4005}, stakesOf(loadStakeAccountTotals(r, 100, entries)))
	require.Len(t, r.batches, 4)
	sort.Slice(r.batches, func(i, j int) bool {
		return binary.LittleEndian.Uint32(r.batches[i][0][:]) < binary.LittleEndian.Uint32(r.batches[j][0][:])
	})
	for i, size := range []int{2000, 2000, 1, 4} {
		require.Len(t, r.batches[i], size)
	}
	require.Empty(t, r.gets)
	r.batches = nil
	require.Empty(t, stakesOf(loadStakeAccountTotals(r, 100, nil)))
	require.Empty(t, r.batches)
}

func TestStakeRebuildReadsStoredAccounts(t *testing.T) {
	voter := solana.PublicKey{9}
	db := epochBoundaryStakeDB(t, 100, []sealevel.Delegation{{VoterPubkey: voter, StakeLamports: 700}, {VoterPubkey: solana.PublicKey{8}, StakeLamports: 300}})
	entries := []accountsdb.StakeIndexEntry{{Pubkey: solana.PublicKey{0xa0, 1}, FileId: 999}, {Pubkey: solana.PublicKey{0xa0, 2}, FileId: 999}}
	// The historical file/offset hint must not replace the current DB index.
	for _, entry := range entries {
		db.CommonAcctsCache.Delete(entry.Pubkey)
	}
	require.Equal(t, map[solana.PublicKey]uint64{voter: 700, {8}: 300}, stakesOf(loadStakeAccountTotals(db, 100, entries)))
}

// Both ranges must overlap, while each range has only one batch outstanding.
type concurrentRebuildReader struct {
	*rebuildTestReader
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
}

func (r *concurrentRebuildReader) GetAccountsBatch(ctx context.Context, slot uint64, keys []solana.PublicKey) ([]*accounts.Account, error) {
	n := r.active.Add(1)
	defer r.active.Add(-1)
	for p := r.peak.Load(); n > p && !r.peak.CompareAndSwap(p, n); p = r.peak.Load() {
	}
	r.started <- struct{}{}
	<-r.release
	return r.rebuildTestReader.GetAccountsBatch(ctx, slot, keys)
}
func TestStakeRebuildOverlapsTwoBoundedRanges(t *testing.T) {
	for _, singleFile := range []bool{false, true} {
		for _, fallback := range []bool{false, true} {
			name := map[bool]string{false: "separate-files", true: "one-large-file"}[singleFile] + map[bool]string{false: "-batch", true: "-fallback"}[fallback]
			t.Run(name, func(t *testing.T) {
				base := &rebuildTestReader{accounts: make(map[solana.PublicKey]*accounts.Account), batchErr: fallback}
				r := &concurrentRebuildReader{rebuildTestReader: base, started: make(chan struct{}, 16), release: make(chan struct{})}
				var entries []accountsdb.StakeIndexEntry
				var want uint64
				for i := 0; i < 6002; i++ {
					var key solana.PublicKey
					binary.LittleEndian.PutUint32(key[:], uint32(i+1))
					file := uint64(1)
					if !singleFile && i >= 3001 {
						file = 2
					}
					entries = append(entries, accountsdb.StakeIndexEntry{Pubkey: key, FileId: file})
					base.accounts[key] = rebuildTestStake(t, key, solana.PublicKey{9}, uint64(i+1))
					want += uint64(i + 1)
				}
				done := make(chan map[solana.PublicKey]uint64, 1)
				go func() { done <- stakesOf(loadStakeAccountTotals(r, 100, entries)) }()
				for i := 0; i < 2; i++ {
					select {
					case <-r.started:
					case <-time.After(5 * time.Second):
						close(r.release)
						<-done
						t.Fatal("two ranges did not overlap")
					}
				}
				close(r.release)
				var got map[solana.PublicKey]uint64
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("rebuild did not finish")
				}
				require.Equal(t, map[solana.PublicKey]uint64{{9}: want}, got)
				require.Equal(t, int32(2), r.peak.Load())
				for _, batch := range base.batches {
					require.LessOrEqual(t, len(batch), stakeRebuildBatchSize)
					require.NotEmpty(t, batch)
				}
			})
		}
	}
}

func stakesOf(totals map[solana.PublicKey]voteDelegation) map[solana.PublicKey]uint64 {
	out := make(map[solana.PublicKey]uint64, len(totals))
	for voter, d := range totals {
		out[voter] = d.stake
	}
	return out
}
