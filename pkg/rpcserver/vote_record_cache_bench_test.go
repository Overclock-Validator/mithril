package rpcserver

import (
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Measures the warm RPC handler plus JSON encoding, not HTTP transport, startup,
// stake aggregation or commits. Vote accounts include lockouts and credit history
// so decoding costs are represented rather than using empty vote-state fixtures.
// Changed cases invalidate decoded records before each query (including that cost),
// simulating account updates with warm account buffers; they do not perform disk
// commits and must not be presented as end-to-end writer/RPC contention results.
func BenchmarkGetVoteAccountsRecords(b *testing.B) {
	db := newRPCAccountsDB(b)
	global.ClearPendingStakePubkeys()
	b.Cleanup(global.ClearPendingStakePubkeys)
	global.EnqueuePendingStakePubkey(500, solana.PublicKey{0xee})
	_, err := global.FlushPendingStakePubkeys(filepath.Dir(db.AcctsDir))
	require.NoError(b, err)
	delta := []*accounts.Account{cacheHistoryAccount(b, nil)}
	stakes := make(map[solana.PublicKey]uint64)
	for i := range 1000 {
		key := solana.PublicKey{0xb0}
		binary.LittleEndian.PutUint64(key[1:], uint64(i))
		v := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
		v.V4.NodePubkey = key
		v.V4.AuthorizedVoters.AuthorizedVoters.Set(0, key)
		for j := range 31 {
			v.V4.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: uint64(470 + j)}})
		}
		for j := range 64 {
			v.V4.EpochCredits = append(v.V4.EpochCredits, sealevel.EpochCredits{Epoch: uint64(j), Credits: uint64(j + 1), PrevCredits: uint64(j)})
		}
		delta = append(delta, voteAccountForRPC(b, key, v))
		stakes[key] = 1
	}
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 500, Delta: delta}}, 500, nil, nil)
	require.NoError(b, err)
	require.NoError(b, db.SeedVoteAccountPubkeys(nil))
	previous, present := global.EpochStakesSnapshot(0)
	global.PutEpochStakes(0, stakes, map[solana.PublicKey]*epochstakes.VoteAccount{}, 1000)
	b.Cleanup(func() {
		if present {
			global.PutEpochStakes(0, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(0)
		}
	})
	s := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1000}}
	s.SetRootedBankState(500, 500, 0)
	for _, tc := range []struct {
		name    string
		params  []interface{}
		count   int
		changed int
	}{
		{"All", []interface{}{}, 1000, 0},
		{"One", []interface{}{map[string]interface{}{"votePubkey": (solana.PublicKey{0xb0}).String()}}, 1, 0},
		{"All10PercentChanged", []interface{}{}, 1000, 100},
		{"AllChanged", []interface{}{}, 1000, 1000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw, err := json.Marshal(tc.params)
			require.NoError(b, err)
			params := jsonrpc.RawParams(raw)
			response, err := s.GetVoteAccounts(b.Context(), params)
			require.NoError(b, err)
			require.Len(b, response.Current, tc.count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if tc.changed > 0 {
					invalidateVoteRecordsForBenchmark(s, delta[1:1+tc.changed])
				}
				response, err := s.GetVoteAccounts(b.Context(), params)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := json.Marshal(response); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
