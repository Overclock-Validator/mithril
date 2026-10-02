package rpcserver

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func recordTestAccount(t testing.TB, slot uint64) *accounts.Account {
	v := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	v.V4.NodePubkey = solana.PublicKey{2}
	v.V4.AuthorizedVoters.AuthorizedVoters.Set(0, v.V4.NodePubkey)
	v.V4.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: slot}})
	v.V4.EpochCredits = []sealevel.EpochCredits{{Epoch: 0, Credits: slot}}
	v.V4.RootSlot = &slot
	v.V4.InflationRewardsCommissionBps = 755
	return voteAccountForRPC(t, solana.PublicKey{1}, v)
}

func readTestVoteRecord(t *testing.T, s *RpcServer) *VoteAccountInfo {
	t.Helper()
	version, stable := s.acctsDb.CommittedAccountVersion()
	require.True(t, stable)
	bank := s.rootedBank.Load()
	records, err := s.rootedVoteRecords(t.Context(), bank.Slot, version, []solana.PublicKey{{1}})
	require.NoError(t, err)
	return records[0]
}

func TestVoteRecordCacheInvalidationAndRecovery(t *testing.T) {
	s := seedStakeCacheServer(t, recordTestAccount(t, 10))
	first := readTestVoteRecord(t, s)
	require.NotNil(t, first)
	require.Same(t, first, readTestVoteRecord(t, s))
	commitCacheAccounts(t, s, 11, &accounts.Account{Key: solana.PublicKey{9}, Lamports: 1, Owner: addresses.SystemProgramAddr})
	require.Same(t, first, readTestVoteRecord(t, s), "unrelated writes must preserve parsed records")
	oldVersion, _ := s.acctsDb.CommittedAccountVersion()
	commitCacheAccounts(t, s, 12, recordTestAccount(t, 12))
	require.False(t, s.storeVoteRecords(oldVersion, []solana.PublicKey{{1}}, []*VoteAccountInfo{first}), "an in-flight old decode cannot resurrect an invalidated record")
	updated := readTestVoteRecord(t, s)
	require.Equal(t, uint64(12), updated.LastVote)
	require.Equal(t, uint64(10), first.LastVote, "in-flight readers retain immutable old records")
	for _, account := range []*accounts.Account{
		{Key: solana.PublicKey{1}, Lamports: 1, Owner: addresses.SystemProgramAddr},
		{Key: solana.PublicKey{1}, Lamports: 0, Owner: addresses.VoteProgramAddr},
		{Key: solana.PublicKey{1}, Lamports: 1, Owner: addresses.VoteProgramAddr, Data: []byte{0}},
		voteAccountForRPC(t, solana.PublicKey{1}, &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}),
	} {
		slot := s.rootedBank.Load().Slot + 1
		commitCacheAccounts(t, s, slot, account)
		require.Nil(t, readTestVoteRecord(t, s))
		commitCacheAccounts(t, s, slot+1, recordTestAccount(t, slot+1))
		require.Equal(t, slot+1, readTestVoteRecord(t, s).LastVote)
	}
	_, err := s.acctsDb.RewindToBatchBoundary(10)
	require.NoError(t, err)
	s.SetRootedBankState(10, 10, 0)
	require.Empty(t, s.voteRecords.records)
	require.Equal(t, first, readTestVoteRecord(t, s))
	commitCacheAccounts(t, s, 11, recordTestAccount(t, 9)) // Same slot, different fork.
	require.Equal(t, uint64(9), readTestVoteRecord(t, s).LastVote)
	_, err = s.acctsDb.RecoverFoldState()
	require.NoError(t, err)
	require.Empty(t, s.voteRecords.records)
	require.Equal(t, uint64(9), readTestVoteRecord(t, s).LastVote)
	// Failed writes use the same observer reset contract as recovery.
	s.voteRecords.AccountsChanged(0, nil, true)
	require.Empty(t, s.voteRecords.records)
}

func TestVoteRecordCacheResponseOwnershipAndPublication(t *testing.T) {
	s := seedStakeCacheServer(t, recordTestAccount(t, 10), cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64))
	previous, present := global.EpochStakesSnapshot(0)
	global.PutEpochStakes(0, map[solana.PublicKey]uint64{{1}: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{}, 1)
	t.Cleanup(func() {
		if present {
			global.PutEpochStakes(0, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(0)
		}
	})
	params := mustRawParams(t, []interface{}{})
	first, err := s.GetVoteAccounts(t.Context(), params)
	require.NoError(t, err)
	require.Len(t, first.Current, 1)
	first.Current[0].EpochCredits[0][1] = 999
	first.Current[0].NodePubkey = "mutated"
	next, err := s.GetVoteAccounts(t.Context(), params)
	require.NoError(t, err)
	require.Equal(t, uint64(10), next.Current[0].EpochCredits[0][1])
	require.Equal(t, (solana.PublicKey{2}).String(), next.Current[0].NodePubkey)
	record := s.voteRecords.records[solana.PublicKey{1}]
	// Root-relative classification is not cached with the account record.
	_, err = s.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{map[string]interface{}{"delinquentSlotDistance": 0}}))
	require.NoError(t, err)
	_, err = s.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 90, Delta: []*accounts.Account{cacheStakeAccount(t, 21, 1, 200, math.MaxUint64, math.MaxUint64)}}}, 90, nil, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = s.GetVoteAccounts(ctx, params)
	require.ErrorIs(t, err, context.DeadlineExceeded, "cached records must not bypass rooted publication")
	s.SetRootedBankState(90, 90, 0)
	next, err = s.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{map[string]interface{}{"delinquentSlotDistance": 20}}))
	require.NoError(t, err)
	require.Empty(t, next.Current)
	require.Len(t, next.Delinquent, 1)
	require.Equal(t, uint64(200), next.Delinquent[0].ActivatedStake)
	require.Same(t, record, s.voteRecords.records[solana.PublicKey{1}])
}

func TestVoteRecordCacheLegacyAndUnknownKeys(t *testing.T) {
	s := seedStakeCacheServer(t, recordTestAccount(t, 10))
	readTestVoteRecord(t, s)
	version, _ := s.acctsDb.CommittedAccountVersion()
	keys := make([]solana.PublicKey, 100)
	for i := range keys {
		keys[i] = solana.PublicKey{0xee, byte(i)}
	}
	records, err := s.rootedVoteRecords(t.Context(), 10, version, keys)
	require.NoError(t, err)
	for _, record := range records {
		require.Nil(t, record)
	}
	require.Len(t, s.voteRecords.records, 1, "arbitrary nonexistent filters must not grow retained memory")
	s.acctsDb.RootedDurable = false
	done := make(chan struct{})
	require.NoError(t, s.acctsDb.StoreAccounts([]*accounts.Account{recordTestAccount(t, 11)}, 11, func() { close(done) }))
	<-done
	require.Eventually(t, func() bool { _, stable := s.acctsDb.CommittedAccountVersion(); return stable }, time.Second, time.Millisecond)
	s.SetRootedBankState(11, 11, 0)
	require.Equal(t, uint64(11), readTestVoteRecord(t, s).LastVote)
}

func invalidateVoteRecordsForBenchmark(s *RpcServer, changed []*accounts.Account) {
	s.voteRecords.AccountsChanged(0, changed, false)
}

func TestDecodeVoteRecordVersions(t *testing.T) {
	for version := uint32(0); version <= sealevel.VoteStateVersionV4; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			key, node := solana.PublicKey{1}, solana.PublicKey{2}
			root := uint64(9)
			credits := []sealevel.EpochCredits{{Epoch: 1, Credits: 12, PrevCredits: 3}}
			lockout := sealevel.VoteLockout{Slot: 10}
			v := &sealevel.VoteStateVersions{Type: version}
			switch version {
			case sealevel.VoteStateVersionV0_23_5:
				v.V0_23_5 = sealevel.VoteState0_23_5{NodePubkey: node, AuthorizedVoter: node, Commission: 8, RootSlot: &root, EpochCredits: credits}
				v.V0_23_5.Votes.PushBack(lockout)
			case sealevel.VoteStateVersionV1_14_11:
				v.V1_14_11 = sealevel.VoteState1_14_11{NodePubkey: node, Commission: 8, RootSlot: &root, EpochCredits: credits}
				v.V1_14_11.AuthorizedVoters.AuthorizedVoters.Set(0, node)
				v.V1_14_11.Votes.PushBack(lockout)
			case sealevel.VoteStateVersionCurrent:
				v.Current = sealevel.VoteState{NodePubkey: node, Commission: 8, RootSlot: &root, EpochCredits: credits}
				v.Current.AuthorizedVoters.AuthorizedVoters.Set(0, node)
				v.Current.Votes.PushBack(sealevel.LandedVote{Lockout: lockout})
			case sealevel.VoteStateVersionV4:
				v.V4.NodePubkey, v.V4.RootSlot, v.V4.EpochCredits = node, &root, credits
				v.V4.InflationRewardsCommissionBps = 800
				v.V4.AuthorizedVoters.AuthorizedVoters.Set(0, node)
				v.V4.Votes.PushBack(sealevel.LandedVote{Lockout: lockout})
			}
			account := voteAccountForRPC(t, key, v)
			got := decodeVoteRecord(key, account)
			clear(account.Data) // The cache must own everything it retains.
			require.Equal(t, &VoteAccountInfo{VotePubkey: key.String(), NodePubkey: node.String(), Commission: 8, InflationRewardsCommissionBPS: 800, RootSlot: 9, LastVote: 10, EpochCredits: [][3]uint64{{1, 12, 3}}}, got)
		})
	}
}
