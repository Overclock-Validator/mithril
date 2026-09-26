package rpcserver

import (
	"bytes"
	"errors"
	"math"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetVoteAccountsUsesRootedVoteState(t *testing.T) {
	const (
		rootedSlot = 500
		epoch      = 0
	)
	currentVote := solana.PublicKey{1}
	delinquentVote := solana.PublicKey{2}
	newVote := solana.PublicKey{3}
	invalidVote := solana.PublicKey{5}
	currentNode := solana.PublicKey{11}
	delinquentNode := solana.PublicKey{12}
	newNode := solana.PublicKey{13}
	currentRoot := uint64(498)
	delinquentRoot := uint64(299)

	currentState := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	currentState.V4.NodePubkey = currentNode
	currentState.V4.AuthorizedVoters.AuthorizedVoters.Set(0, currentNode)
	currentState.V4.InflationRewardsCommissionBps = 755
	currentState.V4.RootSlot = &currentRoot
	currentState.V4.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: 499}})
	currentState.V4.EpochCredits = []sealevel.EpochCredits{
		{Epoch: 0, Credits: 1},
		{Epoch: 1, Credits: 2, PrevCredits: 1},
		{Epoch: 2, Credits: 3, PrevCredits: 2},
		{Epoch: 3, Credits: 4, PrevCredits: 3},
		{Epoch: 4, Credits: 5, PrevCredits: 4},
		{Epoch: 5, Credits: 9, PrevCredits: 5},
	}

	delinquentState := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}
	delinquentState.Current.NodePubkey = delinquentNode
	delinquentState.Current.AuthorizedVoters.AuthorizedVoters.Set(0, delinquentNode)
	delinquentState.Current.Commission = 5
	delinquentState.Current.RootSlot = &delinquentRoot
	delinquentState.Current.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: 300}})

	newState := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}
	newState.Current.NodePubkey = newNode
	newState.Current.AuthorizedVoters.AuthorizedVoters.Set(0, newNode)
	newState.Current.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: 498}})
	uninitializedVote := solana.PublicKey{4}
	uninitializedState := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}

	db := newRPCAccountsDB(t)
	global.ClearPendingStakePubkeys()
	t.Cleanup(func() {
		global.EnqueuePendingStakePubkey(rootedSlot, solana.PublicKey{21})
		_, err := global.FlushPendingStakePubkeys(filepath.Join(db.AcctsDir, ".."))
		require.NoError(t, err)
		global.ClearPendingStakePubkeys()
	})
	stakeHistory := sealevel.SysvarStakeHistory{}
	var historyData bytes.Buffer
	require.NoError(t, stakeHistory.MarshalWithEncoder(bin.NewBinEncoder(&historyData)))
	stakeAccount := func(key, vote solana.PublicKey, lamports uint64) *accounts.Account {
		state := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		state.Stake.Stake.Delegation = sealevel.Delegation{
			VoterPubkey: vote, StakeLamports: lamports,
			ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
		}
		data, err := sealevel.MarshalStakeStake(state)
		require.NoError(t, err)
		global.EnqueuePendingStakePubkey(rootedSlot, key)
		return &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: lamports, Data: data}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: rootedSlot,
		Delta: []*accounts.Account{
			voteAccountForRPC(t, currentVote, &currentState),
			voteAccountForRPC(t, delinquentVote, &delinquentState),
			voteAccountForRPC(t, newVote, &newState),
			voteAccountForRPC(t, uninitializedVote, &uninitializedState),
			{Key: invalidVote, Owner: addresses.VoteProgramAddr, Lamports: 1},
			stakeAccount(solana.PublicKey{21}, currentVote, 125),
			stakeAccount(solana.PublicKey{22}, delinquentVote, 200),
			stakeAccount(solana.PublicKey{23}, uninitializedVote, 50),
			{Key: sealevel.SysvarStakeHistoryAddr, Owner: addresses.SysvarOwnerAddr, Lamports: 1, Data: historyData.Bytes()},
		},
	}}, rootedSlot, nil, nil)
	require.NoError(t, err)
	_, err = global.FlushPendingStakePubkeys(filepath.Join(db.AcctsDir, ".."))
	require.NoError(t, err)

	previous, hadPrevious := global.EpochStakesSnapshot(epoch)
	global.PutEpochStakes(epoch,
		map[solana.PublicKey]uint64{currentVote: 100, delinquentVote: 200},
		map[solana.PublicKey]*epochstakes.VoteAccount{
			currentVote:    {NodePubkey: currentNode},
			delinquentVote: {NodePubkey: delinquentNode},
		},
		300,
	)
	t.Cleanup(func() {
		if hadPrevious {
			global.PutEpochStakes(epoch, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(epoch)
		}
	})

	server := &RpcServer{
		acctsDb: db,
		epochSchedule: &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch: 1_000,
		},
	}
	server.SetRootedBankState(rootedSlot, 490, 1_000)
	got, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{
		map[string]interface{}{"commitment": "confirmed"},
	}))
	require.NoError(t, err)
	require.Len(t, got.Current, 2)
	require.Len(t, got.Delinquent, 1)
	require.Equal(t, currentVote.String(), got.Current[0].VotePubkey)
	require.Equal(t, currentNode.String(), got.Current[0].NodePubkey)
	require.Equal(t, uint64(125), got.Current[0].ActivatedStake, "live stake differs from frozen epoch stake")
	require.Equal(t, uint8(8), got.Current[0].Commission)
	require.Equal(t, uint16(755), got.Current[0].InflationRewardsCommissionBPS)
	require.Equal(t, uint64(499), got.Current[0].LastVote)
	require.Equal(t, [][3]uint64{{1, 2, 1}, {2, 3, 2}, {3, 4, 3}, {4, 5, 4}, {5, 9, 5}}, got.Current[0].EpochCredits)
	require.True(t, got.Current[0].EpochVoteAccount)
	require.Equal(t, newVote.String(), got.Current[1].VotePubkey)
	require.Equal(t, uint64(0), got.Current[1].ActivatedStake)
	require.False(t, got.Current[1].EpochVoteAccount)
	require.Equal(t, delinquentVote.String(), got.Delinquent[0].VotePubkey)
	require.Equal(t, uint8(5), got.Delinquent[0].Commission)

	filtered, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{
		map[string]interface{}{"votePubkey": currentVote.String()},
	}))
	require.NoError(t, err)
	require.Len(t, filtered.Current, 1)
	require.Empty(t, filtered.Delinquent)

	db.VoteAcctCache.Clear()
	cold, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{}))
	require.NoError(t, err)
	require.Len(t, cold.Current, 2)
	require.Equal(t, currentVote.String(), cold.Current[0].VotePubkey)
	require.Equal(t, newVote.String(), cold.Current[1].VotePubkey)
	require.Len(t, cold.Delinquent, 1)
	require.Equal(t, delinquentVote.String(), cold.Delinquent[0].VotePubkey)

	// Candidate keys are append-only, but an account that changes owner must
	// no longer be returned as a vote account.
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 501, Delta: []*accounts.Account{{
		Key: newVote, Lamports: 1, Owner: addresses.SystemProgramAddr,
	}}}}, 501, nil, nil)
	require.NoError(t, err)
	server.SetRootedBankState(501, 490, 1_000)
	changed, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{}))
	require.NoError(t, err)
	require.Len(t, changed.Current, 1)
	require.Equal(t, currentVote.String(), changed.Current[0].VotePubkey)

	activationSlot := uint64(0)
	featureData, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &activationSlot})
	require.NoError(t, err)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 502, Delta: []*accounts.Account{{
		Key: features.ReduceStakeWarmupCooldown.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: featureData,
	}}}}, 502, nil, nil)
	require.NoError(t, err)
	server.SetRootedBankState(502, 490, 1_000)
	withFeature, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{}))
	require.NoError(t, err)
	require.Equal(t, uint64(125), withFeature.Current[0].ActivatedStake)

	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 503, Delta: []*accounts.Account{{
		Key: features.ReduceStakeWarmupCooldown.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: []byte{1},
	}}}}, 503, nil, nil)
	require.NoError(t, err)
	server.SetRootedBankState(503, 490, 1_000)
	_, err = server.GetVoteAccounts(t.Context(), mustRawParams(t, []interface{}{}))
	require.ErrorContains(t, err, "decode rooted stake warmup feature")
}

func TestGetVoteAccountsRejectsInvalidConfig(t *testing.T) {
	server := &RpcServer{}
	for _, params := range [][]interface{}{
		{true},
		{map[string]interface{}{"commitment": "unknown"}},
		{map[string]interface{}{"votePubkey": "invalid"}},
		{map[string]interface{}{"keepUnstakedDelinquents": "yes"}},
		{map[string]interface{}{"keepUnstakedDelinquents": true}},
		{map[string]interface{}{"delinquentSlotDistance": 1.5}},
		{map[string]interface{}{}, map[string]interface{}{}},
	} {
		_, err := server.GetVoteAccounts(t.Context(), mustRawParams(t, params))
		var invalid *InvalidParamsError
		require.True(t, errors.As(err, &invalid), "params: %#v; error: %v", params, err)
	}
}

func voteAccountForRPC(t *testing.T, key solana.PublicKey, state *sealevel.VoteStateVersions) *accounts.Account {
	t.Helper()
	data, err := sealevel.MarshalVersionedVoteState(state)
	require.NoError(t, err)
	return &accounts.Account{
		Key:      key,
		Lamports: 1,
		Owner:    addresses.VoteProgramAddr,
		Data:     data,
	}
}
