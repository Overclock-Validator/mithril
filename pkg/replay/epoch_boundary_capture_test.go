package replay

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type capturedBoundaryDelegation struct {
	Pubkey     solana.PublicKey
	Delegation sealevel.Delegation
}

type capturedEpochBoundary struct {
	BoundarySlot           uint64                                  `json:"boundary_slot"`
	OutgoingEpoch          uint64                                  `json:"outgoing_epoch"`
	EnteringEpoch          uint64                                  `json:"entering_epoch"`
	LeaderScheduleEpoch    uint64                                  `json:"leader_schedule_epoch"`
	MinimumVoteBalance     uint64                                  `json:"minimum_vote_account_balance"`
	NewRateActivationEpoch uint64                                  `json:"new_rate_activation_epoch"`
	Delegations            []capturedBoundaryDelegation            `json:"delegations"`
	History                sealevel.SysvarStakeHistory             `json:"history"`
	EpochSchedule          sealevel.SysvarEpochSchedule            `json:"epoch_schedule"`
	VoteAccounts           map[string]*epochstakes.VoteAccountJSON `json:"vote_accounts"`
	ExpectedStakes         map[string]uint64                       `json:"expected_corrected_stakes"`
	ExpectedTotal          uint64                                  `json:"corrected_total"`
	ExpectedScheduleSHA256 string                                  `json:"expected_schedule_sha256"`
}

func TestCapturedAGEpoch35BoundaryMatchesClusterSchedule(t *testing.T) {
	file, err := os.Open("testdata/ag_epoch35_boundary.json.gz")
	require.NoError(t, err)
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer compressed.Close()
	var fixture capturedEpochBoundary
	require.NoError(t, json.NewDecoder(compressed).Decode(&fixture))
	require.Len(t, fixture.Delegations, 505)

	db := capturedEpochBoundaryDB(t, fixture.BoundarySlot, fixture.Delegations)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceStakeWarmupCooldown, fixture.EpochSchedule.FirstSlotInEpoch(fixture.NewRateActivationEpoch))
	got := scanStakesForEpochBoundary(db, fixture.BoundarySlot, fixture.OutgoingEpoch,
		fixture.EnteringEpoch, &fixture.History, &fixture.EpochSchedule, f)

	votes := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(fixture.VoteAccounts))
	metadata := make(map[solana.PublicKey]rebuiltVoteAccountMeta, len(fixture.VoteAccounts))
	scheduleVotes := make(map[solana.PublicKey]*epochstakes.VoteAccount, len(fixture.VoteAccounts))
	for key, vote := range fixture.VoteAccounts {
		pk := solana.MustPublicKeyFromBase58(key)
		node := solana.MustPublicKeyFromBase58(vote.NodePubkey)
		require.Len(t, vote.BlsPubkeyCompressed, 48)
		bls := [48]byte(vote.BlsPubkeyCompressed)
		votes[pk] = &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4,
			V4: sealevel.VoteState4{NodePubkey: node, BlsPubkeyCompressed: &bls}}
		metadata[pk] = rebuiltVoteAccountMeta{Lamports: vote.Lamports}
		scheduleVotes[pk] = &epochstakes.VoteAccount{NodePubkey: node, BlsPubkeyCompressed: &bls}
	}
	stakes, total := filterEpochStakesForVAT(got.EffectiveStakes, votes, metadata, fixture.MinimumVoteBalance)
	expected := make(map[solana.PublicKey]uint64, len(fixture.ExpectedStakes))
	for key, stake := range fixture.ExpectedStakes {
		expected[solana.MustPublicKeyFromBase58(key)] = stake
	}
	require.Equal(t, expected, stakes, "epoch35 must use stake activated for entering epoch34")
	require.Equal(t, fixture.ExpectedTotal, total)
	require.Len(t, stakes, 99)

	// This digest covers all 54,000 slots, independently checked against two
	// cluster RPC nodes. It detects rank/weight differences even when totals agree.
	schedule := leaderschedule.New(scheduleVotes, stakes, &fixture.EpochSchedule,
		fixture.LeaderScheduleEpoch, fixture.EpochSchedule.SlotsInEpoch(fixture.LeaderScheduleEpoch), 4)
	digest := sha256.New()
	firstSlot := fixture.EpochSchedule.FirstSlotInEpoch(fixture.LeaderScheduleEpoch)
	for offset := uint64(0); offset < fixture.EpochSchedule.SlotsInEpoch(fixture.LeaderScheduleEpoch); offset++ {
		leader, ok := schedule.LeaderForSlot(firstSlot + offset)
		require.True(t, ok)
		_, err := fmt.Fprintln(digest, leader.String())
		require.NoError(t, err)
	}
	require.Equal(t, fixture.ExpectedScheduleSHA256, hex.EncodeToString(digest.Sum(nil)))
}

func capturedEpochBoundaryDB(t *testing.T, slot uint64, delegations []capturedBoundaryDelegation) *accountsdb.AccountsDb {
	t.Helper()
	db := openAlpenglowTestAccountsDB(t)
	dir := filepath.Dir(db.AcctsDir)
	global.ClearPendingStakePubkeys()
	accts := make([]*accounts.Account, 0, len(delegations))
	for _, captured := range delegations {
		stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		stake.Stake.Stake.Delegation = captured.Delegation
		data, err := sealevel.MarshalStakeStake(stake)
		require.NoError(t, err)
		accts = append(accts, &accounts.Account{Key: captured.Pubkey, Owner: addresses.StakeProgramAddr,
			Lamports: captured.Delegation.StakeLamports, Data: data})
		global.EnqueuePendingStakePubkey(slot, captured.Pubkey)
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: accts}}, slot, nil, nil)
	require.NoError(t, err)
	_, err = global.FlushPendingStakePubkeys(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		global.EnqueuePendingStakePubkey(slot, accts[0].Key)
		_, err := global.FlushPendingStakePubkeys(dir)
		require.NoError(t, err)
		global.ClearPendingStakePubkeys()
	})
	return db
}
