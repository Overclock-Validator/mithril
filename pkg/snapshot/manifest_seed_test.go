package snapshot

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestPopulateManifestSeedKeepsManifestEpochFrame(t *testing.T) {
	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{
			Slot:  463538376,
			Epoch: 1073,
			EpochSchedule: sealevel.SysvarEpochSchedule{
				SlotsPerEpoch:            432000,
				LeaderScheduleSlotOffset: 432000,
			},
		},
		VersionedEpochStakes: []VersionedEpochStakesPair{
			{
				Epoch: 1073,
				Val: VersionedEpochStakes{
					TotalStake: 42,
					Stakes:     Stake{},
				},
			},
		},
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 1073, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)

	require.Equal(t, uint64(432000), mithrilState.ManifestEpochSchedule.SlotsPerEpoch)
	require.Equal(t, uint64(432000), mithrilState.ManifestEpochSchedule.LeaderScheduleSlotOffset)

	data, exists := mithrilState.ManifestEpochStakes[1073]
	if !exists {
		t.Fatalf("expected manifest epoch stakes for epoch 1073, got keys %#v", mithrilState.ManifestEpochStakes)
	}
	var persisted epochstakes.PersistedEpochStakes
	if err := json.Unmarshal([]byte(data), &persisted); err != nil {
		t.Fatalf("failed to decode persisted epoch stakes: %v", err)
	}
	if persisted.Epoch != 1073 {
		t.Fatalf("persisted epoch = %d, want 1073", persisted.Epoch)
	}
}

func TestPopulateManifestSeedStoresBlockhashQueueLastHash(t *testing.T) {
	lastHash := [32]byte{7}
	newestRecent := [32]byte{8}
	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{
			Slot: 99,
			BlockhashQueue: BlockHashVec{
				LastHash: &lastHash,
				HashAndAge: []HashAgePair{{
					Key: newestRecent,
					Val: HashAge{HashIndex: 10},
				}},
			},
		},
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 0, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)

	require.Equal(t, base58.Encode(lastHash[:]), mithrilState.ManifestLastBlockhash)
	require.NotEqual(t, mithrilState.ManifestRecentBlockhashes[0].Blockhash, mithrilState.ManifestLastBlockhash)
}

func TestPopulateManifestSeedUsesSnapshotEpochForAuthorizedVoters(t *testing.T) {
	var voteAcct solana.PublicKey
	var authorizedVoter solana.PublicKey
	voteAcct[0] = 1
	authorizedVoter[0] = 2

	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{
			Slot:  2509799,
			Epoch: 0,
			EpochSchedule: sealevel.SysvarEpochSchedule{
				SlotsPerEpoch:            54000,
				LeaderScheduleSlotOffset: 54000,
			},
		},
		VersionedEpochStakes: []VersionedEpochStakesPair{
			{
				Epoch: 46,
				Val: VersionedEpochStakes{
					Stakes: Stake{},
					EpochAuthorizedVoters: []PubkeyPair{
						{Key: voteAcct, Val: authorizedVoter},
					},
				},
			},
		},
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 46, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)

	voteAcctStr := base58.Encode(voteAcct[:])
	require.Equal(t, []string{base58.Encode(authorizedVoter[:])}, mithrilState.ManifestEpochAuthorizedVoters[voteAcctStr])
}

func TestPopulateManifestSeedPrefersBankEpochAuthorizedVoters(t *testing.T) {
	var bankEpochVoteAcct solana.PublicKey
	var bankEpochAuthorizedVoter solana.PublicKey
	var scheduleEpochVoteAcct solana.PublicKey
	var scheduleEpochAuthorizedVoter solana.PublicKey
	bankEpochVoteAcct[0] = 3
	bankEpochAuthorizedVoter[0] = 4
	scheduleEpochVoteAcct[0] = 5
	scheduleEpochAuthorizedVoter[0] = 6

	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{
			Slot:  2509799,
			Epoch: 45,
			EpochSchedule: sealevel.SysvarEpochSchedule{
				SlotsPerEpoch:            54000,
				LeaderScheduleSlotOffset: 54000,
			},
		},
		VersionedEpochStakes: []VersionedEpochStakesPair{
			{
				Epoch: 45,
				Val: VersionedEpochStakes{
					Stakes: Stake{},
					EpochAuthorizedVoters: []PubkeyPair{
						{Key: bankEpochVoteAcct, Val: bankEpochAuthorizedVoter},
					},
				},
			},
			{
				Epoch: 46,
				Val: VersionedEpochStakes{
					Stakes: Stake{},
					EpochAuthorizedVoters: []PubkeyPair{
						{Key: scheduleEpochVoteAcct, Val: scheduleEpochAuthorizedVoter},
					},
				},
			},
		},
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 46, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)

	bankEpochVoteAcctStr := base58.Encode(bankEpochVoteAcct[:])
	scheduleEpochVoteAcctStr := base58.Encode(scheduleEpochVoteAcct[:])
	require.Equal(t, []string{base58.Encode(bankEpochAuthorizedVoter[:])}, mithrilState.ManifestEpochAuthorizedVoters[bankEpochVoteAcctStr])
	require.NotContains(t, mithrilState.ManifestEpochAuthorizedVoters, scheduleEpochVoteAcctStr)
}
