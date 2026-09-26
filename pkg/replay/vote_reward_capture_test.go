package replay

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

// Slot 2000727 stopped replay on the AG test cluster on 2026-09-10. Its 161
// captured shreds authenticate to the scheduled producer, but its skip reward
// certificate is invalid. Two cluster RPC nodes omit this slot from the finalized
// chain. Preserve that rejection before consensus admission, and retain the
// preceding valid notar reward certificate as a control using the same epoch.
// The fixture includes the original certificate bytes, persisted epoch stakes,
// spool hash, and the independent finalized-slot RPC observations.
func TestCapturedInvalidSkipRewardSlot2000727(t *testing.T) {
	f, err := os.Open("testdata/ag_invalid_skip_reward_slot2000727.json.gz")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	var capture struct {
		ShredVersion  uint16                  `json:"shred_version"`
		SlotsPerEpoch uint64                  `json:"slots_per_epoch"`
		EpochStakes   json.RawMessage         `json:"epoch_stakes"`
		InvalidBlock  capturedRewardCertBlock `json:"invalid_block"`
		ValidBlock    capturedRewardCertBlock `json:"valid_block"`
	}
	require.NoError(t, json.NewDecoder(gz).Decode(&capture))
	require.NoError(t, gz.Close())

	const epoch = uint64(37)
	previous, hadPrevious := global.EpochStakesSnapshot(epoch)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		if hadPrevious {
			global.PutEpochStakes(epoch, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		}
		clearVoteRewardVerifierCacheForTest()
	})
	loadedEpoch, err := global.DeserializeAndLoadEpochStakes(capture.EpochStakes)
	require.NoError(t, err)
	require.Equal(t, epoch, loadedEpoch)
	snapshot, ok := global.EpochStakesSnapshot(epoch)
	require.True(t, ok)
	set, err := alpenglow.BuildValidatorSet(epoch, snapshot.Stakes, snapshot.VoteAccounts, snapshot.TotalStake)
	require.NoError(t, err)
	require.Len(t, set.Validators, 99)

	schedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            capture.SlotsPerEpoch,
		LeaderScheduleSlotOffset: capture.SlotsPerEpoch,
	}
	invalidBlock := capture.InvalidBlock.block()
	validBlock := capture.ValidBlock.block()

	t.Run("original_signature_failure_is_preserved", func(t *testing.T) {
		_, err := rewardcerts.ValidateRewardCertificates(invalidBlock.Slot, invalidBlock.SkipRewardCert, invalidBlock.NotarRewardCert, set, capture.ShredVersion)
		require.ErrorContains(t, err, "skip certificate for slot 2000719 failed aggregate BLS signature verification")

		// The producer supplied a valid final certificate alongside the invalid
		// reward certificate; finality of that earlier block cannot admit this one.
		final, err := rewardcerts.ValidateBlockFinalCertificate(invalidBlock.BlockFinalCert, set, capture.ShredVersion)
		require.NoError(t, err)
		require.Equal(t, uint64(2000723), final.FinalSlot)
	})

	t.Run("reject_before_consensus", func(t *testing.T) {
		err := validatePreConsensusRewardCertificates(invalidBlock, schedule, capture.ShredVersion)
		var invalid *InvalidRewardCertificateError
		require.ErrorAs(t, err, &invalid)
		require.Equal(t, uint64(2000727), invalid.Slot)
		require.ErrorContains(t, err, "failed aggregate BLS signature verification")
	})

	t.Run("preceding_valid_notar_certificate_passes", func(t *testing.T) {
		_, err := rewardcerts.ValidateRewardCertificates(validBlock.Slot, validBlock.SkipRewardCert, validBlock.NotarRewardCert, set, capture.ShredVersion)
		require.NoError(t, err)
		require.NoError(t, validatePreConsensusRewardCertificates(validBlock, schedule, capture.ShredVersion))
	})
}

type capturedRewardCertBlock struct {
	Slot             uint64   `json:"slot"`
	ParentSlot       uint64   `json:"parent_slot"`
	BlockID          [32]byte `json:"block_id"`
	ParentBlockID    [32]byte `json:"parent_block_id"`
	ExpectedBankhash [32]byte `json:"expected_bankhash"`
	SkipRewardCert   []byte   `json:"skip_reward_cert"`
	NotarRewardCert  []byte   `json:"notar_reward_cert"`
	BlockFinalCert   []byte   `json:"block_final_cert"`
}

func (capture capturedRewardCertBlock) block() *block.Block {
	return &block.Block{
		Slot:                      capture.Slot,
		SourceParentSlot:          capture.ParentSlot,
		AlpenglowBlockID:          capture.BlockID,
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    capture.ParentBlockID,
		HasAlpenglowParentBlockID: true,
		ExpectedBankhash:          capture.ExpectedBankhash,
		HasExpectedBankhash:       true,
		HasAlpenglowFooter:        true,
		SkipRewardCert:            capture.SkipRewardCert,
		NotarRewardCert:           capture.NotarRewardCert,
		BlockFinalCert:            capture.BlockFinalCert,
	}
}
