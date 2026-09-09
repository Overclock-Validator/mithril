package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestLiveAlpenglowFeatureAccountEnablesVoteRewardProcessing(t *testing.T) {
	const (
		activationSlot = uint64(486_000)
		replaySlot     = uint64(975_512)
	)

	db := openAlpenglowTestAccountsDB(t)

	featureActivationSlot := activationSlot
	featureData, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &featureActivationSlot})
	require.NoError(t, err)
	liveFeatureAddress := base58.MustDecodeFromString("A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS")
	_, err = db.CommitBatch([]accounts.SlotDelta{{
		Slot: activationSlot,
		Delta: []*accounts.Account{{
			Key:      liveFeatureAddress,
			Lamports: 1,
			Data:     featureData,
			Owner:    a.FeatureAddr,
		}},
	}}, activationSlot, nil, nil)
	require.NoError(t, err)

	active, _, _ := scanAndEnableFeatures(db, &ReplayCtx{}, replaySlot, false)
	require.True(t, active.IsActive(features.Alpenglow))
	gotActivationSlot, ok := active.ActivationSlot(features.Alpenglow)
	require.True(t, ok)
	require.Equal(t, activationSlot, gotActivationSlot)

	// A malformed, non-empty certificate is intentional: an inactive/stale
	// Alpenglow feature silently returns nil before decoding it, whereas the live
	// feature must enter the reward path and reject it.
	err = ApplyAlpenglowVoteRewards(
		&sealevel.SlotCtx{Features: active},
		&b.Block{Slot: replaySlot},
		&sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000},
		nil,
		nil,
		[]byte{0xff},
		45_234,
	)
	require.ErrorContains(t, err, "decode final cert")
}
