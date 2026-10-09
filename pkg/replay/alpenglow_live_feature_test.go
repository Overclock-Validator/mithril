package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
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

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))

	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	featureActivationSlot := activationSlot
	featureData, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &featureActivationSlot})
	require.NoError(t, err)
	liveFeatureAddress := base58.MustDecodeFromString("A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS")
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts([]*accounts.Account{{
		Key:      liveFeatureAddress,
		Lamports: 1,
		Data:     featureData,
		Owner:    a.FeatureAddr,
	}}, activationSlot, func() { close(stored) }))
	<-stored

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
