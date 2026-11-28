package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowVATBurnChangesAtEpochAfterSlotTimeActivation(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Alpenglow, 1)
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}

	require.Equal(t, legacyVATBurnPerEpoch, alpenglowVATBurnPerEpoch(f, schedule, 99))
	require.Equal(t, vatBurn200ms, alpenglowVATBurnPerEpoch(f, schedule, 100))
}

func TestFilterEpochStakesForVATRejectsUnderfundedAndMissingBLS(t *testing.T) {
	const minimumBalance = uint64(900)
	var funded, underfunded, missingBLS solana.PublicKey
	funded[0], underfunded[0], missingBLS[0] = 1, 2, 3
	bls := [48]byte{1}
	voteCache := map[solana.PublicKey]*sealevel.VoteStateVersions{
		funded:      {Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{BlsPubkeyCompressed: &bls}},
		underfunded: {Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{BlsPubkeyCompressed: &bls}},
		missingBLS:  {Type: sealevel.VoteStateVersionV4},
	}
	metadata := map[solana.PublicKey]rebuiltVoteAccountMeta{
		funded:      {Lamports: minimumBalance},
		underfunded: {Lamports: minimumBalance - 1},
		missingBLS:  {Lamports: minimumBalance},
	}
	stakes := map[solana.PublicKey]uint64{funded: 100, underfunded: 200, missingBLS: 300}

	filtered, total := filterEpochStakesForVAT(stakes, voteCache, metadata, minimumBalance)
	require.Equal(t, map[solana.PublicKey]uint64{funded: 100}, filtered)
	require.Equal(t, uint64(100), total)
}

// The boundary VAT burns from each admitted vote account and credits the
// incinerator. Every returned parent keeps its pre-burn balance and shares no
// data with its update or with the stored account, so later epoch work that
// mutates the updates cannot reach the parent records.
func TestApplyAlpenglowBoundaryVATOwnership(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	voteA, voteB := solana.PublicKey{0x51}, solana.PublicKey{0x52}
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{
		{Key: voteA, Lamports: 1000, Data: []byte("vote a"), Owner: a.VoteProgramAddr},
		{Key: voteB, Lamports: 500, Data: []byte("vote b"), Owner: a.VoteProgramAddr},
	}}}, 10, nil, nil)
	require.NoError(t, err)

	updated, parents, err := applyAlpenglowBoundaryVAT(db, 10, 100, map[solana.PublicKey]uint64{voteA: 1, voteB: 1})
	require.NoError(t, err)
	require.Len(t, updated, 3)
	require.Len(t, parents, 3)
	for i, want := range []struct {
		key             solana.PublicKey
		parent, updated uint64
	}{{voteA, 1000, 900}, {voteB, 500, 400}, {a.IncineratorAddr, 0, 200}} {
		assert.Equal(t, want.key, updated[i].Key)
		assert.Equal(t, want.parent, parents[i].Lamports)
		assert.Equal(t, want.updated, updated[i].Lamports)
	}
	for i := range 2 {
		updated[i].Data[0] ^= 0xff
		assert.NotEqual(t, updated[i].Data[0], parents[i].Data[0], "update %d shares data with its parent", i)
		stored, err := db.GetAccount(10, updated[i].Key)
		require.NoError(t, err)
		assert.Equal(t, parents[i].Data, stored.Data, "update %d shares data with the stored account", i)
	}
}
