package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestEpochFeatureRescanMatchesSingleProgramMigration(t *testing.T) {
	oldCache := sealevel.SysvarCache
	t.Cleanup(func() { sealevel.SysvarCache = oldCache })
	const parentSlot, childSlot, parentCapitalization = uint64(7), uint64(8), uint64(1_000_000_000)
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	rent := &sealevel.SysvarRent{LamportsPerUint8Year: 1, ExemptionThreshold: 2}
	rentAccount := &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()}
	featureData := make([]byte, 9) // bincode Option::None: activation pending
	bufferHeader, err := marshalUpgradeableLoaderStateSized(&sealevel.UpgradeableLoaderState{
		Type: sealevel.UpgradeableLoaderStateTypeBuffer,
	}, upgradeableLoaderBufferMetadataSize)
	require.NoError(t, err)
	seedDurableAccounts(t, db, []*accounts.Account{
		rentAccount,
		{Key: features.ReplaceSplTokenWithPToken.Address, Owner: a.FeatureAddr, Lamports: 1, Data: featureData},
		{Key: splTokenProgramID, Owner: a.BpfLoader2Addr, Executable: true, Lamports: 1000, Data: []byte{1}},
		{Key: pTokenProgramBuffer, Owner: a.BpfLoaderUpgradeableAddr, Lamports: 100000,
			Data: append(bufferHeader, fixtures.Load(t, "sbpf", "noop_aligned.so")...)},
	}, parentSlot)
	parentSysvars, err := sealevel.NewBankSysvars(parentSlot, rentAccount)
	require.NoError(t, err)
	clean := &ReplayCtx{Capitalization: parentCapitalization}
	require.NoError(t, prepareEpochBoundaryFeatureScan(clean, parentCapitalization, parentSlot, parentSysvars, db))
	_, wantWrites, wantParents := scanAndEnableFeatures(db, clean, childSlot, true)
	require.Len(t, wantWrites, 4, "feature plus program/programdata/buffer migration must execute")
	require.NotEqual(t, parentCapitalization, clean.Capitalization, "fixture must exercise a migration lamport delta")

	// Startup has already staged this migration once. Before the real boundary
	// scan, inject an abandoned branch's Rent and retain the preliminary delta.
	working := &ReplayCtx{Capitalization: clean.Capitalization}
	sealevel.SysvarCache.Rent.Sysvar = &sealevel.SysvarRent{LamportsPerUint8Year: 500, ExemptionThreshold: 7}
	require.NoError(t, prepareEpochBoundaryFeatureScan(working, parentCapitalization, parentSlot, parentSysvars, db))
	_, gotWrites, gotParents := scanAndEnableFeatures(db, working, childSlot, true)
	require.Equal(t, clean.Capitalization, working.Capitalization, "startup+boundary scan charges exactly one migration")
	require.Equal(t, wantWrites, gotWrites)
	require.Equal(t, wantParents, gotParents)

	// A sibling boundary after an in-loop fork also restages every migration
	// write from durable parent state, independently of abandoned globals.
	sealevel.SysvarCache.Rent.Sysvar = &sealevel.SysvarRent{LamportsPerUint8Year: 900, ExemptionThreshold: 3}
	require.NoError(t, prepareEpochBoundaryFeatureScan(working, parentCapitalization, parentSlot, nil, db))
	_, siblingWrites, siblingParents := scanAndEnableFeatures(db, working, childSlot, true)
	require.Equal(t, clean.Capitalization, working.Capitalization)
	require.Equal(t, wantWrites, siblingWrites)
	require.Equal(t, wantParents, siblingParents)
}

func TestEpochFeaturePreparationFailurePreservesWorkingState(t *testing.T) {
	ctx := &ReplayCtx{Capitalization: 456}
	previousRent := sealevel.SysvarCache.Rent
	wrongParent, err := sealevel.NewBankSysvars(8)
	require.NoError(t, err)
	require.Error(t, prepareEpochBoundaryFeatureScan(ctx, 123, 7, wrongParent, nil))
	require.Equal(t, uint64(456), ctx.Capitalization)
	require.Equal(t, previousRent, sealevel.SysvarCache.Rent)
}
