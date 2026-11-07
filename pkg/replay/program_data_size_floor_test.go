package replay

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLoaderV3ProgramDataSizeFloor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		missing    bool
		lamports   uint64
		dataLen    int
		legacySize uint64
		activeSize uint64
	}{
		{"missing", true, 0, 0, 0, 109},
		{"empty", false, 1, 0, 64, 109},
		{"short", false, 1, 44, 108, 109},
		{"metadata", false, 1, 45, 109, 109},
		{"larger", false, 1, 128, 192, 192},
		{"closed", false, 0, 0, 64, 109},
		{"closed with retained bytes", false, 0, 128, 192, 109},
	} {
		for _, active := range []bool{false, true} {
			for _, source := range []string{"current", "shared parent", "owned parent"} {
				t.Run(fmt.Sprintf("%s/active=%t/%s", tc.name, active, source), func(t *testing.T) {
					ctx := newSimd186SlotCtx()
					if active {
						ctx.Features.EnableFeature(features.LoaderV3ProgramDataSizeFloor, 466128000)
					}
					dataKey := testPubkey(89)
					program := sizeTestProgram(t, dataKey)
					var data *accounts.Account
					if !tc.missing {
						data = &accounts.Account{Key: dataKey, Lamports: tc.lamports, Data: make([]byte, tc.dataLen)}
					}
					switch source {
					case "current":
						if data != nil {
							require.NoError(t, ctx.SetAccount(dataKey, data))
						}
					case "shared parent", "owned parent":
						tail := newUnrootedTail(&sharedAccountDurable{acct: data}, &fakeCommitter{}, 512, 1, "")
						ctx.UnrootedRead = tail
						if source == "owned parent" {
							ctx.UnrootedRead = accountOnlyReader{source: tail}
						}
					}
					want := uint64(100) + tc.legacySize // 64-byte base + 36-byte Program
					if active {
						want = 100 + tc.activeSize
					}
					for _, limit := range []uint64{want, want - 1} {
						acc := NewLoadedAcctSizeAccumulatorSimd186(ctx, limit, []solana.PublicKey{program.Key})
						err := acc.collectAcct(program)
						if limit < want {
							require.ErrorIs(t, err, TxErrMaxLoadedAccountsDataSizeExceeded)
						} else {
							require.NoError(t, err)
						}
						require.Equal(t, uint32(limit), acc.loadedAccountsDataSize())
					}
				})
			}
		}
	}
}

func TestLoaderV3ProgramDataFloorDeduplication(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%t/missing=%t/reverse=%t", explicit, missing, reverse), func(t *testing.T) {
					ctx := newSimd186SlotCtx()
					ctx.Features.EnableFeature(features.LoaderV3ProgramDataSizeFloor, 0)
					dataKey := testPubkey(89)
					first, second := sizeTestProgram(t, dataKey), sizeTestProgram(t, dataKey)
					second.Key = testPubkey(90)
					require.NoError(t, ctx.SetAccount(first.Key, first))
					require.NoError(t, ctx.SetAccount(second.Key, second))
					if !missing {
						require.NoError(t, ctx.SetAccount(dataKey, &accounts.Account{Key: dataKey, Lamports: 1, Data: make([]byte, 17)}))
					}
					keys := []solana.PublicKey{first.Key, second.Key}
					want := uint32(200 + 109) // shared implicit ProgramData counted once
					if explicit {
						keys = append(keys, dataKey)
						want = 200 // explicit absent account contributes zero
						if !missing {
							want += 64 + 17 // explicit account is not subject to the floor
						}
					}
					if reverse {
						for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
							keys[i], keys[j] = keys[j], keys[i]
						}
					}
					tx := &solana.Transaction{Message: solana.Message{AccountKeys: keys}}
					_, _, size, err := loadAndValidateTxAcctsSimd186(ctx, nil, tx, nil, nil, math.MaxUint32)
					require.NoError(t, err)
					require.Equal(t, want, size)
				})
			}
		}
	}
}

func TestLoaderV3ProgramDataFloorCurrentBankOverridesParent(t *testing.T) {
	ctx := newSimd186SlotCtx()
	ctx.Features.EnableFeature(features.LoaderV3ProgramDataSizeFloor, 0)
	key := testPubkey(89)
	program := sizeTestProgram(t, key)
	tail := newUnrootedTail(&sharedAccountDurable{acct: &accounts.Account{
		Key: key, Lamports: 1, Data: make([]byte, 2<<20),
	}}, &fakeCommitter{}, 512, 1, "")
	ctx.UnrootedRead = tail
	for _, lamports := range []uint64{0, 1} {
		require.NoError(t, ctx.SetAccount(key, &accounts.Account{Key: key, Lamports: lamports, Data: make([]byte, 17)}))
		acc := NewLoadedAcctSizeAccumulatorSimd186(ctx, 209, []solana.PublicKey{program.Key})
		require.NoError(t, acc.collectAcct(program))
		require.Equal(t, uint32(209), acc.loadedAccountsDataSize())
	}
}

func TestLoaderV3ProgramDataFloorActivation(t *testing.T) {
	const activationSlot = uint64(466128000)
	key := solana.MustPublicKeyFromBase58("8whkDenELnfwYLirBc4Wb4H2Z1z5K7tiykpdxJS7cWg1")
	require.Equal(t, [32]byte(key), features.LoaderV3ProgramDataSizeFloor.Address)

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	seedDurableAccounts(t, db, []*accounts.Account{{
		Key: key, Lamports: 695960, Owner: addresses.FeatureAddr, Data: make([]byte, 9),
	}}, activationSlot-1)

	inactive, modified, parents := scanAndEnableFeatures(db, &ReplayCtx{}, activationSlot-1, false)
	require.False(t, inactive.IsActive(features.LoaderV3ProgramDataSizeFloor))
	require.Empty(t, modified)
	require.Empty(t, parents)

	active, modified, parents := scanAndEnableFeatures(db, &ReplayCtx{}, activationSlot, true)
	require.True(t, active.IsActive(features.LoaderV3ProgramDataSizeFloor))
	require.Len(t, modified, 1)
	require.Len(t, parents, 1)
	require.Equal(t, key, modified[0].Key)
	// Exact mainnet activation bytes; parent must remain the inactive value
	// so the account's LtHash delta includes the feature activation.
	require.Equal(t, []byte{1, 0x80, 0x8c, 0xc8, 0x1b, 0, 0, 0, 0}, modified[0].Data)
	require.Equal(t, make([]byte, 9), parents[0].Data)
	require.Equal(t, parents[0].Lamports, modified[0].Lamports)
	require.Equal(t, parents[0].Owner, modified[0].Owner)

	seedDurableAccounts(t, db, modified, activationSlot)
	for _, slot := range []uint64{activationSlot, activationSlot + 1} {
		resumed, modified, parents := scanAndEnableFeatures(db, &ReplayCtx{}, slot, false)
		require.True(t, resumed.IsActive(features.LoaderV3ProgramDataSizeFloor))
		at, ok := resumed.ActivationSlot(features.LoaderV3ProgramDataSizeFloor)
		require.True(t, ok)
		require.Equal(t, activationSlot, at)
		require.Empty(t, modified)
		require.Empty(t, parents)
	}
}
