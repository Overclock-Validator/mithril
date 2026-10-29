package replay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func rpcRootedBankFixture(t *testing.T) (*accountsdb.AccountsDb, *state.MithrilState) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{3}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 6000}}}
	clock := sealevel.SysvarClock{Slot: 199, Epoch: 1, UnixTimestamp: 123}
	rent := sealevel.SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2}
	slotHashes := sealevel.SysvarSlotHashes{{Slot: 198, Hash: [32]byte{4}}}
	history := sealevel.SysvarSlotHistory{NextSlot: 200}
	active, future := uint64(199), uint64(200)
	activeBytes, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &active})
	require.NoError(t, err)
	futureBytes, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &future})
	require.NoError(t, err)
	storeRPCRootedAccounts(t, db, 199, []*accounts.Account{
		{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()},
		{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1, Data: marshalEpochScheduleForParentLoader(t, schedule)},
		{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: slotHashes.MustMarshal()},
		{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: make([]byte, 8)},
		{Key: sealevel.SysvarLastRestartSlotAddr, Lamports: 1, Data: make([]byte, 8)},
		{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()},
		{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: history.MustMarshal()},
		{Key: features.VirtualAddressSpaceAdjustments.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: activeBytes},
		{Key: features.EnableTxV1.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: futureBytes},
	})
	s := &state.MithrilState{
		ManifestParentSlot: 199, ManifestParentBankhash: (solana.Hash{1}).String(), ManifestBlockHeight: 30,
		ManifestRecentBlockhashes: EncodeRecentBlockhashes(&recent), ManifestEvictedBlockhash: (solana.Hash{2}).String(),
		ManifestLamportsPerSignature: 6000, ManifestFeeRateGovernor: &state.ManifestFeeRateGovernorSeed{TargetLamportsPerSignature: 5000},
		ManifestEpochSchedule: &state.ManifestEpochScheduleSeed{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100},
		ManifestEpochStakes:   map[uint64]string{1: rpcRootedStakeSeed(t, 1, 111), 2: rpcRootedStakeSeed(t, 2, 222)},
	}
	return db, s
}

func storeRPCRootedAccounts(t *testing.T, db *accountsdb.AccountsDb, slot uint64, changed []*accounts.Account) {
	t.Helper()
	done := make(chan struct{})
	require.NoError(t, db.StoreAccounts(changed, slot, func() { close(done) }))
	<-done
}

func rpcRootedStakeSeed(t *testing.T, epoch, total uint64) string {
	t.Helper()
	data, err := json.Marshal(epochstakes.PersistedEpochStakes{Epoch: epoch, TotalStake: total,
		Stakes: map[string]uint64{(solana.PublicKey{1}).String(): total}})
	require.NoError(t, err)
	return string(data)
}

func rpcRootedResumeFixture(t *testing.T, db *accountsdb.AccountsDb, s *state.MithrilState) {
	t.Helper()
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{9}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 7000}}}
	hashes := sealevel.SysvarSlotHashes{{Slot: 298, Hash: [32]byte{7}}}
	clock := sealevel.SysvarClock{Slot: 299, Epoch: 2, UnixTimestamp: 456}
	s.LastRootedSlot = 299
	s.LastRootedContext = &state.ResumeContext{Slot: 299, Epoch: 2, BlockHeight: 60, Bankhash: (solana.Hash{8}).String(),
		Blockhash: (solana.Hash{9}).String(), EvictedBlockhash: (solana.Hash{6}).String(),
		RecentBlockhashes: EncodeRecentBlockhashes(&recent), SlotHashes: EncodeSlotHashes(&hashes),
		Clock: base64.StdEncoding.EncodeToString(clock.MustMarshal()), LamportsPerSignature: 7000, PrevLamportsPerSig: 6500}
	s.ComputedEpochStakes = map[uint64]string{2: rpcRootedStakeSeed(t, 2, 222), 3: rpcRootedStakeSeed(t, 3, 333)}
	s.LastSlot, s.LastLamportsPerSignature = 399, 9999
	s.LastRecentBlockhashes = []state.BlockhashEntry{{Blockhash: (solana.Hash{99}).String(), LamportsPerSignature: 9999}}
	// Durable lifecycle bytes deliberately lag the manifest-carried checkpoint.
	storeRPCRootedAccounts(t, db, 299, []*accounts.Account{{Key: solana.PublicKey{55}, Lamports: 1}})
}

func TestRootedRPCBankContextFreshSnapshotIsReadOnly(t *testing.T) {
	db, s := rpcRootedBankFixture(t)
	staleRecent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{77}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 1}}}
	storeRPCRootedAccounts(t, db, 199, []*accounts.Account{{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: staleRecent.MustMarshal()}})
	before, err := json.Marshal(s)
	require.NoError(t, err)
	cacheBefore := sealevel.SysvarCache
	version, stable := db.CommittedAccountVersion()
	require.True(t, stable)
	bank, err := RootedRPCBankContext(t.Context(), db, s, nil, true)
	require.NoError(t, err)
	require.Equal(t, uint64(199), bank.Slot)
	require.Equal(t, uint64(30), bank.BlockHeight)
	require.Equal(t, solana.Hash{3}, bank.LastBlockhash)
	require.Equal(t, [32]byte{2}, bank.LatestEvictedBlockhash)
	require.Equal(t, uint64(6000), bank.PrevFeeGovernor.LamportsPerSignature)
	require.Equal(t, uint64(222), bank.TotalEpochStake, "bank epoch 1 executes with generation 2, not consensus generation 1")
	require.True(t, bank.Features.IsActive(features.VirtualAddressSpaceAdjustments))
	require.False(t, bank.Features.IsActive(features.EnableTxV1), "next-epoch activation must not leak into the root")
	require.True(t, bank.Features.IsActive(features.VoteStateV4))
	require.Nil(t, bank.UnrootedRead)
	after, err := json.Marshal(s)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, cacheBefore, sealevel.SysvarCache)
	latest, stable := db.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, version, latest)
}

func TestRootedRPCBankContextUsesExactCheckpointNotNewerTip(t *testing.T) {
	db, s := rpcRootedBankFixture(t)
	rpcRootedResumeFixture(t, db, s)
	newerTip := &ResumeState{ParentSlot: 399, LamportsPerSignature: 9999,
		ComputedEpochStakes: map[uint64][]byte{4: []byte(rpcRootedStakeSeed(t, 4, 444))}}
	bank, err := RootedRPCBankContext(t.Context(), db, s, newerTip, false)
	require.NoError(t, err)
	require.Equal(t, uint64(299), bank.Slot)
	require.Equal(t, uint64(60), bank.BlockHeight)
	require.Equal(t, solana.Hash{9}, bank.LastBlockhash)
	require.Equal(t, uint64(7000), bank.PrevFeeGovernor.LamportsPerSignature)
	require.Equal(t, uint64(6500), bank.PrevFeeGovernor.PrevLamportsPerSignature)
	require.Equal(t, uint64(333), bank.TotalEpochStake)
	require.True(t, bank.Features.IsActive(features.EnableTxV1), "the checkpoint's epoch includes the feature that was inactive at the snapshot")
	clock, ok := bank.BankSysvars.Clock()
	require.True(t, ok)
	require.Equal(t, uint64(299), clock.Slot)
	require.Equal(t, int64(456), clock.UnixTimestamp)
	hashes, ok := bank.BankSysvars.SlotHashes()
	require.True(t, ok)
	require.Equal(t, uint64(298), hashes[0].Slot)
	stored, err := db.GetAccount(299, sealevel.SysvarClockAddr)
	require.NoError(t, err)
	var oldClock sealevel.SysvarClock
	require.NoError(t, oldClock.UnmarshalWithDecoder(bin.NewBinDecoder(stored.Data)))
	require.Equal(t, uint64(199), oldClock.Slot, "reconstruction must not overwrite cached durable accounts")
	matching := &ResumeState{ParentSlot: 299, ComputedEpochStakes: map[uint64][]byte{3: []byte(rpcRootedStakeSeed(t, 3, 345))}}
	bank, err = RootedRPCBankContext(t.Context(), db, s, matching, false)
	require.NoError(t, err)
	require.Equal(t, uint64(345), bank.TotalEpochStake, "matching recovery metadata must not use a stale saved stake map")
}

func TestRootedRPCBankContextRejectsMissingOrMalformedState(t *testing.T) {
	for _, invalid := range []string{"root-slot", "height", "fees", "recent", "hash", "fee-head", "evicted", "clock", "epoch", "slot-hashes", "rent", "feature", "account-budget", "stake-generation", "stake-identity", "account-root"} {
		t.Run(invalid, func(t *testing.T) {
			db, s := rpcRootedBankFixture(t)
			rpcRootedResumeFixture(t, db, s)
			switch invalid {
			case "root-slot":
				s.LastRootedSlot++
			case "height":
				s.LastRootedContext.BlockHeight = 0
			case "fees":
				s.ManifestFeeRateGovernor = nil
			case "recent":
				s.LastRootedContext.RecentBlockhashes = nil
			case "hash":
				s.LastRootedContext.Blockhash = (solana.Hash{99}).String()
			case "fee-head":
				s.LastRootedContext.RecentBlockhashes[0].LamportsPerSignature++
			case "evicted":
				s.LastRootedContext.EvictedBlockhash = "invalid"
			case "clock":
				s.LastRootedContext.Clock = base64.StdEncoding.EncodeToString([]byte{1})
			case "epoch":
				s.LastRootedContext.Epoch = 3
			case "slot-hashes":
				s.LastRootedContext.SlotHashes[0].Hash = "invalid"
			case "rent":
				storeRPCRootedAccounts(t, db, 299, []*accounts.Account{{Key: sealevel.SysvarRentAddr}})
			case "feature":
				storeRPCRootedAccounts(t, db, 299, []*accounts.Account{{Key: features.EnableTxV1.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: []byte{1}}})
			case "account-budget":
				storeRPCRootedAccounts(t, db, 299, []*accounts.Account{
					{Key: features.EnableTxV1.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: make([]byte, 9<<20)},
					{Key: features.VirtualAddressSpaceAdjustments.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: make([]byte, 9<<20)},
				})
			case "stake-generation":
				delete(s.ComputedEpochStakes, 3)
			case "stake-identity":
				s.ComputedEpochStakes[3] = rpcRootedStakeSeed(t, 4, 444)
			case "account-root":
				storeRPCRootedAccounts(t, db, 300, []*accounts.Account{{Key: solana.PublicKey{55}, Lamports: 2}})
			}
			bank, err := RootedRPCBankContext(t.Context(), db, s, nil, false)
			require.Error(t, err)
			if invalid == "account-budget" {
				require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
			}
			require.Nil(t, bank)
		})
	}
}

type rpcRootedReadContext struct {
	context.Context
	calls  atomic.Int32
	onRead func()
}

func (ctx *rpcRootedReadContext) Err() error {
	if ctx.calls.Add(1) == 2 {
		ctx.onRead()
	}
	return ctx.Context.Err()
}

func TestRootedRPCBankContextRejectsChangedAccountVersion(t *testing.T) {
	db, s := rpcRootedBankFixture(t)
	ctx := &rpcRootedReadContext{Context: t.Context(), onRead: func() {
		storeRPCRootedAccounts(t, db, 199, []*accounts.Account{{Key: solana.PublicKey{55}, Lamports: 2}})
	}}
	bank, err := RootedRPCBankContext(ctx, db, s, nil, false)
	require.ErrorContains(t, err, "account view changed")
	require.Nil(t, bank)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = RootedRPCBankContext(canceled, db, s, nil, false)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRootedRPCBankContextDoesNotUseStaleGlobalStakes(t *testing.T) {
	db, s := rpcRootedBankFixture(t)
	rpcRootedResumeFixture(t, db, s)
	previous, exists := global.EpochStakesSnapshot(3)
	global.PutEpochStakes(3, map[solana.PublicKey]uint64{{1}: 999}, nil, 999)
	t.Cleanup(func() {
		if exists {
			global.PutEpochStakes(3, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(3)
		}
	})
	delete(s.ComputedEpochStakes, 3)
	_, err := RootedRPCBankContext(t.Context(), db, s, nil, false)
	require.Error(t, err)
	require.Equal(t, uint64(999), global.EpochTotalStake(3))
}
