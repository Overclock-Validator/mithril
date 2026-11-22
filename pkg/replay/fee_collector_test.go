package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The captured LtHash includes every original slot mutation. Replacing only
// the misdirected 72,502-lamport credit must reproduce the certified footer.
// The epoch-1054 metadata was recovered from the retained vote account at
// slot 449372255 (before epoch 1053); all cached metadata matched that account.
func TestFeeCollectorSlot449810864(t *testing.T) {
	var fixture struct {
		Slot, Epoch  uint64
		Leader, Vote solana.PublicKey
		EpochStakes  json.RawMessage `json:"epoch_stakes"`
		Accounts     []*accounts.Account
		Fees         fees.TxFeeInfoAccumulator
		ParentHash   solana.Hash `json:"parent_bankhash"`
		Blockhash    solana.Hash
		Signatures   uint64      `json:"num_signatures"`
		BrokenHash   solana.Hash `json:"broken_bankhash"`
		ExpectedHash solana.Hash `json:"expected_bankhash"`
		BrokenLtHash []byte      `json:"broken_lthash"`
	}
	raw, err := os.ReadFile("testdata/slot_449810864_fee_collector.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() {
		global.ClearEpochStakes(fixture.Epoch)
		global.ClearEpochStakes(fixture.Epoch + 1)
		global.SetLeaderSchedule(nil)
	})
	_, err = global.DeserializeAndLoadEpochStakes(fixture.EpochStakes)
	require.NoError(t, err)
	historical := global.EpochStakesVoteAccts(fixture.Epoch)[fixture.Vote]
	// A newer epoch and the live vote account deliberately disagree. Neither
	// may override the immutable metadata used by this epoch's schedule.
	other := solana.PublicKey{42}
	global.PutEpochStakes(fixture.Epoch+1, map[solana.PublicKey]uint64{fixture.Vote: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{fixture.Vote: {NodePubkey: fixture.Leader, BlockRevenueCollector: &other}}, 1)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	f.EnableFeature(features.RewardFullPriorityFee, 0)
	mem := accounts.NewMemAccounts()
	var leaderBefore, collectorBefore, rentAccount *accounts.Account
	for _, acct := range fixture.Accounts {
		if acct.Key == sealevel.SysvarRentAddr {
			rentAccount = acct
		}
		mem.SetAccountWithoutLock(acct.Key, acct.Clone())
		if acct.Key == fixture.Leader {
			leaderBefore = acct.Clone()
		}
		if acct.Key == *historical.BlockRevenueCollector {
			collectorBefore = acct.Clone()
		}
	}
	require.NotNil(t, leaderBefore)
	require.NotNil(t, collectorBefore)
	ctx := &sealevel.SlotCtx{Slot: fixture.Slot, Epoch: fixture.Epoch, Features: f, Accounts: mem, ParentAccts: accounts.NewMemAccounts(), AcctMapsMu: new(sync.Mutex), ModifiedAccts: map[solana.PublicKey]bool{}, WritableAccts: map[solana.PublicKey]bool{}}
	bankSysvars, err := sealevel.NewBankSysvars(fixture.Slot, rentAccount)
	require.NoError(t, err)
	require.NoError(t, ctx.PublishBankSysvars(bankSysvars))
	block := &b.Block{Slot: fixture.Slot, Epoch: fixture.Epoch, Leader: fixture.Leader, Features: f}
	block.EnablePaymentObservation(fixture.Leader, fixture.Vote)
	block.EnableSelectedEconomics()
	require.NoError(t, distributeBlockTxFees(ctx, block, &fixture.Fees))
	economics, enabled := block.SelectedEconomics()
	require.True(t, enabled)
	require.True(t, economics.FeesKnown)
	require.True(t, economics.FeeBurnKnown)
	require.Equal(t, uint64(72500), economics.FeeBurnLamports)
	require.Equal(t, b.SelectedFeeFacts{ExecutionLamports: 145000, PriorityLamports: 2, TotalLamports: 145002}, economics.Fees)
	payment, ok := block.PaymentObservation()
	require.True(t, ok)
	require.True(t, payment.FeesKnown)
	require.Equal(t, fixture.Fees.TotalFees, payment.Fees)
	require.Equal(t, uint64(72502), payment.FeeDeposited)
	require.Equal(t, uint64(72500), payment.FeeBurned)
	require.Equal(t, *historical.BlockRevenueCollector, payment.FeeCollector)
	require.Equal(t, uint64(72500), ctx.LamportsBurnt)
	leaderAfter, err := ctx.GetAccount(fixture.Leader)
	require.NoError(t, err)
	require.Equal(t, leaderBefore, leaderAfter)
	collectorAfter, err := ctx.GetAccount(*historical.BlockRevenueCollector)
	require.NoError(t, err)
	require.Equal(t, uint64(28653694097), collectorAfter.Lamports)
	require.Equal(t, map[solana.PublicKey]bool{collectorAfter.Key: true}, ctx.ModifiedAccts)
	lt := new(lthash.LtHash).InitWithHash(fixture.BrokenLtHash)
	hash := func() solana.Hash {
		data := append([]byte(nil), fixture.ParentHash[:]...)
		data = binary.LittleEndian.AppendUint64(data, fixture.Signatures)
		data = append(data, fixture.Blockhash[:]...)
		inner := sha256.Sum256(data)
		return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
	}
	require.Equal(t, fixture.BrokenHash, hash())
	wrongLeader := leaderBefore.Clone()
	wrongLeader.Lamports += 72502
	lt.Sub(new(lthash.LtHash).InitWithAcct(wrongLeader))
	lt.Add(new(lthash.LtHash).InitWithAcct(leaderAfter))
	lt.Sub(new(lthash.LtHash).InitWithAcct(collectorBefore))
	lt.Add(new(lthash.LtHash).InitWithAcct(collectorAfter))
	require.Equal(t, fixture.ExpectedHash, hash())
}

func TestTransactionFeeCollectorRequiresHistoricalMetadata(t *testing.T) {
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.ClearEpochStakes(0); global.SetLeaderSchedule(nil) })
	node, vote, collector := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	block := &b.Block{Leader: node, Features: f}
	for _, tc := range []struct {
		name     string
		metadata *epochstakes.VoteAccount
		want     solana.PublicKey
		wantErr  bool
	}{
		{"old_cache_missing_field", &epochstakes.VoteAccount{NodePubkey: node}, solana.PublicKey{}, true},
		{"custom", &epochstakes.VoteAccount{NodePubkey: node, BlockRevenueCollector: &collector}, collector, false},
		{"legacy_explicit_identity", &epochstakes.VoteAccount{NodePubkey: node, BlockRevenueCollector: &node}, node, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			global.PutEpochStakes(0, map[solana.PublicKey]uint64{vote: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{vote: tc.metadata}, 1)
			got, gotVote, err := transactionFeeCollector(block)
			if tc.wantErr {
				require.ErrorContains(t, err, "missing historical block revenue collector")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, vote, gotVote)
		})
	}
	f.DisableFeature(features.CustomCommissionCollector)
	got, _, err := transactionFeeCollector(block)
	require.NoError(t, err)
	require.Equal(t, node, got)
}

func TestSelectedFeeZeroAndInvalidAccumulator(t *testing.T) {
	block := new(b.Block)
	block.EnableSelectedEconomics()
	// The real zero-fee path needs no collector, account database or vote key.
	require.NoError(t, distributeBlockTxFees(nil, block, new(fees.TxFeeInfoAccumulator)))
	got, enabled := block.SelectedEconomics()
	require.True(t, enabled)
	require.True(t, got.FeesKnown)
	require.True(t, got.FeeBurnKnown)
	require.Zero(t, got.FeeBurnLamports)
	require.Zero(t, got.Fees)
	_, paymentEnabled := block.PaymentObservation()
	require.False(t, paymentEnabled)
	// Preserve the existing early-return behavior, but never call an invalid
	// triple a known zero observation.
	require.NoError(t, distributeBlockTxFees(nil, block, &fees.TxFeeInfoAccumulator{ExecutionFees: 1}))
	got, enabled = block.SelectedEconomics()
	require.True(t, enabled)
	require.Equal(t, b.SelectedEconomics{}, got)
}

type selectedFeeFailingStore struct{ accounts.Accounts }

var errSelectedFeeStore = errors.New("selected fee fixture store failure")

func (selectedFeeFailingStore) SetAccount(*[32]byte, *accounts.Account) error {
	return errSelectedFeeStore
}

func TestSelectedFeeDistributionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		before       uint64
		failStore    bool
		wantBurn     uint64
		wantIncrease uint64
	}{
		{name: "collector_deposit", before: 1000000, wantBurn: 50, wantIncrease: 50},
		{name: "collector_overflow_burn", before: math.MaxUint64, wantBurn: 100},
		{name: "store_error", before: 1000000, failStore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := solana.PublicKey{41}
			mem := accounts.NewMemAccounts()
			require.NoError(t, mem.SetAccountWithoutLock(leader, &accounts.Account{Key: leader, Lamports: tc.before}))
			f := features.NewFeaturesDefault()
			f.EnableFeature(features.RewardFullPriorityFee, 0)
			ctx := &sealevel.SlotCtx{Slot: 17, Features: f, Accounts: mem, ParentAccts: accounts.NewMemAccounts(),
				AcctMapsMu: new(sync.Mutex), ModifiedAccts: map[solana.PublicKey]bool{}, WritableAccts: map[solana.PublicKey]bool{}}
			rent := sealevel.NewDefaultRentSysvar()
			sysvars, err := sealevel.NewBankSysvars(ctx.Slot, &accounts.Account{Key: sealevel.SysvarRentAddr, Data: rent.MustMarshal()})
			require.NoError(t, err)
			require.NoError(t, ctx.PublishBankSysvars(sysvars))
			block := &b.Block{Slot: ctx.Slot, Leader: leader, Features: f}
			block.EnableSelectedEconomics()
			block.RecordSelectedFees(1, 0, 1)
			if tc.failStore {
				ctx.Accounts = selectedFeeFailingStore{Accounts: mem}
			}
			err = distributeBlockTxFees(ctx, block, &fees.TxFeeInfoAccumulator{ExecutionFees: 100, TotalFees: 100})
			got, enabled := block.SelectedEconomics()
			require.True(t, enabled)
			if tc.failStore {
				require.ErrorIs(t, err, errSelectedFeeStore)
				require.Equal(t, b.SelectedEconomics{}, got, "failed distribution retained a previous result")
			} else {
				require.NoError(t, err)
				require.True(t, got.FeesKnown)
				require.Equal(t, b.SelectedFeeFacts{ExecutionLamports: 100, TotalLamports: 100}, got.Fees)
				require.Equal(t, tc.wantBurn, ctx.LamportsBurnt)
			}
			after, err := mem.GetAccountWithoutLock(leader)
			require.NoError(t, err)
			require.Equal(t, tc.before+tc.wantIncrease, after.Lamports)
		})
	}
}

func TestSelectedFeeCollectorErrorClearsPriorResult(t *testing.T) {
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.ClearEpochStakes(0); global.SetLeaderSchedule(nil) })
	leader, vote := solana.PublicKey{41}, solana.PublicKey{42}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	global.PutEpochStakes(0, map[solana.PublicKey]uint64{vote: 1},
		map[solana.PublicKey]*epochstakes.VoteAccount{vote: {NodePubkey: leader}}, 1)
	block := &b.Block{Leader: leader, Features: f}
	block.EnableSelectedEconomics()
	block.RecordSelectedFees(1, 0, 1)
	err := distributeBlockTxFees(nil, block, &fees.TxFeeInfoAccumulator{ExecutionFees: 100, TotalFees: 100})
	require.ErrorContains(t, err, "missing historical block revenue collector")
	got, enabled := block.SelectedEconomics()
	require.True(t, enabled)
	require.Equal(t, b.SelectedEconomics{}, got)
}
