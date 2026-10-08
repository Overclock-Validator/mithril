package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
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
	require.NoError(t, distributeBlockTxFees(ctx, block, &fixture.Fees))
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
