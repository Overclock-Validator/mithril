package epochrewards

import (
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestStoreKeepsConfirmedAndFinalizedViewsSeparate(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 256)
	require.NoError(t, err)

	address := solana.NewWallet().PublicKey()
	commission := uint8(7)
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot: 130, Epoch: 2,
		Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: 42, PostBalance: 1_042,
			RewardType: rpc.RewardTypeVoting, Commission: &commission,
		}},
	}))

	confirmed, ok := store.Get(1, address.String(), true)
	require.True(t, ok)
	require.Equal(t, uint64(42), confirmed.Amount)
	require.Equal(t, uint64(130), confirmed.EffectiveSlot)
	require.Equal(t, uint64(1_042), confirmed.PostBalance)
	require.Equal(t, uint8(7), *confirmed.Commission)
	_, ok = store.Get(1, address.String(), false)
	require.False(t, ok)

	require.NoError(t, store.Prepare(130))
	require.NoError(t, store.SetRooted(130))
	finalized, ok := store.Get(1, address.String(), false)
	require.True(t, ok)
	require.Equal(t, confirmed, finalized)

	reopened, err := Open(dir, 256)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(130))
	restarted, ok := reopened.Get(1, address.String(), false)
	require.True(t, ok)
	require.Equal(t, finalized, restarted)
}

func TestStoreHidesPreparedRewardsUntilTheirFoldCommits(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	address := solana.PublicKey{1}
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 11, Epoch: 2, Rewards: []rpc.BlockReward{{
		Pubkey: address, Lamports: 5, RewardType: rpc.RewardTypeStaking,
	}}}))
	require.NoError(t, store.Prepare(12))
	store.rooted.Store(11) // The account fold through slot 12 has not committed.
	_, ok := store.Get(1, address.String(), false)
	require.False(t, ok)
	require.NoError(t, store.SetRooted(12))
	_, ok = store.Get(1, address.String(), false)
	require.True(t, ok)
}

func TestStoreKeepsZeroInflationRewards(t *testing.T) {
	store, err := Open(t.TempDir(), 32)
	require.NoError(t, err)
	address := solana.NewWallet().PublicKey()
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot: 33, Epoch: 1,
		Rewards: []rpc.BlockReward{
			{Pubkey: address, Lamports: 10, RewardType: rpc.RewardTypeFee},
			{Pubkey: address, Lamports: 0, RewardType: rpc.RewardTypeStaking},
		},
	}))
	reward, ok := store.Get(0, address.String(), true)
	require.True(t, ok)
	require.Zero(t, reward.Amount)
	require.Equal(t, uint64(33), reward.EffectiveSlot)
	require.NoError(t, store.Prepare(33))
	entries, err := batchFiles(store.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStoreRecordsLiveAlpenglowVotingRewardAfterVATDebit(t *testing.T) {
	store, err := Open(t.TempDir(), 108_000)
	require.NoError(t, err)
	vote := solana.MustPublicKeyFromBase58("TitanB6gCvNeb5RLM1RuGNT1mgmqxR6Q8hXMALQpLuJ")
	// A VAT debit must not hide the voting inflation credit in the same block.
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot: 7_236_004, Epoch: 134,
		Rewards: []rpc.BlockReward{
			{Pubkey: vote, Lamports: -800_000_000, PostBalance: 19_022_056_333_577, RewardType: rpc.RewardType("VATDebit")},
			{Pubkey: vote, Lamports: 413_137_680_751, PostBalance: 19_435_194_014_328, RewardType: rpc.RewardTypeVoting},
		},
	}))
	require.NoError(t, store.Prepare(7_236_004))
	require.NoError(t, store.SetRooted(7_236_004))
	reward, ok := store.Get(133, vote.String(), false)
	require.True(t, ok)
	require.Equal(t, uint64(7_236_004), reward.EffectiveSlot)
	require.Equal(t, uint64(413_137_680_751), reward.Amount)
	require.Equal(t, uint64(19_435_194_014_328), reward.PostBalance)
	require.Nil(t, reward.Commission)
}

func TestStoreDropsUnselectedAndExpiredBatches(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 10)
	require.NoError(t, err)
	address := solana.NewWallet().PublicKey()
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot: 20, Epoch: 2,
		Rewards: []rpc.BlockReward{{Pubkey: address, Lamports: 5, RewardType: rpc.RewardTypeStaking}},
	}))
	require.NoError(t, store.Prepare(20))

	reopened, err := Open(dir, 10)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(19))
	_, ok := reopened.Get(1, address.String(), false)
	require.False(t, ok, "a batch ahead of the recovered root is an orphan")

	require.NoError(t, reopened.RecordBlock(&b.Block{
		Slot: 21, Epoch: 2,
		Rewards: []rpc.BlockReward{{Pubkey: address, Lamports: 6, RewardType: rpc.RewardTypeStaking}},
	}))
	require.NoError(t, reopened.Prepare(21))
	require.NoError(t, reopened.SetRooted(21))
	_, ok = reopened.Get(1, address.String(), false)
	require.True(t, ok)
	require.NoError(t, reopened.SetRooted(32))
	_, ok = reopened.Get(1, address.String(), false)
	require.False(t, ok, "records outside retention are removed")
}

func TestStoreRewindsSpeculativeRewards(t *testing.T) {
	store, err := Open(t.TempDir(), 32)
	require.NoError(t, err)
	address := solana.NewWallet().PublicKey()
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot: 33, Epoch: 1,
		Rewards: []rpc.BlockReward{{Pubkey: address, Lamports: 5, RewardType: rpc.RewardTypeVoting}},
	}))
	require.NoError(t, store.Prepare(33))

	require.NoError(t, store.Rewind(33))
	_, ok := store.Get(0, address.String(), true)
	require.False(t, ok)
	files, err := batchFiles(store.dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func batchFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}
