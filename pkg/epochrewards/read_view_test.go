package epochrewards

import (
	"fmt"
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestVerifiedRewardViewLifecycle(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, s.SetRooted(10))
	address := solana.PublicKey{1}
	commission := uint8(25)
	record := func(slot, amount uint64) {
		t.Helper()
		require.NoError(t, s.RecordBlock(&b.Block{Slot: slot, Epoch: 1, Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: int64(amount), PostBalance: 100 + amount,
			RewardType: rpc.RewardTypeVoting, Commission: &commission,
		}}}))
	}
	record(11, 7)
	s.RecordCoverage(0, 11, true, true)
	_, ok := s.GetPublished(0, address.String())
	require.False(t, ok)
	require.False(t, s.EpochPublished(0))
	s.Publish(11)
	require.Equal(t, uint64(10), s.RootedSlot())
	require.Equal(t, uint64(11), s.PublishedSlot())
	reward, ok := s.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, uint64(7), reward.Amount)
	require.True(t, s.EpochPublished(0))
	*reward.Commission = 99
	reward, _ = s.GetPublished(0, address.String())
	require.Equal(t, uint8(25), *reward.Commission, "returned values must not alias the published view")

	record(12, 9)
	reward, _ = s.GetPublished(0, address.String())
	require.Equal(t, uint64(7), reward.Amount, "speculative overwrite cannot change a published reward")
	require.NoError(t, s.Prepare(12))
	reward, _ = s.GetPublished(0, address.String())
	require.Equal(t, uint64(7), reward.Amount, "prepared files are not read publication")
	reopened, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(10))
	_, ok = reopened.GetPublished(0, address.String())
	require.False(t, ok, "restart trusts only the durable account root")
	require.Equal(t, uint64(10), reopened.PublishedSlot())

	require.NoError(t, s.Rewind(12))
	require.Equal(t, uint64(10), s.PublishedSlot(), "rewind withdraws the entire uncommitted view")
	_, ok = s.GetPublished(0, address.String())
	require.False(t, ok)
	record(11, 7)
	s.Publish(11)
	require.NoError(t, s.Prepare(11))
	require.NoError(t, s.SetRooted(11))
	require.Empty(t, s.published, "durable history takes ownership")
	require.Empty(t, s.unpublished)
	reward, ok = s.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, uint64(7), reward.Amount)
	s.ResetPublished()
	reward, ok = s.GetPublished(0, address.String())
	require.True(t, ok, "reset does not delete durable rewards")
	reopened, err = Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(11))
	restored, ok := reopened.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, reward, restored)
	require.NoError(t, s.SetRooted(112))
	_, ok = s.GetPublished(0, address.String())
	require.False(t, ok, "retention applies after publication too")
	require.False(t, s.EpochPublished(0))
}

func TestVerifiedViewSurvivesFailedDurableMaintenance(t *testing.T) {
	s, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	address := solana.PublicKey{1}
	require.NoError(t, s.SetRooted(10))
	require.NoError(t, s.RecordBlock(&b.Block{Slot: 11, Epoch: 1, Rewards: []rpc.BlockReward{{
		Pubkey: address, Lamports: 7, RewardType: rpc.RewardTypeVoting,
	}}}))
	s.RecordCoverage(0, 11, true, true)
	s.Publish(11)
	require.NoError(t, s.Prepare(11))
	require.NoError(t, s.Prepare(12))
	path := s.batchPath(11)
	require.NoError(t, os.Rename(path, path+".saved"))
	require.Error(t, s.SetRooted(11), "missing committed batch makes orphan recovery fail")
	require.Equal(t, uint64(10), s.RootedSlot())
	require.Equal(t, uint64(11), s.PublishedSlot())
	reward, ok := s.GetPublished(0, address.String())
	require.True(t, ok, "failed durable maintenance must not erase verified capture")
	require.Equal(t, uint64(7), reward.Amount)
	require.True(t, s.EpochPublished(0))
	require.NoError(t, os.Rename(path+".saved", path))
	require.NoError(t, s.SetRooted(11))
	require.Equal(t, uint64(11), s.RootedSlot())
	require.Empty(t, s.published)
	reward, ok = s.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, uint64(7), reward.Amount)
}

func TestVerifiedViewInvalidatedBeforeFailedRewind(t *testing.T) {
	s, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, s.SetRooted(10))
	address := solana.PublicKey{1}
	require.NoError(t, s.RecordBlock(&b.Block{Slot: 11, Epoch: 1, Rewards: []rpc.BlockReward{{
		Pubkey: address, Lamports: 7, RewardType: rpc.RewardTypeVoting,
	}}}))
	s.Publish(11)
	require.NoError(t, s.Prepare(11))
	path := s.batchPath(11)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	require.NoError(t, os.WriteFile(path+"/occupied", []byte("fixture"), 0o600))
	require.Error(t, s.Rewind(11))
	require.Equal(t, uint64(10), s.PublishedSlot())
	_, ok := s.GetPublished(0, address.String())
	require.False(t, ok)
	s.Publish(11)
	_, ok = s.GetPublished(0, address.String())
	require.False(t, ok, "failed cleanup cannot republish the discarded suffix")
}

func TestVerifiedPublicationOrdersCapturedSlots(t *testing.T) {
	s, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	address := solana.PublicKey{1}
	for _, slot := range []uint64{13, 11, 12} {
		require.NoError(t, s.RecordBlock(&b.Block{Slot: slot, Epoch: 1, Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: int64(slot), RewardType: rpc.RewardTypeVoting,
		}}}))
	}
	s.Publish(12)
	reward, ok := s.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, uint64(12), reward.Amount)
	s.Publish(13)
	reward, ok = s.GetPublished(0, address.String())
	require.True(t, ok)
	require.Equal(t, uint64(13), reward.Amount)
}

func TestVerifiedCoverageRetentionFollowsDurableCheckpoint(t *testing.T) {
	s, err := Open(t.TempDir(), 10)
	require.NoError(t, err)
	s.RecordCoverage(0, 1, true, true)
	s.Publish(1)
	require.NoError(t, s.Prepare(1))
	require.NoError(t, s.SetRooted(1))
	s.Publish(20)
	require.True(t, s.EpochPublished(0), "read publication does not prune retained coverage")
	require.NoError(t, s.SetRooted(20))
	require.False(t, s.EpochPublished(0), "durable pruning invalidates old coverage")
}

func BenchmarkVerifiedRewardPublication(bm *testing.B) {
	for _, count := range []int{1000, 100000} {
		bm.Run(fmt.Sprintf("steady/%d", count), func(bm *testing.B) {
			s, err := Open(bm.TempDir(), 1000000)
			require.NoError(bm, err)
			for i := 0; i < count; i++ {
				key := rewardKey{epoch: 0, address: fmt.Sprint(i)}
				s.published[key] = Record{Epoch: 0, Address: key.address, EffectiveSlot: 1}
			}
			bm.ReportAllocs()
			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				s.Publish(uint64(i + 2))
			}
		})
		bm.Run(fmt.Sprintf("boundary/%d", count), func(bm *testing.B) {
			s, err := Open(bm.TempDir(), 1000000)
			require.NoError(bm, err)
			records := make([]Record, count)
			for i := range records {
				records[i] = Record{Epoch: 0, Address: fmt.Sprint(i), EffectiveSlot: 1}
			}
			bm.ReportAllocs()
			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				bm.StopTimer()
				s.readable = 0
				s.published = make(map[rewardKey]Record)
				s.unpublished[1] = records
				bm.StartTimer()
				s.Publish(1)
			}
		})
	}
}
