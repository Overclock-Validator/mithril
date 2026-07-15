package epochstakes

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestClearAfterKeepsSelectedEpochAndLookahead(t *testing.T) {
	cache := NewEpochStakesCache()
	for epoch := uint64(6); epoch <= 9; epoch++ {
		cache.PutEntry(epoch, solana.PublicKey{byte(epoch)}, epoch, &VoteAccount{})
		cache.PutTotalEpochStake(epoch, epoch*10)
	}

	cache.ClearAfter(7)

	for _, epoch := range []uint64{6, 7} {
		if !cache.HasEpochStakes(epoch) || cache.TotalStake(epoch) == 0 || len(cache.EpochStakesAccts(epoch)) == 0 {
			t.Fatalf("epoch %d was unexpectedly cleared", epoch)
		}
	}
	for _, epoch := range []uint64{8, 9} {
		if cache.HasEpochStakes(epoch) || cache.TotalStake(epoch) != 0 || len(cache.EpochStakesAccts(epoch)) != 0 {
			t.Fatalf("epoch %d survived ClearAfter", epoch)
		}
	}
}
