package rewards

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestDelayedCommissionUsesPersistedHistoryInAgaveOrder(t *testing.T) {
	const epoch = 1058
	key := solana.MustPublicKeyFromBase58("73zB51M61WsAUZZuKYGStsn1bBhBoEEoQmXLznJphJBo")
	current := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{InflationRewardsCommissionBps: 5000}}
	for _, tc := range []struct {
		name         string
		older, newer *uint16
		want         uint16
	}{
		{"epoch 1058 history predates current zero", new(uint16(10000)), new(uint16(8000)), 10000},
		{"explicit historical zero", new(uint16(0)), new(uint16(8000)), 0},
		{"new voter falls back one generation", nil, new(uint16(1234)), 1234},
		{"voter absent from both generations", nil, nil, 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, bps := range []*uint16{tc.older, tc.newer} {
				e := uint64(epoch + i)
				global.ClearEpochStakes(e)
				t.Cleanup(func() { global.ClearEpochStakes(e) })
				votes := map[solana.PublicKey]*epochstakes.VoteAccount{}
				if bps != nil {
					votes[key] = &epochstakes.VoteAccount{InflationRewardsCommissionBps: bps}
				}
				cache := epochstakes.NewEpochStakesCache()
				cache.PutEpoch(e, map[solana.PublicKey]uint64{key: 42}, votes, 42)
				raw, err := cache.SerializeEpoch(e)
				require.NoError(t, err)
				_, err = global.DeserializeAndLoadEpochStakes(raw)
				require.NoError(t, err)
			}
			if tc.want == 10000 {
				current.V4.InflationRewardsCommissionBps = 0
			} else {
				current.V4.InflationRewardsCommissionBps = 5000
			}
			got, err := buildDelayedCommissionVoteStateCache(map[solana.PublicKey]*sealevel.VoteStateVersions{key: current}, epoch)
			require.NoError(t, err)
			require.Equal(t, tc.want, got[key].InflationRewardsCommissionBps())
			if tc.want == 10000 {
				// Actual lost voting reward at slot 451964256, epoch 1058 -> 1059.
				split := voteCommissionSplit(got[key], 350150707774, true, true)
				require.Equal(t, uint64(350150707774), split.VoterPortion)
				require.Zero(t, split.StakerPortion)
				require.Zero(t, current.V4.InflationRewardsCommissionBps, "selection must not mutate live vote state")
			}
		})
	}
}

func TestDelayedCommissionRejectsUnknownHistoricalRate(t *testing.T) {
	const epoch = 91020
	key := solana.PublicKey{21}
	for _, missing := range []*epochstakes.VoteAccount{nil, {}} {
		global.PutEpochStakes(epoch, map[solana.PublicKey]uint64{key: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{key: missing}, 1)
		global.PutEpochStakes(epoch+1, map[solana.PublicKey]uint64{key: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{key: {InflationRewardsCommissionBps: new(uint16(1234))}}, 1)
		_, err := buildDelayedCommissionVoteStateCache(map[solana.PublicKey]*sealevel.VoteStateVersions{key: {Type: sealevel.VoteStateVersionV4}}, epoch)
		require.ErrorContains(t, err, "missing historical inflation commission")
	}
	t.Cleanup(func() { global.ClearEpochStakes(epoch); global.ClearEpochStakes(epoch + 1) })
}
