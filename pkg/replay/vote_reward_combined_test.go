package replay

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func combinedVoteFixture(history int) *sealevel.VoteStateVersions {
	root := uint64(90)
	bls := [48]byte{42}
	v := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{
		RootSlot: &root, BlsPubkeyCompressed: &bls, PendingDelegatorRewards: 123,
		InflationRewardsCommissionBps: 1234, BlockRevenueCommissionBps: 5678,
		LastTimestamp: sealevel.BlockTimestamp{Slot: 90, Timestamp: 90},
	}}
	v.V4.AuthorizedVoters.AuthorizedVoters.Set(64, solana.PublicKey{1})
	v.V4.Votes.PushBack(sealevel.LandedVote{Latency: 3, Lockout: sealevel.VoteLockout{Slot: 95, ConfirmationCount: 2}})
	for i := 0; i < history; i++ {
		v.V4.EpochCredits = append(v.V4.EpochCredits, sealevel.EpochCredits{Epoch: uint64(i + 1), Credits: uint64((i + 1) * 1000), PrevCredits: uint64(i * 1000)})
	}
	return v
}

// Keep the old decode/write/decode/write sequence as the differential oracle.
func sequentialVoteUpdates(a *accounts.Account, reward, final uint64, rewardNs, finalNs int64, epoch, migration uint64, inflation EpochInflationState, total, stake uint64, leader *uint64) (bool, error) {
	if _, err := applyVoteRewardToAccount(a, reward, rewardNs, epoch, migration, inflation, total, stake, leader); err != nil {
		return false, err
	}
	if err := applyFinalCertToAccount(a, final, finalNs); err != nil {
		return false, err
	}
	return true, nil
}

func TestCombinedVoteUpdatesMatchSequential(t *testing.T) {
	old := sbpf.UsePool
	t.Cleanup(func() { sbpf.UsePool = old })
	inflation := EpochInflationState{MaxPossibleValidatorReward: 1_000_000, SlotsPerEpoch: 100}
	for _, pooled := range []bool{false, true} {
		sbpf.UsePool = pooled
		for _, history := range []int{0, 1, 64} {
			for _, root := range []uint64{0, 90, 200} {
				for _, slots := range [][2]uint64{{100, 99}, {100, 100}, {99, 100}, {80, 70}} {
					for _, epoch := range []uint64{64, 65} {
						for _, stake := range []uint64{0, 500} {
							t.Run(fmt.Sprintf("pool=%v/h=%d/root=%d/slots=%v/epoch=%d/stake=%d", pooled, history, root, slots, epoch, stake), func(t *testing.T) {
								v := combinedVoteFixture(history)
								if root == 0 {
									v.V4.RootSlot = nil
								} else {
									r := root
									v.V4.RootSlot = &r
								}
								// Include migration markers, epoch rollover and wrapping accumulation.
								data := make([]byte, sealevel.VoteStateV4Size)
								require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(data, v))
								data[len(data)-1] = 42
								checkCombinedVoteUpdates(t, data, slots[0], slots[1], int64(slots[0])*1e9, int64(slots[1])*1e9, epoch, 65, inflation, 1000, stake, math.MaxUint64-1)
							})
						}
					}
				}
			}
		}
	}
}

func checkCombinedVoteUpdates(t *testing.T, data []byte, reward, final uint64, rewardNs, finalNs int64, epoch, migration uint64, inflation EpochInflationState, total, stake, initialLeader uint64) {
	t.Helper()
	a, b := &accounts.Account{Data: bytes.Clone(data)}, &accounts.Account{Data: bytes.Clone(data)}
	wantLeader, gotLeader := initialLeader, initialLeader
	wantOK, wantErr := sequentialVoteUpdates(a, reward, final, rewardNs, finalNs, epoch, migration, inflation, total, stake, &wantLeader)
	gotOK, gotErr := applyVoteRewardAndFinalCertToAccount(b, reward, rewardNs, final, finalNs, epoch, migration, inflation, total, stake, &gotLeader)
	require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr))
	require.Equal(t, wantOK, gotOK)
	require.Equal(t, wantLeader, gotLeader)
	require.Equal(t, a, b, "including intermediate writes on error")
}

func TestCombinedVoteUpdatesErrors(t *testing.T) {
	inflation := EpochInflationState{MaxPossibleValidatorReward: 1_000_000, SlotsPerEpoch: 100}
	v := combinedVoteFixture(1)
	v.V4.RootSlot = nil
	data, err := sealevel.MarshalVersionedVoteState(v)
	require.NoError(t, err)
	// Epoch 1 keeps reward history the same size; the final root needs eight
	// extra bytes. Epoch 2 also grows reward history, so reward itself cannot fit.
	for _, epoch := range []uint64{1, 2} {
		for _, extra := range []int{0, 7, 8, 24, 32} {
			t.Run(fmt.Sprintf("epoch=%d/extra=%d", epoch, extra), func(t *testing.T) {
				checkCombinedVoteUpdates(t, append(bytes.Clone(data), make([]byte, extra)...), 100, 99, 100e9, 99e9, epoch, 99, inflation, 1000, 500, 17)
			})
		}
	}
	for _, version := range []uint32{sealevel.VoteStateVersionV0_23_5, sealevel.VoteStateVersionV1_14_11, sealevel.VoteStateVersionCurrent} {
		data, err := sealevel.MarshalVersionedVoteState(&sealevel.VoteStateVersions{Type: version})
		require.NoError(t, err)
		checkCombinedVoteUpdates(t, data, 100, 99, 100e9, 99e9, 2, 99, inflation, 1000, 500, 17)
	}
	for _, data := range [][]byte{nil, {99, 0, 0, 0}, {3, 0, 0, 0}, data[:len(data)-1]} {
		checkCombinedVoteUpdates(t, data, 100, 99, 100e9, 99e9, 2, 99, inflation, 1000, 500, 17)
	}
}

func BenchmarkVoteAccountUpdates(b *testing.B) {
	old := sbpf.UsePool
	sbpf.UsePool = true
	b.Cleanup(func() { sbpf.UsePool = old })
	for _, tc := range []struct {
		name           string
		count, overlap int
	}{{"reward", 1, 0}, {"overlap", 1, 1}, {"block656_overlap433", 656, 433}} {
		b.Run(tc.name, func(b *testing.B) {
			v := combinedVoteFixture(64)
			start := make([]byte, sealevel.VoteStateV4Size)
			if err := sealevel.WriteVersionedVoteStateInPlace(start, v); err != nil {
				b.Fatal(err)
			}
			inflation := EpochInflationState{MaxPossibleValidatorReward: 1_000_000, SlotsPerEpoch: 100}
			accts := make([]accounts.Account, tc.count)
			want := make([][]byte, tc.count)
			var wantLeader uint64
			for j := range accts {
				accts[j].Data = bytes.Clone(start)
				a := accounts.Account{Data: bytes.Clone(start)}
				var err error
				if j < tc.overlap {
					_, err = sequentialVoteUpdates(&a, 100, 99, 100e9, 99e9, 64, 60, inflation, 1000, 500, &wantLeader)
				} else {
					_, err = applyVoteRewardToAccount(&a, 100, 100e9, 64, 60, inflation, 1000, 500, &wantLeader)
				}
				if err != nil {
					b.Fatal(err)
				}
				want[j] = a.Data
			}
			var leader uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leader = 0
				for j := range accts {
					copy(accts[j].Data, start) // Identical state; same reset cost in all arms.
					var err error
					if j < tc.overlap {
						_, err = applyVoteRewardAndFinalCertToAccount(&accts[j], 100, 100e9, 99, 99e9, 64, 60, inflation, 1000, 500, &leader)
					} else {
						_, err = applyVoteRewardToAccount(&accts[j], 100, 100e9, 64, 60, inflation, 1000, 500, &leader)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			if leader != wantLeader {
				b.Fatal("leader reward changed")
			}
			for j := range accts {
				if !bytes.Equal(accts[j].Data, want[j]) {
					b.Fatal("account bytes changed")
				}
			}
		})
	}
}
