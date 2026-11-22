package block

import (
	"math"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestPaymentObservationDisabledAndDetached(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	for _, b := range []*Block{nil, {}} {
		b.RecordFeePayment(10, 3, identity, true)
		b.RecordAppliedVotingRewards([]rpc.BlockReward{{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: 7}})
		got, ok := b.PaymentObservation()
		require.False(t, ok)
		require.Equal(t, PaymentObservation{}, got)
	}
	b := &Block{Leader: identity, Rewards: []rpc.BlockReward{{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: 999}}}
	b.EnablePaymentObservation(identity, vote)
	initial, ok := b.PaymentObservation()
	require.True(t, ok)
	require.True(t, initial.RewardsKnown)
	require.False(t, initial.FeesKnown)
	require.Zero(t, initial.VotingRewardPaid, "enabling capture must not trust existing block rewards")
	b.RecordFeePayment(10, 3, identity, true)
	got, _ := b.PaymentObservation()
	got.Fees, got.Identity = 999, solana.PublicKey{9}
	unchanged, _ := b.PaymentObservation()
	require.Equal(t, uint64(10), unchanged.Fees)
	require.Equal(t, identity, unchanged.Identity)
	require.Zero(t, initial.Fees, "previous values must remain detached")
	b.EnablePaymentObservation(identity, vote)
	reset, _ := b.PaymentObservation()
	require.Equal(t, initial, reset)
	for _, binding := range [][2]solana.PublicKey{{{}, vote}, {identity, {}}} {
		b.EnablePaymentObservation(binding[0], binding[1])
		_, ok := b.PaymentObservation()
		require.False(t, ok)
	}
}

func TestPaymentObservationFeeArithmetic(t *testing.T) {
	identity, vote, collector := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	for _, tc := range []struct {
		name         string
		total, burn  uint64
		collector    solana.PublicKey
		valid, known bool
	}{
		{"deposit", 10, 3, collector, true, true},
		{"missing collector", 10, 3, solana.PublicKey{}, true, false},
		{"burn exceeds total", 10, 11, collector, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &Block{Leader: identity}
			b.EnablePaymentObservation(identity, vote)
			b.RecordFeePayment(100, 25, collector, true)
			b.RecordFeePayment(tc.total, tc.burn, tc.collector, tc.valid)
			got, ok := b.PaymentObservation()
			require.True(t, ok)
			require.Equal(t, tc.known, got.FeesKnown)
			require.Equal(t, identity, got.Leader)
			require.Equal(t, tc.collector, got.FeeCollector)
			if tc.known {
				require.Equal(t, tc.total, got.Fees)
				require.Equal(t, tc.burn, got.FeeBurned)
				require.Equal(t, tc.total-tc.burn, got.FeeDeposited)
			} else {
				require.Zero(t, got.Fees)
				require.Zero(t, got.FeeBurned)
				require.Zero(t, got.FeeDeposited)
			}
		})
	}
}

func TestPaymentObservationAppliedReceiptReplacement(t *testing.T) {
	identity, vote, other := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	b := &Block{Leader: identity}
	b.EnablePaymentObservation(identity, vote)
	b.RecordFeePayment(10, 3, identity, true)
	records := []rpc.BlockReward{
		{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: 7},
		{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: 2},
		{Pubkey: vote, RewardType: rpc.RewardTypeStaking, Lamports: -1},
		{Pubkey: other, RewardType: rpc.RewardTypeVoting, Lamports: -1},
	}
	b.RecordAppliedVotingRewards(records)
	got, _ := b.PaymentObservation()
	require.True(t, got.RewardsKnown)
	require.Equal(t, uint64(9), got.VotingRewardPaid)
	records[0].Lamports = 3
	unchanged, _ := b.PaymentObservation()
	require.Equal(t, got, unchanged, "capture must not retain caller-owned receipt storage")
	b.RecordAppliedVotingRewards(records[:1])
	replacement, _ := b.PaymentObservation()
	require.Equal(t, uint64(3), replacement.VotingRewardPaid, "the phase replaces rather than accumulates")
	require.Equal(t, got.Fees, replacement.Fees)
	b.RecordAppliedVotingRewards(nil)
	empty, _ := b.PaymentObservation()
	require.True(t, empty.RewardsKnown)
	require.Zero(t, empty.VotingRewardPaid)
}

func TestPaymentObservationInvalidAndOverflowReceipts(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	for _, tc := range []struct {
		name    string
		amounts []int64
		known   bool
		want    uint64
	}{
		{"negative", []int64{7, -1}, false, 0},
		{"overflow", []int64{math.MaxInt64, math.MaxInt64, 2}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &Block{}
			b.EnablePaymentObservation(identity, vote)
			b.RecordAppliedVotingRewards([]rpc.BlockReward{{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: 42}})
			var records []rpc.BlockReward
			for _, amount := range tc.amounts {
				records = append(records, rpc.BlockReward{Pubkey: vote, RewardType: rpc.RewardTypeVoting, Lamports: amount})
			}
			b.RecordAppliedVotingRewards(records)
			got, _ := b.PaymentObservation()
			require.Equal(t, tc.known, got.RewardsKnown)
			require.Equal(t, tc.want, got.VotingRewardPaid)
			b.EnablePaymentObservation(identity, vote)
			reset, _ := b.PaymentObservation()
			require.True(t, reset.RewardsKnown)
			require.Zero(t, reset.VotingRewardPaid)
		})
	}
}
