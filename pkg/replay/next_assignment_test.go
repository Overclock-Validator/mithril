package replay

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestNextKnownAssignmentPartialSchedules(t *testing.T) {
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.SetLeaderSchedule(nil) })
	ours := solana.PublicKey{1}
	at := time.Unix(100, 0)
	epochs := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	makeSchedule := func(start uint64, slots ...uint64) *leaderschedule.LeaderSchedule {
		return leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{ours: slots}, start)
	}
	for _, tc := range []struct {
		name          string
		current, next *leaderschedule.LeaderSchedule
		want          uint64
		found         bool
	}{
		{name: "missing"},
		{name: "partial absent", current: makeSchedule(0, 2)},
		{name: "partial current", current: makeSchedule(0, 12), want: 12, found: true},
		{name: "missing current known next", next: makeSchedule(32, 3), want: 35, found: true},
		{name: "both", current: makeSchedule(0, 20), next: makeSchedule(32, 0), want: 20, found: true},
		{name: "reject outside epoch", current: makeSchedule(0, 65), next: makeSchedule(32, 34)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			global.SetLeaderSchedule(nil)
			global.SetLeaderScheduleForEpoch(0, tc.current)
			global.SetLeaderScheduleForEpoch(1, tc.next)
			s := nextKnownAssignment(at, ours, 10, epochs)
			require.Equal(t, at.UTC(), s.SampledAt)
			require.Equal(t, uint64(10), s.ReferenceSlot)
			require.Equal(t, uint64(63), s.SearchedThrough)
			require.Equal(t, tc.found, s.HasNextKnownSlot)
			require.Equal(t, tc.want, s.NextKnownSlot)
		})
	}
	// A compatibility singleton can span both epochs; candidates still need
	// exact range checks, and no result claims complete coverage.
	global.SetLeaderSchedule(makeSchedule(0, 35, 70))
	s := nextKnownAssignment(at, ours, 10, epochs)
	require.Equal(t, uint64(35), s.NextKnownSlot)
	require.True(t, s.HasNextKnownSlot)
	s = nextKnownAssignment(at, ours, 36, epochs)
	require.Equal(t, uint64(70), s.NextKnownSlot)
	require.Equal(t, uint64(95), s.SearchedThrough)
}

func TestNextKnownAssignmentReplacementRewindAndBounds(t *testing.T) {
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.SetLeaderSchedule(nil) })
	ours, other := solana.PublicKey{1}, solana.PublicKey{2}
	at := time.Unix(100, 0)
	epochs := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	global.SetLeaderScheduleForEpoch(0, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{ours: {3, 20}}, 0))
	old := nextKnownAssignment(at, ours, 10, epochs)
	require.Equal(t, uint64(20), old.NextKnownSlot)
	require.Equal(t, uint64(3), nextKnownAssignment(at, ours, 0, epochs).NextKnownSlot, "rewind must search earlier again")
	global.SetLeaderScheduleForEpoch(0, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{ours: {12}, other: {15}}, 0))
	require.Equal(t, uint64(12), nextKnownAssignment(at, ours, 10, epochs).NextKnownSlot)
	require.Equal(t, uint64(15), nextKnownAssignment(at, other, 10, epochs).NextKnownSlot)
	require.Equal(t, uint64(20), old.NextKnownSlot, "published value changed")
	global.SetLeaderScheduleForEpoch(0, nil)
	require.False(t, nextKnownAssignment(at, ours, 10, epochs).HasNextKnownSlot)
	for _, reference := range []uint64{31, 32, math.MaxUint64 - 1, math.MaxUint64} {
		s := nextKnownAssignment(at, ours, reference, epochs)
		require.Equal(t, reference, s.ReferenceSlot)
		require.GreaterOrEqual(t, s.SearchedThrough, reference)
		require.LessOrEqual(t, s.SearchedThrough-reference, uint64(63))
	}
	global.SetLeaderSchedule(leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{ours: {math.MaxUint64}}, 0))
	s := nextKnownAssignment(at, ours, math.MaxUint64, epochs)
	require.True(t, s.HasNextKnownSlot)
	require.Equal(t, uint64(math.MaxUint64), s.NextKnownSlot)
	warmup := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, FirstNormalEpoch: 2, FirstNormalSlot: 96, Warmup: true}
	require.Equal(t, uint64(95), nextKnownAssignment(at, ours, 31, warmup).SearchedThrough)
	require.Equal(t, uint64(223), nextKnownAssignment(at, ours, 32, warmup).SearchedThrough)
	require.Equal(t, NextKnownAssignment{}, nextKnownAssignment(at, ours, 1, nil))
	require.Equal(t, NextKnownAssignment{}, nextKnownAssignment(at, ours, 1, &sealevel.SysvarEpochSchedule{}))
	require.Equal(t, NextKnownAssignment{}, nextKnownAssignment(at, solana.PublicKey{}, 1, epochs))
}
