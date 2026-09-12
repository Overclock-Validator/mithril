package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

// newClockSlotCtx returns a SlotCtx holding a Clock sysvar account seeded with
// the given clock, and the schedule used by the footer update.
func newClockSlotCtx(t *testing.T, clock sealevel.SysvarClock) *sealevel.SlotCtx {
	t.Helper()
	mem := accounts.NewMemAccounts()
	acct := &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     clock.MustMarshal(),
	}
	slotCtx := &sealevel.SlotCtx{Accounts: mem}
	require.NoError(t, slotCtx.SetAccount(sealevel.SysvarClockAddr, acct))
	bankSysvars, err := sealevel.NewBankSysvars(slotCtx.Slot, acct)
	require.NoError(t, err)
	require.NoError(t, slotCtx.PublishBankSysvars(bankSysvars))
	return slotCtx
}

// snapshotClockCache saves & restores the global Clock sysvar cache so the test
// does not leak into other package tests.
func snapshotClockCache(t *testing.T) {
	t.Helper()
	prev := sealevel.SysvarCache.Clock
	t.Cleanup(func() { sealevel.SysvarCache.Clock.Sysvar, sealevel.SysvarCache.Clock.Acct = prev.Sysvar, prev.Acct })
}

func TestApplyAlpenglowFooterClockNoOpWhenNoFooterTimestamp(t *testing.T) {
	snapshotClockCache(t)
	sealevel.SysvarCache.Clock.Sysvar = nil
	sealevel.SysvarCache.Clock.Acct = nil

	clock := sealevel.SysvarClock{Slot: 100, Epoch: 5, LeaderScheduleEpoch: 6, EpochStartTimestamp: 1000, UnixTimestamp: 1050}
	slotCtx := newClockSlotCtx(t, clock)
	before, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)

	blk := &block.Block{Slot: 101, ParentSlot: 100, Epoch: 5, UnixTimestamp: 0}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}

	require.NoError(t, applyAlpenglowFooterClock(slotCtx, blk, sched))

	// Cache untouched and stored account bytes unchanged.
	require.Nil(t, sealevel.SysvarCache.Clock.Sysvar)
	require.Nil(t, sealevel.SysvarCache.Clock.Acct)
	after, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)
	require.Equal(t, before.Data, after.Data)
}

func TestApplyAlpenglowFooterClockRewritesClockFromFooter(t *testing.T) {
	snapshotClockCache(t)

	clock := sealevel.SysvarClock{Slot: 100, Epoch: 5, LeaderScheduleEpoch: 6, EpochStartTimestamp: 1000, UnixTimestamp: 1050}
	slotCtx := newClockSlotCtx(t, clock)

	// Same epoch as parent => EpochStartTimestamp preserved; footer time applied.
	blk := &block.Block{Slot: 2160010, ParentSlot: 2160009, Epoch: 5, UnixTimestamp: 1779999999}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}

	require.NoError(t, applyAlpenglowFooterClock(slotCtx, blk, sched))

	// SysvarCache.Clock updated with footer-derived clock.
	require.NotNil(t, sealevel.SysvarCache.Clock.Sysvar)
	require.Equal(t, blk.Slot, sealevel.SysvarCache.Clock.Sysvar.Slot)
	require.Equal(t, blk.Epoch, sealevel.SysvarCache.Clock.Sysvar.Epoch)
	require.Equal(t, blk.UnixTimestamp, sealevel.SysvarCache.Clock.Sysvar.UnixTimestamp)
	require.Equal(t, int64(1000), sealevel.SysvarCache.Clock.Sysvar.EpochStartTimestamp)

	// The cached account bytes decode to the same footer clock.
	require.NotNil(t, sealevel.SysvarCache.Clock.Acct)
	var decoded sealevel.SysvarClock
	require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(sealevel.SysvarCache.Clock.Acct.Data)))
	require.Equal(t, blk.UnixTimestamp, decoded.UnixTimestamp)
	require.Equal(t, blk.Slot, decoded.Slot)
}

func TestApplyAlpenglowFooterClockPersistsFooterNanosTimestamp(t *testing.T) {
	snapshotClockCache(t)

	clock := sealevel.SysvarClock{Slot: 100, Epoch: 5, LeaderScheduleEpoch: 6, EpochStartTimestamp: 1000, UnixTimestamp: 1050}
	slotCtx := newClockSlotCtx(t, clock)
	blk := &block.Block{
		Slot:                    2160010,
		ParentSlot:              2160009,
		Epoch:                   5,
		FooterProducerTimeNanos: 1779999999_987654321,
	}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}

	require.NoError(t, applyAlpenglowFooterClock(slotCtx, blk, sched))

	require.NotNil(t, sealevel.SysvarCache.Clock.Sysvar)
	require.Equal(t, int64(1779999999), sealevel.SysvarCache.Clock.Sysvar.UnixTimestamp)

	stored, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)
	var decoded sealevel.SysvarClock
	require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(stored.Data)))
	require.Equal(t, int64(1779999999), decoded.UnixTimestamp)
	require.Equal(t, blk.Slot, decoded.Slot)
}

func TestApplyAlpenglowFooterClockLocalDoesNotPublishSpeculativeClock(t *testing.T) {
	snapshotClockCache(t)

	parent := sealevel.SysvarClock{Slot: 100, Epoch: 5, LeaderScheduleEpoch: 6, EpochStartTimestamp: 1000, UnixTimestamp: 1050}
	parentAcct := &accounts.Account{
		Key:      sealevel.SysvarClockAddr,
		Lamports: 1,
		Data:     parent.MustMarshal(),
	}
	sealevel.SysvarCache.Clock.Sysvar = &parent
	sealevel.SysvarCache.Clock.Acct = parentAcct

	slotCtx := newClockSlotCtx(t, parent)
	blk := &block.Block{Slot: 2160010, ParentSlot: 2160009, Epoch: 5, FooterProducerTimeNanos: 1779999999_987654321}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}

	require.NoError(t, applyAlpenglowFooterClockLocal(slotCtx, blk, sched))

	// The candidate bank has the footer Clock.
	stored, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	require.NoError(t, err)
	var candidate sealevel.SysvarClock
	require.NoError(t, candidate.UnmarshalWithDecoder(bin.NewBinDecoder(stored.Data)))
	require.Equal(t, blk.Slot, candidate.Slot)
	require.Equal(t, int64(1779999999), candidate.UnixTimestamp)
	cached, err := sealevel.ReadClockSysvar(&sealevel.ExecutionCtx{SlotCtx: slotCtx})
	require.NoError(t, err)
	require.Equal(t, candidate, cached)

	// Ordered replay still sees the genuine parent Clock until it accepts the
	// produced block itself.
	require.Same(t, &parent, sealevel.SysvarCache.Clock.Sysvar)
	require.Same(t, parentAcct, sealevel.SysvarCache.Clock.Acct)
	require.Equal(t, uint64(100), sealevel.SysvarCache.Clock.Sysvar.Slot)
}
