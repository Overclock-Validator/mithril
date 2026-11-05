package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func preserveAdmissionEpochs(t testing.TB, epochs ...uint64) {
	t.Helper()
	for _, epoch := range epochs {
		previous, hadPrevious := global.EpochStakesSnapshot(epoch)
		global.ClearEpochStakes(epoch)
		t.Cleanup(func() {
			global.ClearEpochStakes(epoch)
			if hadPrevious {
				global.PutEpochStakes(epoch, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
			}
		})
	}
}

func putAdmissionEpoch(epoch, stake, lamports uint64, identity, vote solana.PublicKey) uint64 {
	return global.PutEpochStakes(epoch, map[solana.PublicKey]uint64{vote: stake},
		map[solana.PublicKey]*epochstakes.VoteAccount{vote: {NodePubkey: identity, Lamports: lamports}}, stake)
}

func TestAdmissionEpochStakeFields(t *testing.T) {
	identity, vote := testKey(1), testKey(2)
	for _, tc := range []struct {
		name                         string
		known                        bool
		stakes                       map[solana.PublicKey]uint64
		metadata                     *epochstakes.VoteAccount
		member, hasMetadata, matches bool
		stake, lamports              uint64
	}{
		{name: "missing"},
		{name: "known empty", known: true},
		{name: "known absent", known: true, stakes: map[solana.PublicKey]uint64{testKey(3): 9}},
		{name: "zero stake", known: true, stakes: map[solana.PublicKey]uint64{vote: 0}},
		{name: "positive without metadata", known: true, stakes: map[solana.PublicKey]uint64{vote: 5}, member: true, stake: 5},
		{name: "matching metadata", known: true, stakes: map[solana.PublicKey]uint64{vote: 5}, metadata: &epochstakes.VoteAccount{NodePubkey: identity, Lamports: 99}, member: true, stake: 5, hasMetadata: true, matches: true, lamports: 99},
		{name: "different node", known: true, stakes: map[solana.PublicKey]uint64{vote: 5}, metadata: &epochstakes.VoteAccount{NodePubkey: testKey(3)}, member: true, stake: 5, hasMetadata: true},
		{name: "metadata without member", known: true, metadata: &epochstakes.VoteAccount{NodePubkey: identity}, hasMetadata: true, matches: true},
		{name: "maximum values", known: true, stakes: map[solana.PublicKey]uint64{vote: math.MaxUint64}, metadata: &epochstakes.VoteAccount{NodePubkey: identity, Lamports: math.MaxUint64}, member: true, stake: math.MaxUint64, hasMetadata: true, matches: true, lamports: math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := epochstakes.Snapshot{Epoch: 7, Generation: 19, Stakes: tc.stakes, TotalStake: math.MaxUint64,
				VoteAccounts: map[solana.PublicKey]*epochstakes.VoteAccount{vote: tc.metadata}}
			got := accountFactEpochStake(snapshot, tc.known, identity, vote)
			require.Equal(t, uint64(7), got.Epoch)
			require.Equal(t, uint64(19), got.Generation)
			require.Equal(t, tc.known, got.HasMaterial)
			require.Equal(t, tc.member, got.Member)
			require.Equal(t, tc.stake, got.SelectedStake)
			require.Equal(t, tc.hasMetadata, got.HasMetadata)
			require.Equal(t, tc.matches, got.NodeMatchesIdentity)
			require.Equal(t, tc.lamports, got.SelectionLamports)
			if tc.known {
				require.Equal(t, uint64(math.MaxUint64), got.TotalStake)
			} else {
				require.Zero(t, got.TotalStake)
			}
			if tc.hasMetadata {
				require.Equal(t, tc.metadata.NodePubkey, got.MetadataNodeIdentity)
			} else {
				require.True(t, got.MetadataNodeIdentity.IsZero())
			}
		})
	}
}

func TestAdmissionEpochCurrentNextAndPayerReference(t *testing.T) {
	const epoch = uint64(95001)
	preserveAdmissionEpochs(t, epoch, epoch+1, epoch+2)
	identity, vote := testKey(1), testKey(2)
	currentGeneration := putAdmissionEpoch(epoch, 7, 70, identity, vote)
	nextGeneration := global.PutEpochStakes(epoch+1, map[solana.PublicKey]uint64{}, nil, 0)
	putAdmissionEpoch(epoch+2, 9, 90, identity, vote)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}
	bank := accountFactsBank()
	bank.Epoch, bank.Slot = epoch, epoch*100+99
	got := accountFactAdmissionEpochs(bank, identity, vote, schedule)
	require.True(t, got.HasEpochs && got.HasNext && got.Current.HasMaterial && got.Next.HasMaterial)
	require.Equal(t, epoch, got.Current.Epoch)
	require.Equal(t, epoch+1, got.Next.Epoch)
	require.Equal(t, currentGeneration, got.Current.Generation)
	require.Equal(t, nextGeneration, got.Next.Generation)
	require.True(t, got.Current.Member)
	require.False(t, got.Next.Member)
	require.Equal(t, uint64(70), got.Current.SelectionLamports)
	require.True(t, got.HasPayerEpoch && got.HasPayerMembership)
	require.Equal(t, epoch*100, got.PayerReferenceSlot)
	require.Equal(t, epoch+1, got.PayerEpoch)
	require.False(t, got.PayerMember)
	for _, offset := range []uint64{0, 50, 100, 250} {
		schedule.LeaderScheduleSlotOffset = offset
		got = accountFactAdmissionEpochs(bank, identity, vote, schedule)
		require.Equal(t, schedule.LeaderScheduleEpoch(epoch*100), got.PayerEpoch)
		require.True(t, got.HasPayerEpoch)
		require.Equal(t, offset < 200, got.HasPayerMembership)
		require.Equal(t, offset < 100, got.PayerMember)
		if offset == 50 {
			require.NotEqual(t, schedule.LeaderScheduleEpoch(bank.Slot), got.PayerEpoch, "first-slot reference is not a claim about a later actual debit")
		}
	}
	global.ClearEpochStakes(epoch + 1)
	got = accountFactAdmissionEpochs(bank, identity, vote, schedule)
	require.True(t, got.HasNext)
	require.False(t, got.Next.HasMaterial, "missing set is not known nonmembership")
	// Future membership and current nonmembership can coexist.
	global.PutEpochStakes(epoch, nil, nil, 0)
	putAdmissionEpoch(epoch+1, 12, 120, identity, vote)
	schedule.LeaderScheduleSlotOffset = 100
	got = accountFactAdmissionEpochs(bank, identity, vote, schedule)
	require.True(t, got.Current.HasMaterial && got.Next.HasMaterial && got.Next.Member && got.HasPayerMembership && got.PayerMember)
	require.False(t, got.Current.Member)
}

func TestAdmissionEpochWarmupAndOverflow(t *testing.T) {
	identity, vote := testKey(1), testKey(2)
	preserveAdmissionEpochs(t, 0, 1, 3, 4, math.MaxUint64)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512, Warmup: true, FirstNormalEpoch: 4, FirstNormalSlot: 480}
	for _, slot := range []uint64{0, 31, 224, 479} {
		epoch := schedule.GetEpoch(slot)
		putAdmissionEpoch(epoch, 1, 0, identity, vote)
		putAdmissionEpoch(epoch+1, 2, 0, identity, vote)
		bank := accountFactsBank()
		bank.Epoch, bank.Slot = epoch, slot
		got := accountFactAdmissionEpochs(bank, identity, vote, schedule)
		require.True(t, got.HasEpochs && got.HasPayerEpoch && got.HasPayerMembership && got.PayerMember)
		require.Equal(t, epoch+1, got.PayerEpoch)
		require.Equal(t, schedule.FirstSlotInEpoch(epoch), got.PayerReferenceSlot)
	}
	schedule = &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}
	bank := accountFactsBank()
	bank.Epoch, bank.Slot = math.MaxUint64, math.MaxUint64
	putAdmissionEpoch(math.MaxUint64, 3, 30, identity, vote)
	putAdmissionEpoch(0, 99, 990, identity, vote)
	got := accountFactAdmissionEpochs(bank, identity, vote, schedule)
	require.True(t, got.HasEpochs && got.Current.HasMaterial && got.Current.Member && got.HasPayerEpoch && got.HasPayerMembership)
	require.Equal(t, uint64(3), got.Current.SelectedStake)
	require.False(t, got.HasNext)
	require.Equal(t, EpochStakeFacts{}, got.Next, "no wrapped epoch-zero lookup")
	require.Equal(t, uint64(math.MaxUint64), got.PayerEpoch)
	schedule.LeaderScheduleSlotOffset = 1
	got = accountFactAdmissionEpochs(bank, identity, vote, schedule)
	require.True(t, got.HasEpochs && got.Current.HasMaterial)
	require.False(t, got.HasPayerEpoch || got.HasPayerMembership || got.PayerMember)
}

func TestAdmissionEpochDisabledAndInvalidBinding(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	bank := accountFactsBank()
	for _, tc := range []struct {
		bank           *sealevel.SlotCtx
		identity, vote solana.PublicKey
		schedule       *sealevel.SysvarEpochSchedule
	}{
		{nil, testKey(1), testKey(2), schedule}, {bank, solana.PublicKey{}, testKey(2), schedule},
		{bank, testKey(1), solana.PublicKey{}, schedule}, {bank, testKey(1), testKey(2), nil},
		{bank, testKey(1), testKey(2), &sealevel.SysvarEpochSchedule{}},
		{bank, testKey(1), testKey(2), &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, FirstNormalEpoch: 1}},
	} {
		require.Equal(t, AdmissionEpochFacts{}, accountFactAdmissionEpochs(tc.bank, tc.identity, tc.vote, tc.schedule))
	}
	bank.Epoch = 1
	require.Equal(t, AdmissionEpochFacts{}, accountFactAdmissionEpochs(bank, testKey(1), testKey(2), schedule))
}
