package replay

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func storedHeaderAccount(t testing.TB, pending uint64, bls *[48]byte) *accounts.Account {
	t.Helper()
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{NodePubkey: testKey(3), PendingDelegatorRewards: pending, BlsPubkeyCompressed: bls}}
	data, err := sealevel.MarshalVersionedVoteState(&state)
	require.NoError(t, err)
	return &accounts.Account{Key: testKey(2), Owner: addresses.VoteProgramAddr, Data: data}
}

func TestAccountFactStoredHeaderSerializedLayouts(t *testing.T) {
	for _, layout := range commissionLayouts {
		acct := commissionAccount(t, layout.version, 1, 1234)
		var got AccountFacts
		accountFactStoredHeader(&got, acct)
		require.True(t, got.HasStoredNodeIdentity, layout.name)
		require.Equal(t, testKey(3), got.StoredNodeIdentity)
		require.Equal(t, layout.version == sealevel.VoteStateVersionV4, got.HasPendingDelegatorRewards)
		require.Equal(t, layout.version == sealevel.VoteStateVersionV4, got.HasStoredBLSRegistration)
		require.False(t, got.StoredBLSRegistrationPresent)
	}
	for _, pending := range []uint64{0, 1, math.MaxUint64} {
		for _, bls := range []*[48]byte{nil, {}, {1, 2, 3}} {
			acct := storedHeaderAccount(t, pending, bls)
			var got AccountFacts
			accountFactStoredHeader(&got, acct)
			require.Equal(t, pending, binary.LittleEndian.Uint64(acct.Data[136:144]))
			require.True(t, got.HasPendingDelegatorRewards)
			require.Equal(t, pending, got.PendingDelegatorRewards)
			require.True(t, got.HasStoredBLSRegistration)
			require.Equal(t, bls != nil, got.StoredBLSRegistrationPresent, "presence is not key validity")
			if bls != nil {
				require.Equal(t, bls[:], acct.Data[145:193])
			}
		}
	}
}

func TestAccountFactStoredHeaderMalformedAndBounded(t *testing.T) {
	for _, bls := range []*[48]byte{nil, {1}} {
		acct := storedHeaderAccount(t, math.MaxUint64, bls)
		data := acct.Data
		headerEnd := 145
		if bls != nil {
			headerEnd += 48
		}
		for n := 0; n <= headerEnd; n++ {
			acct.Data = data[:n]
			var got AccountFacts
			accountFactStoredHeader(&got, acct)
			require.Equal(t, n >= 36, got.HasStoredNodeIdentity, "length %d", n)
			require.Equal(t, n >= 144, got.HasPendingDelegatorRewards, "length %d", n)
			require.Equal(t, n >= headerEnd, got.HasStoredBLSRegistration, "length %d", n)
		}
		acct.Data = data
		binary.LittleEndian.PutUint64(data[headerEnd:headerEnd+8], math.MaxUint64)
		var got AccountFacts
		accountFactStoredHeader(&got, acct)
		require.True(t, got.HasStoredBLSRegistration, "variable vectors are not decoded")
		require.Zero(t, testing.AllocsPerRun(100, func() { var f AccountFacts; accountFactStoredHeader(&f, acct) }))
		for _, tag := range []byte{2, 127, 255} {
			acct.Data[144] = tag
			got = AccountFacts{}
			accountFactStoredHeader(&got, acct)
			require.False(t, got.HasStoredBLSRegistration)
			require.True(t, got.HasStoredNodeIdentity && got.HasPendingDelegatorRewards)
		}
	}
	for _, edit := range []func(*accounts.Account){
		func(a *accounts.Account) { a.IsDummy = true },
		func(a *accounts.Account) { a.Executable = true },
		func(a *accounts.Account) { a.Owner = solana.PublicKey{} },
		func(a *accounts.Account) { binary.LittleEndian.PutUint32(a.Data, math.MaxUint32) },
	} {
		acct := storedHeaderAccount(t, 99, &[48]byte{1})
		edit(acct)
		var got AccountFacts
		accountFactStoredHeader(&got, acct)
		require.Equal(t, AccountFacts{}, got)
	}
	var got AccountFacts
	accountFactStoredHeader(&got, nil)
	require.Equal(t, AccountFacts{}, got)
	zeroNode := storedHeaderAccount(t, 0, nil)
	clear(zeroNode.Data[4:36])
	accountFactStoredHeader(&got, zeroNode)
	require.True(t, got.HasStoredNodeIdentity)
	require.True(t, got.StoredNodeIdentity.IsZero(), "stored zero is not unknown or a valid registration claim")
}

func admissionFactsBank(t testing.TB, slot uint64, schedule *sealevel.SysvarEpochSchedule, rent sealevel.SysvarRent) *sealevel.SlotCtx {
	t.Helper()
	bank := accountFactsBank()
	bank.Slot, bank.Epoch, bank.Features = slot, schedule.GetEpoch(slot), features.NewFeaturesDefault()
	bank.Features.EnableFeature(features.Alpenglow, 0)
	bank.Features.EnableFeature(features.ValidatorAdmissionTicket, 0)
	bank.Features.EnableFeature(features.EnablePartitionedEpochReward, 0)
	sysvars, err := sealevel.NewBankSysvars(slot, &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()})
	require.NoError(t, err)
	require.NoError(t, bank.PublishBankSysvars(sysvars))
	return bank
}

func TestAdmissionFactsBankRentAndFunding(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	rent := sealevel.NewDefaultRentSysvar()
	bank := admissionFactsBank(t, 50, schedule, rent)
	const rentMinimum = uint64(27_074_400)
	const floor = rentMinimum + legacyVATBurnPerEpoch
	for _, balance := range []uint64{0, floor - 1, floor, floor + 1, math.MaxUint64} {
		got := accountFactAdmissionFunding(bank, schedule, balance, true)
		require.Equal(t, AdmissionFundingFacts{HasRules: true, VATFilteringActive: true, VATChargeGateActive: true,
			RentMinimumLamports: rentMinimum, BurnLamports: legacyVATBurnPerEpoch, FloorLamports: floor,
			HasFundingComparison: true, MeetsFloor: balance >= floor}, got)
	}
	got := accountFactAdmissionFunding(bank, schedule, 0, false)
	require.True(t, got.HasRules)
	require.False(t, got.HasFundingComparison || got.MeetsFloor)
	zeroRent := admissionFactsBank(t, 50, schedule, sealevel.SysvarRent{})
	zeroRent.Features = features.NewFeaturesDefault()
	got = accountFactAdmissionFunding(zeroRent, schedule, 0, true)
	require.True(t, got.HasRules && got.HasFundingComparison && got.MeetsFloor)
	require.Zero(t, got.FloorLamports)
	require.False(t, got.VATFilteringActive || got.VATChargeGateActive)
	// The bank-owned rent is used even when it differs from process-global rent.
	bank = admissionFactsBank(t, 50, schedule, sealevel.SysvarRent{LamportsPerUint8Year: 1, ExemptionThreshold: 1})
	require.Equal(t, uint64(sealevel.VoteStateV4Size+128), accountFactAdmissionFunding(bank, schedule, 0, true).RentMinimumLamports)
	// Only the final rent+burn addition saturates, matching the admission formula.
	rent = sealevel.SysvarRent{LamportsPerUint8Year: 1, ExemptionThreshold: (math.Exp2(64) - (1 << 30)) / (sealevel.VoteStateV4Size + 128)}
	bank = admissionFactsBank(t, 50, schedule, rent)
	got = accountFactAdmissionFunding(bank, schedule, math.MaxUint64, true)
	require.True(t, got.HasRules && got.MeetsFloor)
	require.Equal(t, uint64(math.MaxUint64), got.FloorLamports)
	require.Equal(t, rent.MinimumBalance(sealevel.VoteStateV4Size), got.RentMinimumLamports)
}

func TestAdmissionFactsFeatureBoundaries(t *testing.T) {
	schedules := []*sealevel.SysvarEpochSchedule{{SlotsPerEpoch: 100},
		{SlotsPerEpoch: 512, Warmup: true, FirstNormalEpoch: 4, FirstNormalSlot: 480}}
	for _, schedule := range schedules {
		for _, gate := range []struct {
			feature features.FeatureGate
			burn    uint64
		}{{features.ReduceSlotTimeTo350ms, vatBurn350ms}, {features.ReduceSlotTimeTo300ms, vatBurn300ms},
			{features.ReduceSlotTimeTo250ms, vatBurn250ms}, {features.ReduceSlotTimeTo200ms, vatBurn200ms}} {
			const activation = 50
			boundary := schedule.FirstSlotInEpoch(schedule.GetEpoch(activation) + 1)
			for _, slot := range []uint64{activation, boundary - 1, boundary, boundary + 1} {
				bank := admissionFactsBank(t, slot, schedule, sealevel.NewDefaultRentSysvar())
				bank.Features.EnableFeature(gate.feature, activation)
				got := accountFactAdmissionFunding(bank, schedule, 0, true)
				require.True(t, got.HasRules)
				want := legacyVATBurnPerEpoch
				if slot >= boundary {
					want = gate.burn
				}
				require.Equal(t, want, got.BurnLamports, "%s slot %d", gate.feature.Name, slot)
				require.Equal(t, alpenglowVATBurnPerEpoch(bank.Features, schedule, slot), got.BurnLamports)
			}
		}
	}
	schedule := schedules[0]
	for mask := 0; mask < 32; mask++ {
		bank := admissionFactsBank(t, 100, schedule, sealevel.NewDefaultRentSysvar())
		bank.Features = features.NewFeaturesDefault()
		for bit, gate := range []features.FeatureGate{features.Alpenglow, features.AlpenglowDevContext, features.ValidatorAdmissionTicket,
			features.EnablePartitionedEpochReward, features.EnablePartitionedEpochRewardsSuperfeature} {
			if mask&(1<<bit) != 0 {
				bank.Features.EnableFeature(gate, 0)
			}
		}
		got := accountFactAdmissionFunding(bank, schedule, 0, true)
		require.True(t, got.HasRules)
		require.Equal(t, mask&4 != 0, got.VATFilteringActive, "feature mask %d", mask)
		require.Equal(t, mask&1 != 0 && mask&4 != 0 && mask&24 != 0, got.VATChargeGateActive, "feature mask %d", mask)
		burn := uint64(0)
		if mask&3 != 0 {
			burn = legacyVATBurnPerEpoch
		}
		require.Equal(t, burn, got.BurnLamports, "feature mask %d", mask)
	}
	// Replay's schedule, not a cached or global sysvar schedule, sets the boundary.
	bank := admissionFactsBank(t, 100, schedule, sealevel.NewDefaultRentSysvar())
	bank.Features.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	require.Equal(t, vatBurn200ms, accountFactAdmissionFunding(bank, schedule, 0, true).BurnLamports)
	for _, gate := range []features.FeatureGate{features.ReduceSlotTimeTo350ms, features.ReduceSlotTimeTo300ms, features.ReduceSlotTimeTo250ms} {
		bank.Features.EnableFeature(gate, 50)
	}
	require.Equal(t, vatBurn200ms, accountFactAdmissionFunding(bank, schedule, 0, true).BurnLamports, "the runtime's ordered gates choose the final reduction")
	schedule = &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 200}
	bank.Epoch = 0
	require.Equal(t, legacyVATBurnPerEpoch, accountFactAdmissionFunding(bank, schedule, 0, true).BurnLamports)
}

func TestAdmissionFactsInvalidInputsUnknown(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	for _, bad := range []*sealevel.SysvarEpochSchedule{nil, {},
		{SlotsPerEpoch: 100, FirstNormalEpoch: 1}, {SlotsPerEpoch: 100, FirstNormalSlot: 32},
		{SlotsPerEpoch: 31, Warmup: true}, {SlotsPerEpoch: math.MaxUint64, Warmup: true},
		{SlotsPerEpoch: 512, Warmup: true, FirstNormalEpoch: 64},
		{SlotsPerEpoch: 512, Warmup: true, FirstNormalEpoch: 4, FirstNormalSlot: 481},
	} {
		bank := admissionFactsBank(t, 50, schedule, sealevel.NewDefaultRentSysvar())
		require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, bad, 0, true))
	}
	for _, rent := range []sealevel.SysvarRent{
		{ExemptionThreshold: math.NaN()}, {ExemptionThreshold: math.Inf(1)}, {ExemptionThreshold: math.Inf(-1)},
		{ExemptionThreshold: -1}, {ExemptionThreshold: 1, BurnPercent: 101},
		{LamportsPerUint8Year: math.MaxUint64, ExemptionThreshold: 0},
		{LamportsPerUint8Year: 1, ExemptionThreshold: math.Nextafter(math.Exp2(64)/(sealevel.VoteStateV4Size+128), math.Inf(1))},
		{LamportsPerUint8Year: 1, ExemptionThreshold: math.Exp2(64)},
		{LamportsPerUint8Year: 1, ExemptionThreshold: math.MaxFloat64},
	} {
		bank := admissionFactsBank(t, 50, schedule, rent)
		require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true))
	}
	bank := admissionFactsBank(t, 50, schedule, sealevel.NewDefaultRentSysvar())
	bank.Features = nil
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true))
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(nil, schedule, 0, true))
	bank = accountFactsBank()
	bank.Features = features.NewFeaturesDefault()
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true))
	emptySysvars, err := sealevel.NewBankSysvars(0)
	require.NoError(t, err)
	require.NoError(t, bank.PublishBankSysvars(emptySysvars))
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true))
	bank = admissionFactsBank(t, 50, schedule, sealevel.NewDefaultRentSysvar())
	bank.Epoch++
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true))
	bank.Epoch--
	bank.Slot++
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true), "bank sysvars must belong to the sampled slot")
	bank.Slot--
	bank.Features.EnableFeature(features.ReduceSlotTimeTo200ms, math.MaxUint64)
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true), "overflowing activation boundary")
	schedule = &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}
	bank = admissionFactsBank(t, math.MaxUint64, schedule, sealevel.NewDefaultRentSysvar())
	require.Equal(t, AdmissionFundingFacts{}, accountFactAdmissionFunding(bank, schedule, 0, true), "overflowing next bank boundary")
}

func TestAccountFactsAdmissionReuseAndCadence(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	for _, sameKey := range []bool{false, true} {
		bank := admissionFactsBank(t, 50, schedule, sealevel.NewDefaultRentSysvar())
		acct := storedHeaderAccount(t, math.MaxUint64, &[48]byte{1})
		acct.Lamports = math.MaxUint64
		require.NoError(t, bank.SetAccount(acct.Key, acct))
		identity := testKey(1)
		if sameKey {
			identity = acct.Key
		} else {
			require.NoError(t, bank.SetAccount(identity, testAccount(1, 0)))
		}
		store := &accountFactsStore{Accounts: bank.Accounts}
		bank.Accounts = store
		var sampler AccountFactsSampler
		now := time.Unix(100, 0)
		first, ok := sampler.sampleAt(now, bank, identity, acct.Key, schedule)
		require.True(t, ok)
		require.True(t, first.HasStoredBLSRegistration && first.StoredBLSRegistrationPresent && first.AdmissionFunding.MeetsFloor)
		reads := 2
		if sameKey {
			reads = 1
		}
		require.Equal(t, reads, store.reads)
		clear(acct.Data[136:145])
		acct.Lamports = 0
		cached, ok := sampler.sampleAt(now.Add(time.Millisecond), bank, identity, acct.Key, schedule)
		require.True(t, ok)
		require.Equal(t, first, cached)
		require.Equal(t, reads, store.reads)
		fresh, ok := sampler.sampleAt(now.Add(time.Second), bank, identity, acct.Key, schedule)
		require.True(t, ok)
		require.True(t, fresh.HasPendingDelegatorRewards && fresh.HasStoredBLSRegistration && fresh.AdmissionFunding.HasFundingComparison)
		require.Zero(t, fresh.PendingDelegatorRewards)
		require.False(t, fresh.StoredBLSRegistrationPresent || fresh.AdmissionFunding.MeetsFloor)
		require.Equal(t, 2*reads, store.reads)
		require.Empty(t, bank.ModifiedAccts)
		require.Empty(t, bank.ParentAccts.AllAccounts())
		sampler.Reset()
		require.Equal(t, AccountFactsSampler{}, sampler)
		_, ok = sampler.Sample(bank, solana.PublicKey{}, acct.Key, schedule)
		require.False(t, ok)
		require.Equal(t, 2*reads, store.reads, "disabled sampling must not read accounts")
	}
}

func TestAccountFactsAdmissionUsesSharedParentWithoutCopy(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	bank := admissionFactsBank(t, 50, schedule, sealevel.NewDefaultRentSysvar())
	acct := storedHeaderAccount(t, 123, &[48]byte{1})
	acct.Lamports = math.MaxUint64
	reader := &accountFactsSharedReader{accountFactsReader: accountFactsReader{acct: acct}}
	bank.UnrootedRead = reader
	store := &accountFactsStore{Accounts: bank.Accounts}
	bank.Accounts = store
	var sampler AccountFactsSampler
	got, ok := sampler.Sample(bank, acct.Key, acct.Key, schedule)
	require.True(t, ok)
	require.True(t, got.AdmissionFunding.HasRules && got.AdmissionFunding.MeetsFloor && got.StoredBLSRegistrationPresent)
	require.Equal(t, uint64(123), got.PendingDelegatorRewards)
	require.Equal(t, 1, store.reads)
	require.Equal(t, 1, reader.sharedReads)
	require.Zero(t, reader.reads, "no owned parent copy")
	require.Empty(t, bank.ParentAccts.AllAccounts())
	acct.Owner = solana.PublicKey{}
	sampler.Reset()
	got, ok = sampler.Sample(bank, acct.Key, acct.Key, schedule)
	require.True(t, ok)
	require.True(t, got.HasVoteBalance && got.AdmissionFunding.HasFundingComparison && got.AdmissionFunding.MeetsFloor)
	require.False(t, got.HasStoredNodeIdentity || got.HasPendingDelegatorRewards || got.HasStoredBLSRegistration,
		"a numerical funding comparison must not assert a valid vote account")
}

func BenchmarkAdmissionFacts(b *testing.B) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	bank := admissionFactsBank(b, 100, schedule, sealevel.NewDefaultRentSysvar())
	bank.Features.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	b.ReportAllocs()
	for b.Loop() {
		accountFactAdmissionFunding(bank, schedule, 0, true)
	}
}
