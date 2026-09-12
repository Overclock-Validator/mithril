package sealevel

import (
	"bytes"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type bankSysvarFixture struct {
	snapshot *BankSysvars
	accounts []*accounts.Account
	raw      map[solana.PublicKey][]byte

	clock             SysvarClock
	rent              SysvarRent
	epochSchedule     SysvarEpochSchedule
	epochRewards      SysvarEpochRewards
	slotHashes        SysvarSlotHashes
	stakeHistory      SysvarStakeHistory
	lastRestartSlot   SysvarLastRestartSlot
	recentBlockhashes SysvarRecentBlockhashes
	slotHistory       SysvarSlotHistory
	fees              SysvarFees
}

func newBankSysvarFixture(t *testing.T, slot uint64) bankSysvarFixture {
	t.Helper()
	f := bankSysvarFixture{
		clock: SysvarClock{
			Slot: slot, EpochStartTimestamp: 1_700_000_000, Epoch: 7,
			LeaderScheduleEpoch: 8, UnixTimestamp: 1_700_000_123,
		},
		rent: SysvarRent{
			LamportsPerUint8Year: 3_480, ExemptionThreshold: 2, BurnPercent: 50,
		},
		epochSchedule: SysvarEpochSchedule{
			SlotsPerEpoch: 432_000, LeaderScheduleSlotOffset: 432_000,
			Warmup: false, FirstNormalEpoch: 0, FirstNormalSlot: 0,
		},
		epochRewards: SysvarEpochRewards{
			DistributionStartingBlockHeight: 100, NumPartitions: 3,
			ParentBlockhash: [32]byte{9}, TotalPoints: wide.Uint128{Lo: 12, Hi: 34},
			TotalRewards: 56, DistributedRewards: 7, Active: true,
		},
		slotHashes: SysvarSlotHashes{
			{Slot: slot - 1, Hash: [32]byte{1}},
			{Slot: slot - 2, Hash: [32]byte{2}},
		},
		stakeHistory: SysvarStakeHistory{
			{Epoch: 7, Entry: StakeHistoryEntry{Effective: 10, Activating: 2, Deactivating: 1}},
		},
		lastRestartSlot: SysvarLastRestartSlot{LastRestartSlot: slot - 10},
		recentBlockhashes: SysvarRecentBlockhashes{
			{Blockhash: [32]byte{3}, FeeCalculator: FeeCalculator{LamportsPerSignature: 5_000}},
			{Blockhash: [32]byte{4}, FeeCalculator: FeeCalculator{LamportsPerSignature: 4_999}},
		},
		slotHistory: SysvarSlotHistory{
			Bits: SlotHistoryBitvec{
				Bits: SlotHistoryInner{BlocksLen: 2, Blocks: []uint64{0x11, 0x22}},
				Len:  128,
			},
			NextSlot: slot,
		},
		fees: SysvarFees{FeeCalculator: FeeCalculator{LamportsPerSignature: 5_000}},
		raw:  make(map[solana.PublicKey][]byte, bankSysvarCount),
	}

	values := []struct {
		id    bankSysvarID
		value any
	}{
		{bankSysvarClock, &f.clock},
		{bankSysvarRent, &f.rent},
		{bankSysvarEpochSchedule, &f.epochSchedule},
		{bankSysvarEpochRewards, &f.epochRewards},
		{bankSysvarSlotHashes, &f.slotHashes},
		{bankSysvarStakeHistory, &f.stakeHistory},
		{bankSysvarLastRestartSlot, &f.lastRestartSlot},
		{bankSysvarRecentBlockhashes, &f.recentBlockhashes},
		{bankSysvarSlotHistory, &f.slotHistory},
		{bankSysvarFees, &f.fees},
	}
	for _, item := range values {
		data, err := marshalLegacySysvarValue(item.id, item.value)
		require.NoError(t, err)
		key := bankSysvarAddress(item.id)
		f.raw[key] = bytes.Clone(data)
		f.accounts = append(f.accounts, &accounts.Account{Key: key, Lamports: 1, Data: data})
	}

	var err error
	f.snapshot, err = NewBankSysvars(slot, f.accounts...)
	require.NoError(t, err)
	return f
}

func TestBankSysvarsAllTypedAndRawReadsAreBankLocal(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)
	require.NoError(t, fixture.snapshot.ValidateForExecution())
	previous := SysvarCache
	SysvarCache = sysvarCache{}
	t.Cleanup(func() { SysvarCache = previous })

	counting := &countingSysvarAccounts{Accounts: accounts.NewMemAccounts()}
	slotCtx := &SlotCtx{Slot: 42, Accounts: counting}
	require.NoError(t, slotCtx.PublishBankSysvars(fixture.snapshot))
	execCtx := &ExecutionCtx{SlotCtx: slotCtx}

	clock, err := ReadClockSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.clock, clock)
	rent, err := ReadRentSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.rent, rent)
	epochSchedule, err := ReadEpochScheduleSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.epochSchedule, epochSchedule)
	epochRewards, err := ReadEpochRewardsSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.epochRewards, epochRewards)
	slotHashes, err := ReadSlotHashesSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.slotHashes, slotHashes)
	stakeHistory, err := ReadStakeHistorySysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.stakeHistory, stakeHistory)
	lastRestartSlot, err := ReadLastRestartSlotSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.lastRestartSlot, lastRestartSlot)
	recentBlockhashes, err := ReadRecentBlockHashesSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, fixture.recentBlockhashes, recentBlockhashes)
	require.Equal(t, fixture.slotHistory, ReadSlotHistorySysvar(execCtx))
	require.Equal(t, fixture.fees, ReadFeesSysvar(execCtx))

	for key, expected := range fixture.raw {
		got, ok := fixture.snapshot.RawView(key)
		require.True(t, ok)
		require.Equal(t, expected, got)
	}
	for _, key := range permittedSysvarAddrs {
		got, err := fetchSysvarBytesForPubkey(execCtx, key)
		require.NoError(t, err)
		require.Equal(t, fixture.raw[key], got)
	}
	require.Zero(t, counting.getCalls)
}

func TestLegacyLocalSysvarAccountsBeatProcessGlobalCache(t *testing.T) {
	local := newBankSysvarFixture(t, 42)
	conflicting := newBankSysvarFixture(t, 99)
	conflicting.rent.LamportsPerUint8Year++
	conflicting.epochSchedule.SlotsPerEpoch++
	conflicting.epochRewards.DistributedRewards++
	conflicting.stakeHistory[0].Entry.Effective++
	conflicting.recentBlockhashes[0].FeeCalculator.LamportsPerSignature++
	conflicting.slotHistory.NextSlot++
	conflicting.fees.FeeCalculator.LamportsPerSignature++
	previous := SysvarCache
	SysvarCache.Clock.Sysvar = &conflicting.clock
	SysvarCache.Rent.Sysvar = &conflicting.rent
	SysvarCache.EpochSchedule.Sysvar = &conflicting.epochSchedule
	SysvarCache.EpochRewards.Sysvar = &conflicting.epochRewards
	SysvarCache.SlotHashes.Sysvar = &conflicting.slotHashes
	SysvarCache.StakeHistory.Sysvar = &conflicting.stakeHistory
	SysvarCache.LastRestartSlot.Sysvar = &conflicting.lastRestartSlot
	SysvarCache.RecentBlockHashes.Sysvar = &conflicting.recentBlockhashes
	SysvarCache.SlotHistory.Sysvar = &conflicting.slotHistory
	SysvarCache.Fees.Sysvar = &conflicting.fees
	t.Cleanup(func() { SysvarCache = previous })

	mem := accounts.NewMemAccounts()
	for _, acct := range local.accounts {
		require.NoError(t, mem.SetAccountWithoutLock(acct.Key, acct.Clone()))
	}
	execCtx := &ExecutionCtx{SlotCtx: &SlotCtx{Slot: 42, Accounts: mem}}

	clock, err := ReadClockSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.clock, clock)
	rent, err := ReadRentSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.rent, rent)
	schedule, err := ReadEpochScheduleSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.epochSchedule, schedule)
	rewards, err := ReadEpochRewardsSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.epochRewards, rewards)
	hashes, err := ReadSlotHashesSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.slotHashes, hashes)
	stakeHistory, err := ReadStakeHistorySysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.stakeHistory, stakeHistory)
	lastRestart, err := ReadLastRestartSlotSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.lastRestartSlot, lastRestart)
	recent, err := ReadRecentBlockHashesSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, local.recentBlockhashes, recent)
	require.Equal(t, local.slotHistory, ReadSlotHistorySysvar(execCtx))
	require.Equal(t, local.fees, ReadFeesSysvar(execCtx))
}

func TestBankSysvarsValidateForExecutionFailsClosedOnMissingRequiredSysvar(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)

	withoutRent := fixture.snapshot.Without(SysvarRentAddr)
	require.ErrorContains(t, withoutRent.ValidateForExecution(), solana.PublicKey(SysvarRentAddr).String())

	// Feature/lifecycle-dependent entries remain optional.
	withoutOptional := fixture.snapshot.Without(SysvarEpochRewardsAddr, SysvarFeesAddr)
	require.NoError(t, withoutOptional.ValidateForExecution())
}

func TestBankSysvarsHotReadsDoNotAllocateOrReadAccounts(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)
	counting := &countingSysvarAccounts{Accounts: accounts.NewMemAccounts()}
	slotCtx := &SlotCtx{Slot: 42, Accounts: counting}
	require.NoError(t, slotCtx.PublishBankSysvars(fixture.snapshot))
	execCtx := &ExecutionCtx{SlotCtx: slotCtx}

	allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := ReadClockSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadRentSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadEpochScheduleSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadEpochRewardsSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadSlotHashesSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadStakeHistorySysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadLastRestartSlotSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadRecentBlockHashesSysvar(execCtx); err != nil {
			panic(err)
		}
		_ = ReadSlotHistorySysvar(execCtx)
		_ = ReadFeesSysvar(execCtx)
		for _, key := range permittedSysvarAddrs {
			if _, err := fetchSysvarBytesForPubkey(execCtx, key); err != nil {
				panic(err)
			}
		}
	})
	require.Zero(t, allocs)
	require.Zero(t, counting.getCalls)
}

func TestBankSysvarsDefensiveCopyAndCopyOnWrite(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)
	parent := fixture.snapshot

	// NewBankSysvars owns defensive copies of its inputs.
	fixture.accounts[0].Data[0]++
	parentRaw, ok := parent.RawView(SysvarClockAddr)
	require.True(t, ok)
	require.Equal(t, fixture.raw[solana.PublicKey(SysvarClockAddr)], parentRaw)

	childClock := fixture.clock
	childClock.Slot = 43
	childClockRaw := childClock.MustMarshal()
	expectedChildClockRaw := bytes.Clone(childClockRaw)
	update := &accounts.Account{Key: SysvarClockAddr, Lamports: 1, Data: childClockRaw}
	child, err := parent.Derive(43, update)
	require.NoError(t, err)
	update.Data[0]++

	gotParentClock, ok := parent.Clock()
	require.True(t, ok)
	require.Equal(t, fixture.clock, gotParentClock)
	gotChildClock, ok := child.Clock()
	require.True(t, ok)
	require.Equal(t, childClock, gotChildClock)
	childRaw, ok := child.RawView(SysvarClockAddr)
	require.True(t, ok)
	require.Equal(t, expectedChildClockRaw, childRaw)

	// Unchanged large entries are shared rather than cloned for every bank.
	parentSlotHashes, ok := parent.AccountView(SysvarSlotHashesAddr)
	require.True(t, ok)
	childSlotHashes, ok := child.AccountView(SysvarSlotHashesAddr)
	require.True(t, ok)
	require.Same(t, parentSlotHashes, childSlotHashes)
	parentClockAcct, ok := parent.AccountView(SysvarClockAddr)
	require.True(t, ok)
	childClockAcct, ok := child.AccountView(SysvarClockAddr)
	require.True(t, ok)
	require.NotSame(t, parentClockAcct, childClockAcct)

	withoutFees := child.Without(SysvarFeesAddr)
	_, ok = withoutFees.Fees()
	require.False(t, ok)
	_, ok = withoutFees.RawView(SysvarFeesAddr)
	require.False(t, ok)
	_, ok = child.Fees()
	require.True(t, ok)
}

func TestBankSysvarsSnapshotIsAuthoritativeForAccountDbReads(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)
	previousRent := SysvarCache.Rent
	SysvarCache.Rent.Sysvar = &fixture.rent
	SysvarCache.Rent.Acct = &accounts.Account{
		Key: SysvarRentAddr, Lamports: 1,
		Data: bytes.Clone(fixture.raw[solana.PublicKey(SysvarRentAddr)]),
	}
	t.Cleanup(func() { SysvarCache.Rent = previousRent })

	withoutRent := fixture.snapshot.Without(SysvarRentAddr)
	reader := &countingBankSysvarReader{acct: &accounts.Account{Key: SysvarRentAddr, Lamports: 1, Data: fixture.raw[solana.PublicKey(SysvarRentAddr)]}}
	slotCtx := &SlotCtx{Slot: 42, UnrootedRead: reader}
	require.NoError(t, slotCtx.PublishBankSysvars(withoutRent))

	_, err := slotCtx.GetAccountFromAccountsDb(SysvarRentAddr)
	require.Error(t, err)
	require.Zero(t, reader.calls)
	_, err = ReadRentSysvar(&ExecutionCtx{SlotCtx: slotCtx})
	require.ErrorIs(t, err, InstrErrUnsupportedSysvar)
	_, err = fetchSysvarBytesForPubkey(&ExecutionCtx{SlotCtx: slotCtx}, SysvarRentAddr)
	require.Error(t, err)
	require.Zero(t, reader.calls)

	clock, err := slotCtx.GetAccountFromAccountsDb(SysvarClockAddr)
	require.NoError(t, err)
	clock.Data[0]++
	original, ok := fixture.snapshot.RawView(SysvarClockAddr)
	require.True(t, ok)
	require.Equal(t, fixture.raw[solana.PublicKey(SysvarClockAddr)], original)
	require.Zero(t, reader.calls)
}

type countingBankSysvarReader struct {
	acct  *accounts.Account
	calls int
}

func (r *countingBankSysvarReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	r.calls++
	return r.acct.Clone(), nil
}

func TestSnapshotLegacySysvarCacheUsesDecodedAuthoritativeValue(t *testing.T) {
	previous := SysvarCache
	SysvarCache = sysvarCache{}
	t.Cleanup(func() { SysvarCache = previous })

	stale := SysvarClock{Slot: 40}
	authoritative := SysvarClock{Slot: 42, Epoch: 7, UnixTimestamp: 123}
	SysvarCache.Clock.Acct = &accounts.Account{Key: SysvarClockAddr, Lamports: 1, Data: stale.MustMarshal()}
	SysvarCache.Clock.Sysvar = &authoritative

	snapshot, err := SnapshotLegacySysvarCache(42, nil)
	require.NoError(t, err)
	got, ok := snapshot.Clock()
	require.True(t, ok)
	require.Equal(t, authoritative, got)
	raw, ok := snapshot.RawView(SysvarClockAddr)
	require.True(t, ok)
	require.Equal(t, authoritative.MustMarshal(), raw)
}

func TestBankSysvarsAtomicPublicationIsRaceFree(t *testing.T) {
	fixture := newBankSysvarFixture(t, 42)
	slotCtx := &SlotCtx{Slot: 42}
	require.NoError(t, slotCtx.PublishBankSysvars(fixture.snapshot))

	const readers = 8
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for range iterations {
				clock, ok := slotCtx.BankSysvars().Clock()
				if !ok || clock.Slot != 42 {
					t.Errorf("unexpected clock during publication: %+v, %t", clock, ok)
					return
				}
			}
		}()
	}
	for range iterations {
		clockAcct, ok := fixture.snapshot.CloneAccount(SysvarClockAddr)
		require.True(t, ok)
		next, err := fixture.snapshot.WithOwnedAccounts(clockAcct)
		require.NoError(t, err)
		require.NoError(t, slotCtx.PublishBankSysvars(next))
	}
	wg.Wait()
}
