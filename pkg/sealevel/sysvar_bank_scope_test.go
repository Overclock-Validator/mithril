package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

type countingSysvarAccounts struct {
	accounts.Accounts
	getCalls int
}

func (accts *countingSysvarAccounts) GetAccount(pubkey *[32]byte) (*accounts.Account, error) {
	accts.getCalls++
	return accts.Accounts.GetAccount(pubkey)
}

func TestReadClockSysvarPrefersBankLocalAccount(t *testing.T) {
	previous := SysvarCache.Clock
	t.Cleanup(func() { SysvarCache.Clock = previous })

	parentClock := SysvarClock{Slot: 41, Epoch: 2, UnixTimestamp: 1_700_000_000}
	parentAccount := clockSysvarTestAccount(parentClock)
	SysvarCache.Clock.Sysvar = &parentClock
	SysvarCache.Clock.Acct = parentAccount

	bankClock := SysvarClock{Slot: 42, Epoch: 2, UnixTimestamp: 1_700_000_000}
	snapshot, err := NewBankSysvars(42, clockSysvarTestAccount(bankClock))
	require.NoError(t, err)
	slotCtx := &SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts(), Replay: false}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))
	execCtx := &ExecutionCtx{Accounts: accounts.NewMemAccounts(), SlotCtx: slotCtx}

	got, err := ReadClockSysvar(execCtx)
	require.NoError(t, err)
	require.Equal(t, bankClock, got)
}

func TestGetSysvarBytesPrefersBankLocalAccount(t *testing.T) {
	previous := SysvarCache.Clock
	t.Cleanup(func() { SysvarCache.Clock = previous })

	parentClock := SysvarClock{Slot: 41, Epoch: 2, UnixTimestamp: 1_700_000_000}
	SysvarCache.Clock.Sysvar = &parentClock
	SysvarCache.Clock.Acct = clockSysvarTestAccount(parentClock)

	bankClock := SysvarClock{Slot: 42, Epoch: 2, UnixTimestamp: 1_700_000_000}
	snapshot, err := NewBankSysvars(42, clockSysvarTestAccount(bankClock))
	require.NoError(t, err)
	slotCtx := &SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts(), Replay: false}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))
	execCtx := &ExecutionCtx{Accounts: accounts.NewMemAccounts(), SlotCtx: slotCtx}

	got, err := fetchSysvarBytesForPubkey(execCtx, SysvarClockAddr)
	require.NoError(t, err)
	require.Equal(t, bankClock.MustMarshal(), got)
}

func TestReadClockSysvarFallsBackWhenBankHasNoClock(t *testing.T) {
	previous := SysvarCache.Clock
	t.Cleanup(func() { SysvarCache.Clock = previous })

	cachedClock := SysvarClock{Slot: 41, Epoch: 2, UnixTimestamp: 1_700_000_000}
	SysvarCache.Clock.Sysvar = &cachedClock
	SysvarCache.Clock.Acct = clockSysvarTestAccount(cachedClock)

	got, err := ReadClockSysvar(&ExecutionCtx{
		SlotCtx: &SlotCtx{Accounts: accounts.NewMemAccounts(), Replay: false},
	})
	require.NoError(t, err)
	require.Equal(t, cachedClock, got)

	got, err = ReadClockSysvar(nil)
	require.NoError(t, err)
	require.Equal(t, cachedClock, got)
}

func TestReadSlotHashesSysvarPrefersBankLocalAccount(t *testing.T) {
	previous := SysvarCache.SlotHashes
	t.Cleanup(func() { SysvarCache.SlotHashes = previous })

	parentSlotHashes := SysvarSlotHashes{{Slot: 41, Hash: [32]byte{41}}}
	SysvarCache.SlotHashes.Sysvar = &parentSlotHashes
	SysvarCache.SlotHashes.Acct = slotHashesSysvarTestAccount(parentSlotHashes)

	bankSlotHashes := SysvarSlotHashes{{Slot: 42, Hash: [32]byte{42}}}
	snapshot, err := NewBankSysvars(42, slotHashesSysvarTestAccount(bankSlotHashes))
	require.NoError(t, err)
	slotCtx := &SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts(), Replay: false}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))

	got, err := ReadSlotHashesSysvar(&ExecutionCtx{Accounts: accounts.NewMemAccounts(), SlotCtx: slotCtx})
	require.NoError(t, err)
	require.Equal(t, bankSlotHashes, got)
}

func TestBankSysvarCacheAvoidsRepeatedAccountReads(t *testing.T) {
	previousClock := SysvarCache.Clock
	previousSlotHashes := SysvarCache.SlotHashes
	t.Cleanup(func() {
		SysvarCache.Clock = previousClock
		SysvarCache.SlotHashes = previousSlotHashes
	})

	parentClock := SysvarClock{Slot: 41}
	parentSlotHashes := SysvarSlotHashes{{Slot: 41, Hash: [32]byte{41}}}
	SysvarCache.Clock.Sysvar = &parentClock
	SysvarCache.Clock.Acct = clockSysvarTestAccount(parentClock)
	SysvarCache.SlotHashes.Sysvar = &parentSlotHashes
	SysvarCache.SlotHashes.Acct = slotHashesSysvarTestAccount(parentSlotHashes)

	bankClock := SysvarClock{Slot: 42, Epoch: 2, UnixTimestamp: 1_700_000_000}
	bankSlotHashes := SysvarSlotHashes{{Slot: 42, Hash: [32]byte{42}}}
	expectedSlotHashes := append(SysvarSlotHashes(nil), bankSlotHashes...)
	expectedSlotHashesRaw := expectedSlotHashes.MustMarshal()
	counting := &countingSysvarAccounts{Accounts: accounts.NewMemAccounts()}
	clockAccount := clockSysvarTestAccount(bankClock)
	slotHashesAccount := slotHashesSysvarTestAccount(bankSlotHashes)
	snapshot, err := NewBankSysvars(42, clockAccount, slotHashesAccount)
	require.NoError(t, err)
	slotCtx := &SlotCtx{Slot: 42, Accounts: counting}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))
	execCtx := &ExecutionCtx{SlotCtx: slotCtx}

	// Snapshot construction owns defensive copies; later caller mutation cannot change the
	// bank's immutable view.
	clockAccount.Data[0]++
	slotHashesAccount.Data[0]++
	bankSlotHashes[0].Slot++

	for range 10 {
		gotClock, err := ReadClockSysvar(execCtx)
		require.NoError(t, err)
		require.Equal(t, bankClock, gotClock)

		gotSlotHashes, err := ReadSlotHashesSysvar(execCtx)
		require.NoError(t, err)
		require.Equal(t, expectedSlotHashes, gotSlotHashes)

		gotClockRaw, err := fetchSysvarBytesForPubkey(execCtx, SysvarClockAddr)
		require.NoError(t, err)
		require.Equal(t, bankClock.MustMarshal(), gotClockRaw)

		gotSlotHashesRaw, err := fetchSysvarBytesForPubkey(execCtx, SysvarSlotHashesAddr)
		require.NoError(t, err)
		require.Equal(t, expectedSlotHashesRaw, gotSlotHashesRaw)
	}
	require.Zero(t, counting.getCalls)

	allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := ReadClockSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := ReadSlotHashesSysvar(execCtx); err != nil {
			panic(err)
		}
		if _, err := fetchSysvarBytesForPubkey(execCtx, SysvarClockAddr); err != nil {
			panic(err)
		}
		if _, err := fetchSysvarBytesForPubkey(execCtx, SysvarSlotHashesAddr); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs)
}

func clockSysvarTestAccount(clock SysvarClock) *accounts.Account {
	return &accounts.Account{
		Key:      SysvarClockAddr,
		Lamports: 1,
		Data:     clock.MustMarshal(),
	}
}

func slotHashesSysvarTestAccount(slotHashes SysvarSlotHashes) *accounts.Account {
	return &accounts.Account{
		Key:      SysvarSlotHashesAddr,
		Lamports: 1,
		Data:     slotHashes.MustMarshal(),
	}
}
