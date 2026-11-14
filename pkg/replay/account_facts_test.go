package replay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type accountFactsStore struct {
	accounts.Accounts
	reads int
}

func (s *accountFactsStore) GetAccount(key *[32]byte) (*accounts.Account, error) {
	s.reads++
	return s.Accounts.GetAccount(key)
}

type accountFactsReader struct {
	acct  *accounts.Account
	err   error
	reads int
	slot  uint64
}

func (r *accountFactsReader) GetAccount(slot uint64, _ solana.PublicKey) (*accounts.Account, error) {
	r.reads++
	r.slot = slot
	return r.acct, r.err
}

type accountFactsSharedReader struct {
	accountFactsReader
	sharedReads int
}

func (r *accountFactsSharedReader) GetAccountShared(slot uint64, _ solana.PublicKey) (*accounts.Account, error) {
	r.sharedReads++
	r.slot = slot
	return r.acct, r.err
}

func accountFactsBank() *sealevel.SlotCtx {
	return &sealevel.SlotCtx{Accounts: accounts.NewMemAccounts(), ParentAccts: accounts.NewMemAccounts(),
		Slot: 0, Epoch: 0, FinalBankhash: testHashBytes(1),
		ModifiedAccts: make(map[solana.PublicKey]bool), WritableAccts: make(map[solana.PublicKey]bool)}
}

func TestAccountFactsBankPrecedence(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o600))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	seedDurableAccounts(t, db, []*accounts.Account{testAccount(1, 10), testAccount(2, 20)}, 1)
	tail := newUnrootedTail(db, db, 512, 128, "")
	tail.Add(2, []*accounts.Account{testAccount(1, 30)}, testHashBytes(2))
	bank := accountFactsBank()
	bank.Accounts = accounts.NewOverlayAccounts(bank.Accounts)
	bank.Slot, bank.Epoch, bank.AccountsDb, bank.UnrootedRead = 3, 4, db, tail
	current := testAccount(1, 40)
	require.NoError(t, bank.SetAccount(current.Key, current))
	var sampler AccountFactsSampler
	now := time.Unix(100, 0)
	got, ok := sampler.sampleAt(now, bank, testKey(1), testKey(2), nil)
	require.True(t, ok)
	require.Equal(t, AccountFacts{SampledAt: now.UTC(), Slot: 3, Bankhash: solana.Hash(testHash(1)), Epoch: 4,
		Identity: testKey(1), VoteAccount: testKey(2), IdentityLamports: 40, VoteLamports: 20,
		HasIdentityBalance: true, HasVoteBalance: true}, got)
	require.Empty(t, bank.ModifiedAccts)
	require.Empty(t, bank.WritableAccts)
	require.Empty(t, bank.ParentAccts.AllAccounts())
	require.Equal(t, uint64(40), current.Lamports)
	require.Equal(t, 1, tail.overlay.HeldSlots())

	bank.Accounts = accounts.NewMemAccounts()
	sampler.Reset()
	got, ok = sampler.sampleAt(now, bank, testKey(1), testKey(2), nil)
	require.True(t, ok)
	require.Equal(t, uint64(30), got.IdentityLamports, "unrooted write must shadow the durable value")
	bank.UnrootedRead = nil
	sampler.Reset()
	got, ok = sampler.sampleAt(now, bank, testKey(1), testKey(2), nil)
	require.True(t, ok)
	require.Equal(t, uint64(10), got.IdentityLamports, "legacy parent read uses the database")

	sampler.Reset()
	got, ok = sampler.sampleAt(now, bank, testKey(3), testKey(4), nil)
	require.True(t, ok)
	require.False(t, got.HasIdentityBalance)
	require.False(t, got.HasVoteBalance)
}

func TestAccountFactsUnavailableAndZero(t *testing.T) {
	readErr := errors.New("account read failed")
	for _, tc := range []struct {
		name  string
		acct  *accounts.Account
		err   error
		known bool
	}{
		{name: "present zero", acct: testAccount(1, 0), known: true},
		{name: "nil"},
		{name: "missing", err: accountsdb.ErrNoAccount},
		{name: "read error", err: readErr},
		{name: "value with error", acct: testAccount(1, 9), err: readErr},
		{name: "wrong key", acct: testAccount(2, 9)},
		{name: "dummy", acct: &accounts.Account{Key: testKey(1), Lamports: 9, IsDummy: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bank := accountFactsBank()
			reader := &accountFactsReader{acct: tc.acct, err: tc.err}
			bank.UnrootedRead = reader
			var sampler AccountFactsSampler
			got, ok := sampler.sampleAt(time.Unix(100, 0), bank, testKey(1), testKey(1), nil)
			require.True(t, ok)
			require.Equal(t, tc.known, got.HasIdentityBalance)
			require.Equal(t, tc.known, got.HasVoteBalance)
			require.Zero(t, got.IdentityLamports)
			require.Zero(t, got.VoteLamports)
			require.Equal(t, 1, reader.reads, "identical configured keys resolve once")
		})
	}
	for _, current := range []*accounts.Account{nil, testAccount(2, 99), {Key: testKey(1), IsDummy: true}, testAccount(1, 0)} {
		bank := accountFactsBank()
		require.NoError(t, bank.SetAccount(testKey(1), current))
		reader := &accountFactsReader{acct: testAccount(1, 100)}
		bank.UnrootedRead = reader
		var sampler AccountFactsSampler
		got, ok := sampler.sampleAt(time.Unix(100, 0), bank, testKey(1), solana.PublicKey{}, nil)
		require.True(t, ok)
		require.Zero(t, reader.reads, "an invalid current value or tombstone must not resurrect its parent")
		require.Zero(t, got.IdentityLamports)
		require.Equal(t, current != nil && current.Key == testKey(1) && !current.IsDummy, got.HasIdentityBalance)
	}
}

func TestAccountFactsCadenceAndReset(t *testing.T) {
	bank := accountFactsBank()
	for _, key := range []byte{1, 2, 3} {
		require.NoError(t, bank.SetAccount(testKey(key), testAccount(key, uint64(key))))
	}
	store := &accountFactsStore{Accounts: bank.Accounts}
	bank.Accounts = store
	var sampler AccountFactsSampler
	now := time.Unix(100, 0)
	first, ok := sampler.sampleAt(now, bank, testKey(1), testKey(2), nil)
	require.True(t, ok)
	require.Zero(t, first.Slot, "slot zero is a valid bank")
	require.Equal(t, 2, store.reads)
	bank.Slot = 9
	cached, ok := sampler.sampleAt(now.Add(time.Second-time.Nanosecond), bank, testKey(1), testKey(2), nil)
	require.True(t, ok)
	require.Equal(t, first, cached)
	require.Equal(t, 2, store.reads)
	for _, tc := range []struct {
		name     string
		at       time.Time
		identity solana.PublicKey
		vote     solana.PublicKey
		epoch    uint64
	}{
		{"exact interval", now.Add(time.Second), testKey(1), testKey(2), 0},
		{"identity change", now.Add(time.Second), testKey(3), testKey(2), 0},
		{"vote change", now.Add(time.Second), testKey(3), testKey(1), 0},
		{"epoch change", now.Add(time.Second), testKey(3), testKey(1), 1},
		{"clock regression", now.Add(-time.Hour), testKey(3), testKey(1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bank.Epoch = tc.epoch
			before := store.reads
			got, ok := sampler.sampleAt(tc.at, bank, tc.identity, tc.vote, nil)
			require.True(t, ok)
			require.Equal(t, before+2, store.reads)
			require.Equal(t, tc.at.UTC(), got.SampledAt)
			require.Equal(t, bank.Slot, got.Slot)
		})
	}
	before := store.reads
	got, ok := sampler.sampleAt(now.Add(-time.Hour+time.Millisecond), bank, testKey(3), testKey(1), nil)
	require.True(t, ok)
	require.Equal(t, before, store.reads, "regression must re-anchor the cadence")
	require.Equal(t, now.Add(-time.Hour).UTC(), got.SampledAt)
	sampler.Reset()
	require.Equal(t, AccountFactsSampler{}, sampler)
	_, ok = sampler.sampleAt(now.Add(-time.Hour+time.Millisecond), bank, testKey(3), testKey(1), nil)
	require.True(t, ok)
	require.Equal(t, before+2, store.reads)
}

func TestAccountFactsSharedReadAndDetachedValue(t *testing.T) {
	acct := testAccount(1, 50)
	acct.Data = make([]byte, 1<<20)
	reader := &accountFactsSharedReader{accountFactsReader: accountFactsReader{acct: acct}}
	bank := accountFactsBank()
	bank.Slot, bank.UnrootedRead = 7, reader
	var sampler AccountFactsSampler
	before := time.Now()
	got, ok := sampler.Sample(bank, testKey(1), solana.PublicKey{}, nil)
	after := time.Now()
	require.True(t, ok)
	require.False(t, got.SampledAt.Before(before))
	require.False(t, got.SampledAt.After(after))
	require.Equal(t, 1, reader.sharedReads)
	require.Zero(t, reader.reads, "native immutable read avoids an owned data copy")
	require.Equal(t, uint64(7), reader.slot)
	retained := got
	acct.Lamports, bank.FinalBankhash[0] = 500, 99
	got.IdentityLamports, got.Bankhash[0] = 999, 100
	cached, ok := sampler.sampleAt(sampler.lastSample, bank, testKey(1), solana.PublicKey{}, nil)
	require.True(t, ok)
	require.Equal(t, retained, cached)
	require.Equal(t, 1, reader.sharedReads)
}

func TestAccountFactsDisabledAndInvalidBank(t *testing.T) {
	bank := accountFactsBank()
	reader := &accountFactsReader{acct: testAccount(1, 50)}
	bank.UnrootedRead = reader
	var disabled *AccountFactsSampler
	disabled.Reset()
	got, ok := disabled.Sample(bank, testKey(1), testKey(2), nil)
	require.False(t, ok)
	require.Equal(t, AccountFacts{}, got)
	var sampler AccountFactsSampler
	_, ok = sampler.Sample(bank, solana.PublicKey{}, testKey(2), nil)
	require.False(t, ok)
	require.Zero(t, reader.reads)
	for _, invalid := range []*sealevel.SlotCtx{nil, {FinalBankhash: []byte{1}}} {
		_, ok = sampler.Sample(bank, testKey(1), solana.PublicKey{}, nil)
		require.True(t, ok)
		reads := reader.reads
		got, ok = sampler.Sample(invalid, testKey(1), testKey(2), nil)
		require.False(t, ok)
		require.Equal(t, AccountFacts{}, got)
		require.Equal(t, AccountFactsSampler{}, sampler)
		require.Equal(t, reads, reader.reads)
	}
	_, ok = sampler.Sample(bank, testKey(1), solana.PublicKey{}, nil)
	require.True(t, ok)
	_, ok = sampler.Sample(bank, solana.PublicKey{}, testKey(2), nil)
	require.False(t, ok)
	require.Equal(t, AccountFactsSampler{}, sampler)
}

func TestAccountFactsMissingParentSource(t *testing.T) {
	for _, store := range []accounts.Accounts{nil, accounts.NewMemAccounts()} {
		bank := accountFactsBank()
		bank.Accounts = store
		// A real bank binding with no parent backend still yields unavailable
		// balances: AccountsDb.GetAccount handles a nil database receiver.
		var sampler AccountFactsSampler
		got, ok := sampler.Sample(bank, testKey(1), testKey(2), nil)
		require.True(t, ok)
		require.False(t, got.HasIdentityBalance)
		require.False(t, got.HasVoteBalance)
		require.Zero(t, got.IdentityLamports)
		require.Zero(t, got.VoteLamports)
	}
}

func TestAccountFactsBankSysvarPrecedence(t *testing.T) {
	bank := accountFactsBank()
	key := sealevel.SysvarClockAddr
	bankSysvars, err := sealevel.NewBankSysvars(0, &accounts.Account{Key: key, Lamports: 55,
		Data: (&sealevel.SysvarClock{}).MustMarshal()})
	require.NoError(t, err)
	require.NoError(t, bank.PublishBankSysvars(bankSysvars))
	reader := &accountFactsSharedReader{accountFactsReader: accountFactsReader{acct: &accounts.Account{Key: key, Lamports: 99}}}
	bank.UnrootedRead = reader
	var sampler AccountFactsSampler
	got, ok := sampler.Sample(bank, key, solana.PublicKey{}, nil)
	require.True(t, ok)
	require.True(t, got.HasIdentityBalance)
	require.Equal(t, uint64(55), got.IdentityLamports)
	require.Zero(t, reader.sharedReads)
	require.Zero(t, reader.reads)
}
