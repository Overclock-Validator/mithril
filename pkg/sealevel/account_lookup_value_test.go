package sealevel

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type lookupFallbackStore struct {
	accounts.Accounts
	calls int
}

func (s *lookupFallbackStore) GetAccount(key *[32]byte) (*accounts.Account, error) {
	s.calls++
	return s.Accounts.GetAccount(key)
}

func TestAccountLookupValueParity(t *testing.T) {
	mem := accounts.NewMemAccounts()
	key := solana.PublicKey{1}
	acct := &accounts.Account{Key: key, Lamports: 12, Data: []byte{3}}
	require.NoError(t, mem.SetAccountWithoutLock(key, acct))
	fallback := &lookupFallbackStore{Accounts: mem}
	overlay := accounts.NewOverlayAccounts(mem)
	nested := accounts.NewOverlayAccounts(overlay)
	customParent := accounts.NewOverlayAccounts(fallback)
	for _, store := range []accounts.Accounts{mem, &mem, fallback, overlay, nested, customParent} {
		ctx := &SlotCtx{Accounts: store}
		for i := 0; i < 256; i++ {
			k := solana.PublicKey{byte(i)}
			pk := [32]byte(k)
			want, we := store.GetAccount(&pk)
			got, ge := ctx.GetAccountShared(k)
			require.Same(t, want, got)
			if we == nil {
				require.NoError(t, ge)
			} else {
				require.EqualError(t, ge, we.Error())
			}
		}
	}
	require.Equal(t, 1024, fallback.calls, "custom Accounts implementations must retain their GetAccount behavior")
	for _, store := range []accounts.Accounts{mem, &mem, overlay, nested} {
		ctx := &SlotCtx{Accounts: store}
		n := testing.AllocsPerRun(1000, func() {
			got, err := ctx.GetAccountShared(key)
			if got != acct || err != nil {
				panic("lookup changed")
			}
		})
		require.Zero(t, n, "successful built-in lookup should not allocate a temporary key")
	}
}

func TestAccountLookupValueConcurrent(t *testing.T) {
	mem := accounts.NewMemAccounts()
	ctx := &SlotCtx{Accounts: &mem}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := solana.PublicKey{byte(id)}
			acct := &accounts.Account{Key: key}
			for i := 0; i < 1000; i++ {
				if err := mem.SetAccount((*[32]byte)(&key), acct); err != nil {
					t.Error(err)
					return
				}
				got, err := ctx.GetAccountShared(key)
				if err != nil || got != acct {
					t.Error("lookup changed")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

//go:noinline
func referencePointerLookup(ctx *SlotCtx, key solana.PublicKey) (*accounts.Account, error) {
	pk := [32]byte(key)
	return ctx.Accounts.GetAccount(&pk)
}

func BenchmarkAccountLookupValue(b *testing.B) {
	for _, ptr := range []bool{false, true} {
		mem := accounts.NewMemAccounts()
		key := solana.PublicKey{1}
		acct := &accounts.Account{Key: key}
		_ = mem.SetAccountWithoutLock(key, acct)
		var store accounts.Accounts = mem
		if ptr {
			store = &mem
		}
		ctx := &SlotCtx{Accounts: store}
		for _, fast := range []bool{false, true} {
			b.Run(fmt.Sprintf("pointerStore=%v/valueLookup=%v", ptr, fast), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var got *accounts.Account
					var err error
					if fast {
						got, err = ctx.GetAccountShared(key)
					} else {
						got, err = referencePointerLookup(ctx, key)
					}
					if err != nil || got != acct {
						b.Fatal("lookup changed")
					}
				}
			})
		}
	}
}

func TestAccountLookupValueOverlayConcurrent(t *testing.T) {
	mem := accounts.NewMemAccounts()
	overlay := accounts.NewOverlayAccountsWithLen(mem, 1024)
	ctx := &SlotCtx{Accounts: overlay}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := solana.PublicKey{byte(id + 1)}
			acct := &accounts.Account{Key: key}
			for i := 0; i < 1000; i++ {
				if err := overlay.SetAccount((*[32]byte)(&key), acct); err != nil {
					t.Error(err)
					return
				}
				got, err := ctx.GetAccountShared(key)
				if err != nil || got != acct {
					t.Error("lookup changed")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func BenchmarkAccountLookupOverlayValue(b *testing.B) {
	for _, delta := range []bool{false, true} {
		mem := accounts.NewMemAccounts()
		key := solana.PublicKey{1}
		acct := &accounts.Account{Key: key}
		_ = mem.SetAccountWithoutLock(key, acct)
		overlay := accounts.NewOverlayAccountsWithLen(mem, 1024)
		if delta {
			_ = overlay.SetAccountWithoutLock(key, acct)
		}
		ctx := &SlotCtx{Accounts: overlay}
		for _, fast := range []bool{false, true} {
			b.Run(fmt.Sprintf("delta=%v/valueLookup=%v", delta, fast), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var got *accounts.Account
					var err error
					if fast {
						got, err = ctx.GetAccountShared(key)
					} else {
						got, err = referencePointerLookup(ctx, key)
					}
					if err != nil || got != acct {
						b.Fatal("lookup changed")
					}
				}
			})
		}
	}
}
