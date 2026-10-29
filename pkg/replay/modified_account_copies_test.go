package replay

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func modifiedAccountsBank(t *testing.T, n int) (*sealevel.SlotCtx, map[solana.PublicKey]*accounts.Account) {
	t.Helper()
	slotCtx := &sealevel.SlotCtx{
		Accounts:      accounts.NewOverlayAccounts(accounts.NewMemAccounts()),
		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: map[solana.PublicKey]bool{},
		WritableAccts: map[solana.PublicKey]bool{},
	}
	stored := map[solana.PublicKey]*accounts.Account{}
	for i := range n {
		var key solana.PublicKey
		key[0], key[1], key[31] = byte(i), byte(i>>8), 0x5c
		acct := &accounts.Account{Key: key, Lamports: uint64(i + 1), Data: bytes.Repeat([]byte{byte(i)}, i%300)}
		require.NoError(t, slotCtx.SetAccount(key, acct))
		slotCtx.ModifiedAccts[key] = true
		stored[key] = acct
	}
	return slotCtx, stored
}

func TestAppendModifiedAccountCopies(t *testing.T) {
	for _, n := range []int{0, 5, 1500} { // none, one worker, several
		slotCtx, stored := modifiedAccountsBank(t, n)
		head := &accounts.Account{Key: solana.PublicKey{0xee}}
		got := appendModifiedAccountCopies([]*accounts.Account{head}, slotCtx)
		require.Len(t, got, n+1)
		require.Same(t, head, got[0])
		seen := map[solana.PublicKey]bool{}
		for _, acct := range got[1:] {
			orig := stored[acct.Key]
			require.NotNil(t, orig)
			require.False(t, seen[acct.Key])
			seen[acct.Key] = true
			require.NotSame(t, orig, acct)
			require.Equal(t, orig.Lamports, acct.Lamports)
			require.Equal(t, orig.Data, acct.Data)
			if len(acct.Data) > 0 {
				acct.Data[0] ^= 0xff
				require.NotEqual(t, orig.Data[0], acct.Data[0], "copy shares data with the bank")
			}
		}
	}
}

func TestAppendModifiedAccountCopiesPanicsOnCallerForMissingAccount(t *testing.T) {
	slotCtx, _ := modifiedAccountsBank(t, 1500)
	slotCtx.ModifiedAccts[solana.PublicKey{0xfa}] = true
	require.PanicsWithValue(t, "compileWritableAndModifiedAccts: unable to get "+solana.PublicKey{0xfa}.String()+": no such account "+solana.PublicKey{0xfa}.String()+" found",
		func() { appendModifiedAccountCopies(nil, slotCtx) })
}

// A mainnet tip slot copies about 1,800 accounts and 8.5 MB.
func BenchmarkAppendModifiedAccountCopies1800(b *testing.B) {
	slotCtx := &sealevel.SlotCtx{
		Accounts:      accounts.NewOverlayAccounts(accounts.NewMemAccounts()),
		ModifiedAccts: map[solana.PublicKey]bool{},
	}
	for i := range 1800 {
		var key solana.PublicKey
		key[0], key[1], key[31] = byte(i), byte(i>>8), 0x5c
		size := 3762 // vote accounts
		if i >= 664 {
			size = []int{165, 200, 1000, 3000, 8000, 10240, 20000}[i%7]
		}
		if i == 700 {
			size = 131097 // SlotHistory
		}
		_ = slotCtx.SetAccount(key, &accounts.Account{Key: key, Lamports: 1, Data: make([]byte, size)})
		slotCtx.ModifiedAccts[key] = true
	}
	ballast := make([]byte, 1<<30)
	defer runtime.KeepAlive(ballast)
	b.Run("serial", func(b *testing.B) {
		for b.Loop() {
			out := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts))
			for pk := range slotCtx.ModifiedAccts {
				acct, _ := slotCtx.GetAccount(pk)
				out = append(out, acct)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		for b.Loop() {
			appendModifiedAccountCopies(make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)), slotCtx)
		}
	})
}
