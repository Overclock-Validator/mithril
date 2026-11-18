package replay

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type leaderParentSource struct {
	values                  map[solana.PublicKey]*accounts.Account
	ownedReads, sharedReads int
}

func (s *leaderParentSource) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	s.ownedReads++
	if acct := s.values[key]; acct != nil {
		return acct.Clone(), nil
	}
	return nil, accountsdb.ErrNoAccount
}

func (s *leaderParentSource) GetAccountShared(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	s.sharedReads++
	if acct := s.values[key]; acct != nil {
		return acct, nil
	}
	return nil, accountsdb.ErrNoAccount
}

// This wrapper deliberately exposes only owned reads.
type ownedLeaderParentSource struct{ source *leaderParentSource }

func (s ownedLeaderParentSource) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	return s.source.GetAccount(slot, key)
}

func leaderAccountFixture(n, payload int, removeADH bool) (*sealevel.SlotCtx, *leaderParentSource, []*accounts.Account) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.AccountsLtHash, 0)
	if removeADH {
		feats.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	}
	source := &leaderParentSource{values: make(map[solana.PublicKey]*accounts.Account)}
	ctx := &sealevel.SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts(), ParentAccts: accounts.NewMemAccounts(),
		Features: feats, UnrootedRead: source, ModifiedAccts: make(map[solana.PublicKey]bool),
		WritableAccts: make(map[solana.PublicKey]bool), AcctMapsMu: &sync.Mutex{}, AcctsLtHash: &lthash.LtHash{}}
	live := make([]*accounts.Account, n)
	for i := range n {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[:8], uint64(i+1))
		parent := &accounts.Account{Key: key, Lamports: uint64(i + 100), Data: make([]byte, payload), Owner: [32]byte{12}, RentEpoch: 17}
		for j := range parent.Data {
			parent.Data[j] = byte(i + j)
		}
		if i%7 != 0 {
			source.values[key] = parent
		} // Newly created accounts have a zero before-value.
		acct := parent.Clone()
		acct.Lamports += 10
		if i%5 == 0 {
			acct.Lamports = 0
		} // Also cover deletions.
		if len(acct.Data) > 0 {
			acct.Data[0] ^= 1
		}
		_ = ctx.SetAccount(key, acct)
		ctx.RecordModifiedAcct(key)
		live[i] = acct
	}
	return ctx, source, live
}

func leaderOutput(accts []*accounts.Account) LoadAndExecuteTransactionOutput {
	touched := make([]bool, len(accts))
	for i := range touched {
		touched[i] = true
	}
	return LoadAndExecuteTransactionOutput{ExecCtx: &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{
		Accounts: sealevel.TransactionAccounts{Accounts: accts, Touched: touched},
	}}}
}

// The old finalizer owned both the final account copies and its before-values.
// Keep that behavior as a differential reference and allocation benchmark.
func legacyLeaderAccountViews(ctx *sealevel.SlotCtx, block *b.Block, rent []*accounts.Account) ([]*accounts.Account, []*accounts.Account) {
	writable, modified := compileLeaderAccounts(ctx, block, rent)
	for i, acct := range writable {
		writable[i] = acct.Clone()
	}
	for i, acct := range modified {
		modified[i] = acct.Clone()
	}
	return writable, modified
}

func legacyLeaderParentImages(ctx *sealevel.SlotCtx, modified []*accounts.Account) error {
	for _, acct := range modified {
		if _, err := ctx.GetParentAccount(acct.Key); err == nil {
			continue
		}
		parent, err := ctx.GetAccountFromAccountsDb(acct.Key)
		if err != nil {
			parent = &accounts.Account{Key: acct.Key}
		}
		if err := ctx.ParentAccts.SetAccountWithoutLock(acct.Key, parent.Clone()); err != nil {
			return err
		}
	}
	return nil
}

func TestLeaderFrozenAccountViewsPreserveBankHash(t *testing.T) {
	for _, removeADH := range []bool{false, true} {
		for _, shared := range []bool{false, true} {
			t.Run(fmt.Sprintf("removeADH=%t/shared=%t", removeADH, shared), func(t *testing.T) {
				want, _, _ := leaderAccountFixture(40, 333, removeADH)
				got, source, live := leaderAccountFixture(40, 333, removeADH)
				if !shared {
					got.UnrootedRead = ownedLeaderParentSource{source}
				}
				before := make(map[solana.PublicKey]*accounts.Account)
				for key, acct := range source.values {
					before[key] = acct.Clone()
				}
				// Include duplicates and stale epoch-account data: the live bank wins.
				block := &b.Block{EpochUpdatedAccts: []*accounts.Account{live[1].Clone(), live[1].Clone()}}
				block.EpochUpdatedAccts[0].Lamports = 99999
				wantW, wantM := legacyLeaderAccountViews(want, block, nil)
				require.NoError(t, legacyLeaderParentImages(want, wantM))
				// Half are prepared before freeze; footer-only writes use fallback.
				require.NoError(t, PrepareLeaderTransactionParents(got, leaderOutput(live[:20])))
				gotW, gotM := compileLeaderAccounts(got, block, nil)
				require.Len(t, gotM, 40)
				require.NoError(t, ensureParentAccountsForModified(got, gotM))
				wantHash := bankhash.CalculateBankHash(want, wantW, wantM, [32]byte{7}, 17, [32]byte{8})
				gotHash := bankhash.CalculateBankHash(got, gotW, gotM, [32]byte{7}, 17, [32]byte{8})
				require.Equal(t, wantHash, gotHash)
				for _, acct := range gotM {
					stored, err := got.GetAccountShared(acct.Key)
					require.NoError(t, err)
					require.Same(t, stored, acct, "finalization must borrow the frozen bank value")
					parent, err := got.GetParentAccount(acct.Key)
					require.NoError(t, err)
					wantParent, err := want.GetParentAccount(acct.Key)
					require.NoError(t, err)
					require.Equal(t, wantParent.Clone(), parent.Clone())
				}
				require.Equal(t, before, source.values, "hashing must leave the pinned source unchanged")
				if shared {
					require.Zero(t, source.ownedReads)
				} else {
					require.Zero(t, source.sharedReads)
				}
			})
		}
	}
}

func TestLeaderParentPreparationKeepsFirstImageAcrossWrites(t *testing.T) {
	ctx, source, live := leaderAccountFixture(3, 1024, true)
	key := live[1].Key
	original := source.values[key].Clone()
	out := leaderOutput([]*accounts.Account{live[1]})
	require.NoError(t, PrepareLeaderTransactionParents(ctx, out))
	parent, err := ctx.GetParentAccount(key)
	require.NoError(t, err)
	require.Equal(t, original, parent)
	require.NotSame(t, source.values[key], parent, "the header is private")
	// Replace both the source version and a transaction's current value. The
	// retained bank parent must remain the original, not the last transaction.
	source.values[key] = original.Clone()
	source.values[key].Data[0] ^= 0x80
	live[1].Data[1] ^= 0x40
	live[1].Lamports++
	require.NoError(t, PrepareLeaderTransactionParents(ctx, out, out))
	require.Equal(t, original, parent)
	require.Equal(t, 1, source.sharedReads)
	require.Zero(t, source.ownedReads)
	// Header bookkeeping must not mutate the source's original header.
	parent.Lamports++
	require.Equal(t, original.Lamports, source.values[key].Lamports)
}

func TestLeaderParentPreparationSkipsUntouchedAndKeepsAbsence(t *testing.T) {
	ctx, source, live := leaderAccountFixture(3, 64, true)
	out := leaderOutput(live)
	out.ExecCtx.TransactionContext.Accounts.Touched[1] = false
	require.NoError(t, PrepareLeaderTransactionParents(ctx, out))
	_, err := ctx.GetParentAccount(live[1].Key)
	require.Error(t, err)
	parent, err := ctx.GetParentAccount(live[0].Key)
	require.NoError(t, err)
	require.Equal(t, &accounts.Account{Key: live[0].Key}, parent)
	source.values[live[0].Key] = live[0].Clone()
	require.NoError(t, PrepareLeaderTransactionParents(ctx, out))
	require.Zero(t, parent.Lamports, "known absence must not turn into an in-slot value")
}

func TestLeaderParentPreparationConcurrentScratchReads(t *testing.T) {
	ctx, _, live := leaderAccountFixture(1000, 64, true)
	scratch := sealevel.NewScratchSlotCtx(ctx)
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 3 {
				for _, acct := range live {
					// A not-yet-prepared image may be absent, but concurrent
					// preparation must not race the store's map access.
					_, _ = scratch.GetParentAccount(acct.Key)
				}
			}
		}()
	}
	close(start)
	require.NoError(t, PrepareLeaderTransactionParents(ctx, leaderOutput(live[:500])))
	require.NoError(t, ensureParentAccountsForModified(ctx, live))
	readers.Wait()
}

func BenchmarkLeaderAccountCompletion(bm *testing.B) {
	// 800 modified accounts including vote-sized records and a heavier 16MiB
	// payload fixture. These benchmarks exclude execution and hash computation.
	for _, payload := range []int{4096, 20 * 1024} {
		for _, legacy := range []bool{true, false} {
			bm.Run(fmt.Sprintf("bytes=%d/legacy=%t", payload, legacy), func(bm *testing.B) {
				ctx, _, _ := leaderAccountFixture(800, payload, true)
				block := &b.Block{}
				bm.ReportAllocs()
				bm.ResetTimer()
				for i := 0; i < bm.N; i++ {
					ctx.ParentAccts = accounts.NewMemAccounts()
					if legacy {
						_, modified := legacyLeaderAccountViews(ctx, block, nil)
						if err := legacyLeaderParentImages(ctx, modified); err != nil {
							bm.Fatal(err)
						}
					} else {
						_, modified := compileLeaderAccounts(ctx, block, nil)
						if err := ensureParentAccountsForModified(ctx, modified); err != nil {
							bm.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkLeaderParentPreparation(bm *testing.B) {
	for _, phase := range []string{"preparation", "completion_after_preparation"} {
		bm.Run(phase, func(bm *testing.B) {
			ctx, _, live := leaderAccountFixture(800, 20*1024, true)
			out := leaderOutput(live)
			block := &b.Block{}
			if err := PrepareLeaderTransactionParents(ctx, out); err != nil {
				bm.Fatal(err)
			}
			bm.ReportAllocs()
			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				if phase == "preparation" {
					ctx.ParentAccts = accounts.NewMemAccounts()
					if err := PrepareLeaderTransactionParents(ctx, out); err != nil {
						bm.Fatal(err)
					}
				} else {
					_, modified := compileLeaderAccounts(ctx, block, nil)
					if err := ensureParentAccountsForModified(ctx, modified); err != nil {
						bm.Fatal(err)
					}
				}
			}
		})
	}
}
