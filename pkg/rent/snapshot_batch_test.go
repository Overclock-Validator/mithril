package rent

import (
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"math/rand"
	"reflect"
	"testing"
)

// Frozen implementation from the buffer-fix baseline.
func referenceRentStateFromAcct(acct *accounts.Account, rent *sealevel.SysvarRent, relaxPostExecMinBalanceCheck bool) *RentStateInfo {
	info := &RentStateInfo{
		RentPayingInfo: RentPayingInfo{
			Lamports: acct.Lamports,
			DataSize: uint64(len(acct.Data)),
			Pk:       acct.Key,
		},
		owner:                        acct.Owner,
		relaxPostExecMinBalanceCheck: relaxPostExecMinBalanceCheck,
	}
	if acct.Lamports == 0 {
		info.RentState = RentStateUninitialized
	} else if rent.IsExempt(acct.Lamports, uint64(len(acct.Data))) {
		info.RentState = RentStateRentExempt
	} else {
		info.RentState = RentStateRentPaying
	}
	return info
}

func referenceRentSnapshot(rent *sealevel.SysvarRent, txCtx *sealevel.TransactionCtx, f *features.Features) []*RentStateInfo {
	rentStateInfos := make([]*RentStateInfo, 0, len(txCtx.Accounts.Accounts))
	acctsMetas := txCtx.Accounts.AcctMetas
	relaxPostExecMinBalanceCheck := f != nil && f.IsActive(features.RelaxPostExecMinBalanceCheck)

	for idx, acct := range txCtx.Accounts.Accounts {
		if sealevel.IsWritable(acctsMetas[idx], f) {
			rentStateInfo := referenceRentStateFromAcct(acct, rent, relaxPostExecMinBalanceCheck)
			rentStateInfos = append(rentStateInfos, rentStateInfo)
		} else {
			rentStateInfos = append(rentStateInfos, nil)
		}
	}

	return rentStateInfos
}

func rentSnapshotFixture(n int, active bool) (*sealevel.TransactionCtx, *features.Features) {
	f := features.NewFeaturesDefault()
	if active {
		f.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
	}
	ctx := &sealevel.TransactionCtx{Accounts: sealevel.TransactionAccounts{Accounts: make([]*accounts.Account, n), AcctMetas: make([]*sealevel.AccountMeta, n)}}
	for i := 0; i < n; i++ {
		key := solana.PublicKey{byte(i)}
		ctx.Accounts.Accounts[i] = &accounts.Account{Key: key, Owner: solana.PublicKey{9}, Lamports: uint64(i + 1), Data: make([]byte, i%17)}
		ctx.Accounts.AcctMetas[i] = &sealevel.AccountMeta{Pubkey: key, IsWritable: i%2 == 0}
	}
	return ctx, f
}
func TestRentSnapshotBatchParity(t *testing.T) {
	rng := rand.New(rand.NewSource(392))
	rent := testRent()
	for run := 0; run < 1000; run++ {
		ctx, f := rentSnapshotFixture(rng.Intn(128), run%2 == 0)
		if run%3 == 0 {
			f = nil
		}
		for i, acct := range ctx.Accounts.Accounts {
			acct.Lamports = uint64(rng.Intn(1000))
			ctx.Accounts.AcctMetas[i].IsWritable = f != nil && rng.Intn(2) == 0
		}
		got := NewRentStateInfo(&rent, ctx, f)
		want := referenceRentSnapshot(&rent, ctx, f)
		require.Equal(t, want, got)
		addresses := map[*RentStateInfo]bool{}
		for _, v := range got {
			if v != nil {
				require.False(t, addresses[v])
				addresses[v] = true
			}
		}
		for _, acct := range ctx.Accounts.Accounts {
			acct.Lamports++
			acct.Owner[0]++
		}
		post := NewRentStateInfo(&rent, ctx, f)
		refPost := referenceRentSnapshot(&rent, ctx, f)
		require.Equal(t, want, got, "pre-snapshot must remain independent")
		require.Equal(t, refPost, post)
		ge := VerifyRentStateChanges(got, post, ctx)
		we := VerifyRentStateChanges(want, refPost, ctx)
		require.Equal(t, reflect.TypeOf(we), reflect.TypeOf(ge))
		if we != nil {
			require.EqualError(t, ge, we.Error())
		}
	}
	ctx, f := rentSnapshotFixture(32, true)
	checkRentSnapshotAllocationCount(t, testing.AllocsPerRun(100, func() { _ = NewRentStateInfo(&rent, ctx, f) }), 2)
}
func BenchmarkRentSnapshotBatch(b *testing.B) {
	rent := testRent()
	for _, n := range []int{8, 32, 128, 256} {
		ctx, f := rentSnapshotFixture(n, true)
		for _, batch := range []bool{false, true} {
			b.Run(fmt.Sprintf("accounts=%d/batch=%v", n, batch), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var s []*RentStateInfo
					if batch {
						s = NewRentStateInfo(&rent, ctx, f)
					} else {
						s = referenceRentSnapshot(&rent, ctx, f)
					}
					if len(s) != n {
						b.Fatal("snapshot changed")
					}
				}
			})
		}
	}
}

func TestRentSnapshotMaskBoundaries(t *testing.T) {
	rent := testRent()
	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513} {
		for _, active := range []bool{false, true} {
			for _, pattern := range []string{"none", "all", "alternating", "boundaries"} {
				t.Run(fmt.Sprintf("n=%d/active=%v/%s", n, active, pattern), func(t *testing.T) {
					ctx, f := rentSnapshotFixture(n, active)
					for i, meta := range ctx.Accounts.AcctMetas {
						switch pattern {
						case "none":
							meta.IsWritable = false
						case "all":
							meta.IsWritable = true
						case "alternating":
							meta.IsWritable = i%2 == 0
						case "boundaries":
							meta.IsWritable = i%64 == 0 || i%64 == 63
						}
					}
					before := NewRentStateInfo(&rent, ctx, f)
					want := referenceRentSnapshot(&rent, ctx, f)
					require.Equal(t, want, before)
					for _, acct := range ctx.Accounts.Accounts {
						acct.Lamports++
						acct.Owner[0]++
					}
					after := NewRentStateInfo(&rent, ctx, f)
					require.Equal(t, referenceRentSnapshot(&rent, ctx, f), after)
					require.Equal(t, want, before)
					seen := map[*RentStateInfo]bool{}
					for _, v := range before {
						if v != nil {
							require.False(t, seen[v])
							seen[v] = true
						}
					}
				})
			}
		}
	}
	ctx, f := rentSnapshotFixture(256, true)
	checkRentSnapshotAllocationCount(t, testing.AllocsPerRun(100, func() { _ = NewRentStateInfo(&rent, ctx, f) }), 2)
	ctx, f = rentSnapshotFixture(513, true)
	checkRentSnapshotAllocationCount(t, testing.AllocsPerRun(100, func() { _ = NewRentStateInfo(&rent, ctx, f) }), 3)
}

func checkRentSnapshotAllocationCount(t *testing.T, got, optimizedMax float64) {
	t.Helper()
	require.LessOrEqual(t, got, optimizedMax)
}
