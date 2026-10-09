package fees

import (
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// staleTailReader fakes the speculative-state read: it holds the leader's LATEST
// balance (including unrooted fee credits), while the durable store would be stale.
type staleTailReader struct {
	acct *accounts.Account
}

func (r *staleTailReader) GetAccount(slot uint64, pk solana.PublicKey) (*accounts.Account, error) {
	return r.acct.Clone(), nil
}

// Regression for the rooted-durable divergence (slot 430276043): when the leader
// is absent from the block, its balance MUST come from the speculative-state read
// (UnrootedRead), not a direct disk read that misses unrooted fee credits.
func TestDistributeTxFeesUsesSpeculativeRead(t *testing.T) {
	leader := solana.PublicKey{0xAB}
	latest := &accounts.Account{Key: leader, Lamports: 10_000_000} // includes unrooted credits

	slotCtx := &sealevel.SlotCtx{
		Slot:          42,
		Accounts:      accounts.NewMemAccounts(), // leader NOT in the block
		ParentAccts:   accounts.NewMemAccounts(),
		UnrootedRead:  &staleTailReader{acct: latest},
		Features:      features.NewFeaturesDefault(),
		ModifiedAccts: make(map[solana.PublicKey]bool),
		AcctMapsMu:    new(sync.Mutex),
		WritableAccts: make(map[solana.PublicKey]bool),
	}

	acc := TxFeeInfoAccumulator{TotalFees: 10_000}
	if _, err := DistributeTxFees(slotCtx, leader, solana.PublicKey{}, &acc); err != nil {
		t.Fatal(err)
	} // nil AccountsDb: MUST not be touched

	got, err := slotCtx.GetAccount(leader)
	if err != nil {
		t.Fatalf("leader must be set on slotCtx: %v", err)
	}
	wantFees := uint64(10_000 - 10_000/2) // non-full-priority split: total - burn(half)
	if got.Lamports != 10_000_000+wantFees {
		t.Fatalf("leader balance = %d, want latest(10000000) + fees(%d) — stale base means the speculative read was bypassed", got.Lamports, wantFees)
	}
}

func TestDistributeTxFeesCollectorValidation(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	minimum := rent.MinimumBalance(0)
	for _, tc := range []struct {
		name                         string
		custom, relax, vote, missing bool
		key                          solana.PublicKey
		owner                        solana.PublicKey
		balance, deposit             uint64
		burn                         bool
	}{
		{name: "custom_system", custom: true, balance: minimum, deposit: 5000},
		{name: "vote_itself", custom: true, vote: true, owner: solana.VoteProgramID, balance: minimum, deposit: 5000},
		{name: "other_vote_owner", custom: true, owner: solana.VoteProgramID, balance: minimum, deposit: 5000, burn: true},
		{name: "reserved_system_key", custom: true, key: a.ComputeBudgetProgramAddr, balance: minimum, deposit: 5000, burn: true},
		{name: "overflow", custom: true, balance: math.MaxUint64, deposit: 1, burn: true},
		{name: "absent_below_rent", custom: true, missing: true, deposit: minimum - 1, burn: true},
		{name: "absent_funded", custom: true, missing: true, deposit: minimum},
		{name: "below_rent_strict", custom: true, balance: 1, deposit: 1, burn: true},
		{name: "below_rent_relaxed", custom: true, relax: true, balance: 1, deposit: 1},
		{name: "absent_below_rent_relaxed", custom: true, relax: true, missing: true, deposit: minimum - 1, burn: true},
		{name: "incinerator", custom: true, key: solana.PublicKey(a.IncineratorAddr), balance: 0, deposit: 1},
		{name: "legacy_identity", balance: minimum, deposit: 5000},
		{name: "legacy_wrong_owner", owner: solana.VoteProgramID, balance: minimum, deposit: 5000, burn: true},
		{name: "zero_fee", custom: true, missing: true, deposit: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.key
			if key.IsZero() {
				key = solana.PublicKey{20}
			}
			vote := solana.PublicKey{21}
			if tc.vote {
				vote = key
			}
			f := features.NewFeaturesDefault()
			f.EnableFeature(features.RewardFullPriorityFee, 0)
			if tc.custom {
				f.EnableFeature(features.CustomCommissionCollector, 0)
			}
			if tc.relax {
				f.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
			}
			mem := accounts.NewMemAccounts()
			before := &accounts.Account{Key: key, Owner: tc.owner, Lamports: tc.balance}
			if !tc.missing {
				mem.SetAccountWithoutLock(key, before.Clone())
			}
			ctx := &sealevel.SlotCtx{Accounts: mem, ParentAccts: accounts.NewMemAccounts(), Features: f, AcctMapsMu: new(sync.Mutex), ModifiedAccts: map[solana.PublicKey]bool{}, WritableAccts: map[solana.PublicKey]bool{}}
			sysvars, err := sealevel.NewBankSysvars(0, &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()})
			require.NoError(t, err)
			require.NoError(t, ctx.PublishBankSysvars(sysvars))
			burned, err := DistributeTxFees(ctx, key, vote, &TxFeeInfoAccumulator{TotalFees: tc.deposit, PriorityFees: tc.deposit})
			require.NoError(t, err)
			if tc.burn || tc.deposit == 0 {
				require.Equal(t, tc.deposit, burned)
				require.Empty(t, ctx.ModifiedAccts)
				after, err := ctx.GetAccount(key)
				if tc.missing {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, before.Clone(), after)
				}
			} else {
				require.Zero(t, burned)
				after, err := ctx.GetAccount(key)
				require.NoError(t, err)
				require.Equal(t, tc.balance+tc.deposit, after.Lamports)
				require.True(t, ctx.ModifiedAccts[key])
			}
		})
	}
}
