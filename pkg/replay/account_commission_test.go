package replay

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

var commissionLayouts = []struct {
	name       string
	version    uint32
	headerEnd  int
	votesCount int
}{
	{"v0", sealevel.VoteStateVersionV0_23_5, 1909, 1909},
	{"v1", sealevel.VoteStateVersionV1_14_11, 69, 69},
	{"current", sealevel.VoteStateVersionCurrent, 69, 69},
	{"v4", sealevel.VoteStateVersionV4, 136, 145},
}

func commissionAccount(t testing.TB, version uint32, inflation, block uint16) *accounts.Account {
	t.Helper()
	state := sealevel.VoteStateVersions{Type: version}
	state.V0_23_5 = sealevel.VoteState0_23_5{NodePubkey: testKey(3), AuthorizedVoter: testKey(4),
		AuthorizedVoterEpoch: 7, AuthorizedWithdrawer: testKey(5), Commission: byte(inflation)}
	for i := range state.V0_23_5.PriorVoters.Buf {
		state.V0_23_5.PriorVoters.Buf[i] = sealevel.PriorVoter{Pubkey: testKey(byte(i + 6)),
			EpochStart: uint64(i + 10), EpochEnd: uint64(i + 11), Slot: uint64(i + 12)}
	}
	state.V0_23_5.PriorVoters.Index = 17
	state.V1_14_11 = sealevel.VoteState1_14_11{NodePubkey: testKey(3), AuthorizedWithdrawer: testKey(5), Commission: byte(inflation)}
	state.Current = sealevel.VoteState{NodePubkey: testKey(3), AuthorizedWithdrawer: testKey(5), Commission: byte(inflation)}
	state.V4 = sealevel.VoteState4{NodePubkey: testKey(3), AuthorizedWithdrawer: testKey(5),
		InflationRewardsCollector: testKey(7), BlockRevenueCollector: testKey(8),
		InflationRewardsCommissionBps: inflation, BlockRevenueCommissionBps: block}
	data, err := sealevel.MarshalVersionedVoteState(&state)
	require.NoError(t, err)
	return &accounts.Account{Key: testKey(2), Lamports: 50, Owner: addresses.VoteProgramAddr, Data: data}
}

func TestAccountFactCommissionsSerializedLayouts(t *testing.T) {
	for _, layout := range commissionLayouts {
		t.Run(layout.name, func(t *testing.T) {
			values := []uint16{0, 1, 12, 100, 255}
			if layout.version == sealevel.VoteStateVersionV4 {
				values = []uint16{0, 1, 1234, 5678, 10000, 65535}
			}
			for _, value := range values {
				for _, blockValue := range []uint16{value, 65535 - value} {
					acct := commissionAccount(t, layout.version, value, blockValue)
					inflation, block, hasInflation, hasBlock := accountFactCommissions(acct)
					require.True(t, hasInflation)
					if layout.version == sealevel.VoteStateVersionV4 {
						require.Equal(t, value, binary.LittleEndian.Uint16(acct.Data[132:134]))
						require.Equal(t, blockValue, binary.LittleEndian.Uint16(acct.Data[134:136]))
						require.Equal(t, value, inflation)
						require.Equal(t, blockValue, block)
						require.True(t, hasBlock)
					} else {
						require.Equal(t, byte(value), acct.Data[layout.headerEnd-1])
						require.Equal(t, value*100, inflation)
						require.Zero(t, block)
						require.False(t, hasBlock, "legacy block commission must not be inferred")
					}
				}
			}
		})
	}
}

func TestAccountFactCommissionsRejectInvalidHeader(t *testing.T) {
	unknown := func(acct *accounts.Account) {
		t.Helper()
		inflation, block, hasInflation, hasBlock := accountFactCommissions(acct)
		require.Zero(t, inflation)
		require.Zero(t, block)
		require.False(t, hasInflation)
		require.False(t, hasBlock)
	}
	unknown(nil)
	for _, layout := range commissionLayouts {
		t.Run(layout.name, func(t *testing.T) {
			acct := commissionAccount(t, layout.version, 12, 34)
			full := acct.Data
			for n := 0; n < layout.headerEnd; n++ {
				acct.Data = full[:n]
				unknown(acct)
			}
			acct.Data = full
			acct.Executable = true
			unknown(acct)
			acct.Executable, acct.IsDummy = false, true
			unknown(acct)
			acct.IsDummy, acct.Owner = false, solana.PublicKey{}
			unknown(acct)
			acct.Owner = addresses.VoteProgramAddr
			for _, version := range []uint32{4, 255, math.MaxUint32} {
				binary.LittleEndian.PutUint32(acct.Data[:4], version)
				unknown(acct)
			}
		})
	}
}

func TestAccountFactsCommissionValidationPreservesBalances(t *testing.T) {
	for _, tc := range []struct {
		name         string
		edit         func(*accounts.Account)
		err          error
		knownBalance bool
	}{
		{name: "short header", edit: func(a *accounts.Account) { a.Data = a.Data[:135] }, knownBalance: true},
		{name: "wrong owner", edit: func(a *accounts.Account) { a.Owner = solana.PublicKey{} }, knownBalance: true},
		{name: "executable", edit: func(a *accounts.Account) { a.Executable = true }, knownBalance: true},
		{name: "wrong key", edit: func(a *accounts.Account) { a.Key = testKey(3) }},
		{name: "dummy", edit: func(a *accounts.Account) { a.IsDummy = true }},
		{name: "read error", err: errors.New("unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bank := accountFactsBank()
			require.NoError(t, bank.SetAccount(testKey(1), testAccount(1, 99)))
			acct := commissionAccount(t, sealevel.VoteStateVersionV4, 1234, 5678)
			if tc.edit != nil {
				tc.edit(acct)
			}
			reader := &accountFactsSharedReader{accountFactsReader: accountFactsReader{acct: acct, err: tc.err}}
			bank.UnrootedRead = reader
			store := &accountFactsStore{Accounts: bank.Accounts}
			bank.Accounts = store
			var sampler AccountFactsSampler
			got, ok := sampler.sampleAt(time.Unix(100, 0), bank, testKey(1), testKey(2), nil)
			require.True(t, ok)
			require.Equal(t, uint64(99), got.IdentityLamports)
			require.True(t, got.HasIdentityBalance)
			require.Equal(t, tc.knownBalance, got.HasVoteBalance)
			if tc.knownBalance {
				require.Equal(t, uint64(50), got.VoteLamports)
			}
			require.False(t, got.HasInflationRewardsCommission || got.HasBlockRevenueCommission)
			require.Equal(t, 2, store.reads)
			require.Equal(t, 1, reader.sharedReads)
		})
	}
}
