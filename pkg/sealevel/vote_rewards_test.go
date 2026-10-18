package sealevel

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func newVoteRewardsTestContext(t *testing.T, pending uint64) (*ExecutionCtx, []AccountMeta) {
	t.Helper()
	vote, source, withdrawer := voteCommissionTestPubkey(101), voteCommissionTestPubkey(102), voteCommissionTestPubkey(103)
	bls := [48]byte{42}
	state := VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{
		NodePubkey: voteCommissionTestPubkey(104), AuthorizedWithdrawer: withdrawer,
		InflationRewardsCollector: voteCommissionTestPubkey(105), BlockRevenueCollector: voteCommissionTestPubkey(106),
		InflationRewardsCommissionBps: 1234, BlockRevenueCommissionBps: 5678,
		PendingDelegatorRewards: pending, BlsPubkeyCompressed: &bls,
		EpochCredits: []EpochCredits{{Epoch: 1055, Credits: 10000, PrevCredits: 9000}},
	}}
	state.V4.AuthorizedVoters.AuthorizedVoters.Set(1058, withdrawer)
	data := make([]byte, VoteStateV4Size)
	require.NoError(t, WriteVersionedVoteStateInPlace(data, &state))
	data[len(data)-1] = 42
	rent := NewDefaultRentSysvar()
	accts := []accounts.Account{
		{Key: a.VoteProgramAddr, Owner: a.NativeLoaderAddr, Executable: true, Lamports: 1},
		{Key: vote, Owner: a.VoteProgramAddr, Data: data, Lamports: rent.MinimumBalance(VoteStateV4Size) + 1000000},
		{Key: source, Owner: a.SystemProgramAddr, Lamports: 10000000},
		{Key: a.SystemProgramAddr, Owner: a.NativeLoaderAddr, Executable: true, Lamports: 1},
		{Key: withdrawer, Owner: a.SystemProgramAddr, Lamports: 1},
	}
	txCtx := NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64)
	txCtx.Rent = rent
	ft := features.NewFeaturesDefault()
	for _, gate := range []features.FeatureGate{features.VoteStateV4, features.CommissionRateInBasisPoints, features.CustomCommissionCollector, features.BlockRevenueSharing} {
		ft.EnableFeature(gate, 0)
	}
	execCtx := &ExecutionCtx{TransactionContext: txCtx, Features: *ft, ComputeMeter: cu.NewComputeMeterDefault(), RecordInnerInstructions: true}
	metas := []AccountMeta{{Pubkey: vote, IsWritable: true}, {Pubkey: source, IsWritable: true, IsSigner: true}, {Pubkey: a.SystemProgramAddr}}
	return execCtx, metas
}

// Agave v4.4.0-beta.0 vote_processor.rs's deposit tests exercise real System
// CPI, including its errors and 150 CU cost. Do not replace it with a balance
// adjustment or return early for a zero deposit.
func TestVoteDepositDelegatorRewards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ExecutionCtx, *[]AccountMeta, *[]byte)
		want   error
		cpi    bool
	}{
		{name: "success", cpi: true},
		{name: "zero", mutate: func(_ *ExecutionCtx, _ *[]AccountMeta, d *[]byte) { binary.LittleEndian.PutUint64((*d)[4:], 0) }, cpi: true},
		{name: "trailing_bytes", mutate: func(_ *ExecutionCtx, _ *[]AccountMeta, d *[]byte) { *d = append(*d, 1, 2, 3) }, cpi: true},
		{name: "preserve_v4_without_migration_gate", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.Features.DisableFeature(features.VoteStateV4)
		}, cpi: true},
		{name: "resize_v4", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[1].Data = e.TransactionContext.Accounts.Accounts[1].Data[:1000]
		}, cpi: true},
		{name: "resize_not_rent_exempt_after_cpi", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			account := e.TransactionContext.Accounts.Accounts[1]
			account.Data = account.Data[:1000]
			account.Lamports = 1
		}, want: InstrErrAccountNotRentExempt, cpi: true},
		{name: "bps_disabled", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.Features.DisableFeature(features.CommissionRateInBasisPoints)
		}, want: InstrErrInvalidInstructionData},
		{name: "collector_disabled", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.Features.DisableFeature(features.CustomCommissionCollector)
		}, want: InstrErrInvalidInstructionData},
		{name: "revenue_disabled", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.Features.DisableFeature(features.BlockRevenueSharing)
		}, want: InstrErrInvalidInstructionData},
		{name: "old_feature_address", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.Features.DisableFeature(features.BlockRevenueSharing)
			e.Features.EnableFeature(features.FeatureGate{Name: "BlockRevenueSharing", Address: base58.MustDecodeFromString("B1ockRevenueSharing111111111111111111111111")}, 0)
		}, want: InstrErrInvalidInstructionData},
		{name: "truncated_deposit", mutate: func(_ *ExecutionCtx, _ *[]AccountMeta, d *[]byte) { *d = (*d)[:11] }, want: InstrErrInvalidInstructionData},
		{name: "missing_source", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) { *m = (*m)[:1] }, want: InstrErrMissingAccount},
		{name: "missing_system_program", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) { *m = (*m)[:2] }, want: InstrErrMissingAccount},
		{name: "unsigned_source", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) { (*m)[1].IsSigner = false }, want: InstrErrMissingRequiredSignature},
		{name: "invalid_state", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[1].Data = []byte{99}
		}, want: InstrErrInvalidAccountData},
		{name: "zeroed_state", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			clear(e.TransactionContext.Accounts.Accounts[1].Data)
		}, want: InstrErrInvalidAccountData},
		{name: "v3_not_migrated", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			account := e.TransactionContext.Accounts.Accounts[1]
			v, err := UnmarshalVersionedVoteState(account.Data)
			require.NoError(t, err)
			require.NoError(t, WriteVersionedVoteStateInPlace(account.Data, &VoteStateVersions{Type: VoteStateVersionCurrent, Current: *v.ConvertToCurrent()}))
		}, want: InstrErrInvalidAccountData},
		{name: "v4_without_voters", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			account := e.TransactionContext.Accounts.Accounts[1]
			v, err := UnmarshalVersionedVoteState(account.Data)
			require.NoError(t, err)
			v.V4.AuthorizedVoters = AuthorizedVoters{}
			require.NoError(t, WriteVersionedVoteStateInPlace(account.Data, v))
		}, cpi: true},
		{name: "source_owner", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[2].Owner = a.VoteProgramAddr
		}, want: InstrErrExternalAccountLamportSpend, cpi: true},
		{name: "source_data", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[2].Data = []byte{1}
		}, want: InstrErrInvalidArgument, cpi: true},
		{name: "source_equals_vote", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) {
			(*m)[1].Pubkey = (*m)[0].Pubkey
			(*m)[0].IsSigner = true // Duplicate accounts share message privileges.
		}, want: InstrErrInvalidArgument, cpi: true},
		{name: "insufficient_lamports", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[2].Lamports = 99999
		}, want: SystemProgErrResultWithNegativeLamports, cpi: true},
		{name: "source_readonly", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) { (*m)[1].IsWritable = false }, want: InstrErrPrivilegeEscalation},
		{name: "vote_readonly", mutate: func(_ *ExecutionCtx, m *[]AccountMeta, _ *[]byte) { (*m)[0].IsWritable = false }, want: InstrErrPrivilegeEscalation},
		{name: "pending_overflow_after_cpi", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			binary.LittleEndian.PutUint64(e.TransactionContext.Accounts.Accounts[1].Data[136:], math.MaxUint64-99999)
		}, want: InstrErrArithmeticOverflow, cpi: true},
		{name: "lamports_overflow", mutate: func(e *ExecutionCtx, _ *[]AccountMeta, _ *[]byte) {
			e.TransactionContext.Accounts.Accounts[1].Lamports = math.MaxUint64
		}, want: InstrErrArithmeticOverflow, cpi: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execCtx, metas := newVoteRewardsTestContext(t, 123)
			data := binary.LittleEndian.AppendUint32(nil, 19)
			data = binary.LittleEndian.AppendUint64(data, 100000)
			if tc.mutate != nil {
				tc.mutate(execCtx, &metas, &data)
			}
			txCtx := execCtx.TransactionContext
			beforeVote, beforeSource := txCtx.Accounts.Accounts[1].Clone(), txCtx.Accounts.Accounts[2].Clone()
			err := execCtx.ProcessInstruction(data, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0})
			wantCU := uint64(2100)
			if tc.cpi {
				wantCU += 150
			}
			require.Equal(t, wantCU, execCtx.ComputeMeter.Used())
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, beforeVote.Data, txCtx.Accounts.Accounts[1].Data, "failed deposit must not update pending rewards")
				require.Empty(t, execCtx.ModifiedVoteStates)
				// The instruction layer can retain CPI effects on failure. Transaction
				// rollback is verified separately through replay's publication path.
				return
			}
			require.NoError(t, err)
			deposit := binary.LittleEndian.Uint64(data[4:])
			afterVote, afterSource := txCtx.Accounts.Accounts[1], txCtx.Accounts.Accounts[2]
			require.Equal(t, beforeVote.Lamports+deposit, afterVote.Lamports)
			require.Equal(t, beforeSource.Lamports-deposit, afterSource.Lamports)
			expected := beforeVote.Clone()
			if len(expected.Data) < VoteStateV4Size {
				expected.Data = append(expected.Data, make([]byte, VoteStateV4Size-len(expected.Data))...)
			}
			pending := binary.LittleEndian.Uint64(expected.Data[136:]) + deposit
			binary.LittleEndian.PutUint64(expected.Data[136:], pending)
			require.Equal(t, expected.Data, afterVote.Data, "preserve every other field and trailing byte")
			require.Equal(t, pending, execCtx.ModifiedVoteStates[afterVote.Key].V4.PendingDelegatorRewards)
			require.Len(t, execCtx.InnerInstrs, 1)
			require.Equal(t, uint8(3), execCtx.InnerInstrs[0].ProgramIdIndex)
			require.Equal(t, []byte{2, 1}, execCtx.InnerInstrs[0].Accounts)
			transfer := binary.LittleEndian.AppendUint32(nil, 2)
			transfer = binary.LittleEndian.AppendUint64(transfer, deposit)
			require.Equal(t, transfer, execCtx.InnerInstrs[0].Data)
			if deposit > 0 {
				require.NoError(t, execCtx.ProcessInstruction(data, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0}))
				require.Equal(t, beforeVote.Lamports+2*deposit, txCtx.Accounts.Accounts[1].Lamports)
				require.Equal(t, pending+deposit, execCtx.ModifiedVoteStates[afterVote.Key].V4.PendingDelegatorRewards)
			}
		})
	}
}

func TestVoteWithdrawReservesDelegatorRewards(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		pending, remaining             uint64
		close, recent, sharingDisabled bool
		want                           error
	}{
		{name: "exact_reserved_balance", pending: 500000, remaining: 500000},
		{name: "spends_reserved_lamport", pending: 500000, remaining: 499999, want: InstrErrInsufficientFunds},
		{name: "close_with_pending", pending: 500000, close: true, want: InstrErrInsufficientFunds},
		{name: "pending_precedes_active_close", pending: 500000, close: true, recent: true, want: InstrErrInsufficientFunds},
		{name: "rent_plus_pending_overflow", pending: math.MaxUint64, remaining: 500000, want: InstrErrArithmeticOverflow},
		{name: "no_pending_rent_boundary", remaining: 0},
		{name: "close_without_pending", close: true},
		{name: "active_close_without_pending", close: true, recent: true, want: VoteErrActiveVoteAccountClose},
		{name: "reserve_even_if_gate_disabled", pending: 500000, remaining: 499999, sharingDisabled: true, want: InstrErrInsufficientFunds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execCtx, _ := newVoteRewardsTestContext(t, tc.pending)
			if tc.sharingDisabled {
				execCtx.Features.DisableFeature(features.BlockRevenueSharing)
			}
			txCtx := execCtx.TransactionContext
			vote, recipient, withdrawer := txCtx.Accounts.Accounts[1], txCtx.Accounts.Accounts[2], txCtx.Accounts.Accounts[4]
			if tc.recent {
				v, err := UnmarshalVersionedVoteState(vote.Data)
				require.NoError(t, err)
				v.V4.EpochCredits[0].Epoch = 1058
				require.NoError(t, WriteVersionedVoteStateInPlace(vote.Data, v))
			}
			before := vote.Clone()
			recipientBefore := recipient.Lamports
			rent := txCtx.Rent
			clock := SysvarClock{Epoch: 1058}
			old := SysvarCache
			SysvarCache.Rent.Sysvar = &rent
			SysvarCache.Clock.Sysvar = &clock
			t.Cleanup(func() { SysvarCache = old })
			remaining := rent.MinimumBalance(uint64(len(vote.Data))) + tc.remaining
			if tc.close {
				remaining = 0
			}
			amount := vote.Lamports - remaining
			data := binary.LittleEndian.AppendUint32(nil, 3)
			data = binary.LittleEndian.AppendUint64(data, amount)
			metas := []AccountMeta{{Pubkey: vote.Key, IsWritable: true}, {Pubkey: recipient.Key, IsWritable: true}, {Pubkey: withdrawer.Key, IsSigner: true}}
			err := execCtx.ProcessInstruction(data, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0})
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, before, txCtx.Accounts.Accounts[1])
				require.Equal(t, recipientBefore, recipient.Lamports)
				return
			}
			require.NoError(t, err)
			require.Equal(t, remaining, vote.Lamports)
			require.Equal(t, recipientBefore+amount, recipient.Lamports)
			if !tc.close {
				require.Equal(t, before.Data, vote.Data)
			}
		})
	}
}

// Keep the gate keyed by the deployed address, not the former placeholder.
func TestVoteRewardsFeatureAddress(t *testing.T) {
	require.Equal(t, solana.MustPublicKeyFromBase58("7MYx95UBiJufqnumyN7HfskJ9vKdcGMmhreVguqrE97K"), solana.PublicKey(features.BlockRevenueSharing.Address))
}
