package sealevel

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVoteUpdateCommissionCollectorSlot447456774(t *testing.T) {
	var fixture struct {
		Slot         uint64
		Transaction  string
		Accounts     []accounts.Account
		Rent         accounts.Account
		HashEvidence struct {
			ParentBankhash       string           `json:"parent_bankhash"`
			Blockhash            string           `json:"blockhash"`
			NumSignatures        uint64           `json:"num_signatures"`
			BrokenBankhash       string           `json:"broken_bankhash"`
			ExpectedBankhash     string           `json:"expected_bankhash"`
			BrokenLtHashChecksum string           `json:"broken_lthash_checksum"`
			BrokenLtHash         []byte           `json:"broken_lthash"`
			BrokenVote           accounts.Account `json:"broken_vote"`
		} `json:"hash_evidence"`
	}
	data, err := os.ReadFile("testdata/slot_447456774_collector.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixture))
	wire, err := base64.StdEncoding.DecodeString(fixture.Transaction)
	require.NoError(t, err)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.NoError(t, tx.VerifySignatures())
	var rent SysvarRent
	require.NoError(t, rent.UnmarshalWithDecoder(bin.NewBinDecoder(fixture.Rent.Data)))
	previousRent := SysvarCache.Rent
	SysvarCache.Rent.Sysvar = &rent
	t.Cleanup(func() { SysvarCache.Rent = previousRent })
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.VoteStateV4, 0)
	ft.EnableFeature(features.CustomCommissionCollector, 0)

	byKey := make(map[solana.PublicKey]accounts.Account)
	for _, acct := range fixture.Accounts {
		byKey[acct.Key] = acct
	}
	txAccts := make([]accounts.Account, len(tx.Message.AccountKeys))
	for i, key := range tx.Message.AccountKeys {
		txAccts[i] = byKey[key]
		if key == a.VoteProgramAddr {
			txAccts[i] = accounts.Account{Key: key, Lamports: 1, Owner: a.NativeLoaderAddr, Executable: true}
		}
	}
	before := txAccts[3].Clone()
	txCtx := NewTransactionCtx(*NewTransactionAccounts(txAccts), 5, 64)
	txCtx.Rent = rent
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *ft}
	instr := tx.Message.Instructions[0]
	var metas []AccountMeta
	for _, idx := range instr.Accounts {
		key := tx.Message.AccountKeys[idx]
		writable, err := tx.Message.IsWritable(key)
		require.NoError(t, err)
		metas = append(metas, AccountMeta{Pubkey: key, IsSigner: tx.Message.IsSigner(key), IsWritable: writable})
	}
	err = execCtx.ProcessInstruction(instr.Data, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{uint64(instr.ProgramIDIndex)})
	require.NoError(t, err, "testnet accepted this transaction at slot %d", fixture.Slot)
	after, err := txCtx.Accounts.GetAccount(3)
	require.NoError(t, err)
	// The only account-data change is the 32-byte V4 block-revenue collector.
	expected := before.Clone()
	copy(expected.Data[100:132], tx.Message.AccountKeys[2][:])
	require.Equal(t, expected.Data, after.Data)
	require.Equal(t, before.Lamports, after.Lamports)
	require.Equal(t, uint64(2100), execCtx.ComputeMeter.Used())
	require.Equal(t, tx.Message.AccountKeys[2], execCtx.ModifiedVoteStates[before.Key].V4.BlockRevenueCollector)

	// The artifact's full LtHash was reconstructed from the persisted parent
	// and all 455 modified accounts, checking each account's data SHA-256
	// against the crash artifact. Reward updates touch different V4 fields,
	// so apply the executed collector change to the post-reward vote account.
	evidence := fixture.HashEvidence
	require.Len(t, evidence.BrokenLtHash, 2048)
	lt := new(lthash.LtHash).InitWithHash(evidence.BrokenLtHash)
	require.Equal(t, evidence.BrokenLtHashChecksum, solana.HashFromBytes(lt.Checksum()).String())
	bankhash := func() string {
		h := sha256.New()
		parent := solana.MustHashFromBase58(evidence.ParentBankhash)
		block := solana.MustHashFromBase58(evidence.Blockhash)
		h.Write(parent[:])
		h.Write(binary.LittleEndian.AppendUint64(nil, evidence.NumSignatures))
		h.Write(block[:])
		inner := h.Sum(nil)
		h.Reset()
		h.Write(inner)
		h.Write(lt.Hash())
		return solana.HashFromBytes(h.Sum(nil)).String()
	}
	require.Equal(t, evidence.BrokenBankhash, bankhash())
	fixedVote := evidence.BrokenVote.Clone()
	copy(fixedVote.Data[100:132], after.Data[100:132])
	lt.Sub(new(lthash.LtHash).InitWithAcct(&evidence.BrokenVote))
	lt.Add(new(lthash.LtHash).InitWithAcct(fixedVote))
	require.Equal(t, evidence.ExpectedBankhash, bankhash())
}

func TestVoteUpdateCommissionCollector(t *testing.T) {
	for _, kind := range []uint32{0, 1} {
		t.Run(map[uint32]string{0: "inflation", 1: "block_revenue"}[kind], func(t *testing.T) {
			rent := NewDefaultRentSysvar()
			previousRent := SysvarCache.Rent
			SysvarCache.Rent.Sysvar = &rent
			t.Cleanup(func() { SysvarCache.Rent = previousRent })
			for _, tc := range []struct {
				name         string
				mutate       func(*features.Features, []accounts.Account, []AccountMeta, *[]byte)
				want         error
				accountCount int
			}{
				{name: "success"},
				{name: "missing_collector", accountCount: 1, want: InstrErrMissingAccount},
				{name: "missing_third_account", accountCount: 2, want: InstrErrMissingAccount},
				{name: "disabled", mutate: func(f *features.Features, _ []accounts.Account, _ []AccountMeta, _ *[]byte) {
					f.DisableFeature(features.CustomCommissionCollector)
				}, want: InstrErrInvalidInstructionData},
				{name: "unknown_kind", mutate: func(_ *features.Features, _ []accounts.Account, _ []AccountMeta, d *[]byte) {
					binary.LittleEndian.PutUint32((*d)[4:], 2)
				}, want: InstrErrInvalidInstructionData},
				{name: "truncated_kind", mutate: func(_ *features.Features, _ []accounts.Account, _ []AccountMeta, d *[]byte) { *d = (*d)[:7] }, want: InstrErrInvalidInstructionData},
				{name: "missing_signature", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) { m[2].IsSigner = false }, want: InstrErrMissingRequiredSignature},
				{name: "wrong_signer", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) {
					m[2].IsSigner = false
					m[1].IsSigner = true
				}, want: InstrErrMissingRequiredSignature},
				{name: "collector_owner", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) { a[2].Owner = a[1].Owner }, want: InstrErrInvalidAccountOwner},
				{name: "collector_rent", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) {
					a[2].Lamports = rent.MinimumBalance(0) - 1
				}, want: InstrErrInsufficientFunds},
				{name: "collector_readonly", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) { m[1].IsWritable = false }, want: InstrErrInvalidArgument},
				{name: "vote_readonly", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) { m[0].IsWritable = false }, want: InstrErrReadonlyDataModified},
				{name: "invalid_vote_data", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) { a[1].Data = []byte{99} }, want: InstrErrInvalidAccountData},
				{name: "zero_tag_full_size", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) {
					a[1].Data = make([]byte, VoteStateV4Size)
				}, want: InstrErrInvalidAccountData},
				{name: "zero_tag_only", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) {
					a[1].Data = make([]byte, 4)
				}, want: InstrErrInvalidAccountData},
				{name: "v4_without_authorized_voters", mutate: func(_ *features.Features, a []accounts.Account, _ []AccountMeta, _ *[]byte) {
					state, err := UnmarshalVersionedVoteState(a[1].Data)
					require.NoError(t, err)
					state.V4.AuthorizedVoters = AuthorizedVoters{}
					require.NoError(t, WriteVersionedVoteStateInPlace(a[1].Data, state))
				}},
				{name: "collector_aliases_vote", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) {
					m[1].Pubkey = m[0].Pubkey
				}},
				{name: "collector_aliases_withdrawer", mutate: func(_ *features.Features, _ []accounts.Account, m []AccountMeta, _ *[]byte) {
					m[1].Pubkey = m[2].Pubkey
					m[2].IsWritable = true
				}},
				{name: "trailing_bytes", mutate: func(_ *features.Features, _ []accounts.Account, _ []AccountMeta, d *[]byte) { *d = append(*d, 1, 2, 3) }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ft := features.NewFeaturesDefault()
					ft.EnableFeature(features.VoteStateV4, 0)
					ft.EnableFeature(features.CustomCommissionCollector, 0)
					vote, collector, withdrawer := voteCommissionTestPubkey(71), voteCommissionTestPubkey(72), voteCommissionTestPubkey(73)
					bls := [48]byte{42}
					vs := VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{NodePubkey: voteCommissionTestPubkey(74), AuthorizedWithdrawer: withdrawer, InflationRewardsCollector: vote, BlockRevenueCollector: withdrawer, InflationRewardsCommissionBps: 1234, BlockRevenueCommissionBps: 5678, PendingDelegatorRewards: 999, BlsPubkeyCompressed: &bls, EpochCredits: []EpochCredits{{Epoch: 1048, Credits: 10000, PrevCredits: 9000}}}}
					vs.V4.AuthorizedVoters.AuthorizedVoters.Set(1048, withdrawer)
					data := make([]byte, VoteStateV4Size)
					require.NoError(t, WriteVersionedVoteStateInPlace(data, &vs))
					accts := []accounts.Account{{Key: a.VoteProgramAddr, Owner: a.NativeLoaderAddr, Lamports: 1, Executable: true}, {Key: vote, Owner: a.VoteProgramAddr, Lamports: rent.MinimumBalance(uint64(len(data))), Data: data}, {Key: collector, Owner: a.SystemProgramAddr, Lamports: rent.MinimumBalance(0)}, {Key: withdrawer, Owner: a.SystemProgramAddr, Lamports: rent.MinimumBalance(0)}}
					metas := []AccountMeta{{Pubkey: vote, IsWritable: true}, {Pubkey: collector, IsWritable: true}, {Pubkey: withdrawer, IsSigner: true}}
					instr := binary.LittleEndian.AppendUint32(nil, 17)
					instr = binary.LittleEndian.AppendUint32(instr, kind)
					if tc.mutate != nil {
						tc.mutate(ft, accts, metas, &instr)
					}
					if tc.accountCount != 0 {
						metas = metas[:tc.accountCount]
					}
					before := append([]byte(nil), accts[1].Data...)
					txCtx := NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64)
					txCtx.Rent = rent
					execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *ft}
					err := execCtx.ProcessInstruction(instr, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0})
					after, getErr := txCtx.Accounts.GetAccount(1)
					require.NoError(t, getErr)
					if tc.want != nil {
						require.ErrorIs(t, err, tc.want)
						require.Equal(t, before, after.Data)
						return
					}
					require.NoError(t, err)
					expected := append([]byte(nil), before...)
					offset := 68 + int(kind)*32
					copy(expected[offset:offset+32], metas[1].Pubkey[:])
					require.Equal(t, expected, after.Data, "preserve every other V4 field and trailing byte")
				})
			}
		})
	}
}

func TestVoteUpdateCommissionCollectorMigratesLegacyState(t *testing.T) {
	for _, version := range []uint32{VoteStateVersionV1_14_11, VoteStateVersionCurrent} {
		for _, kind := range []uint32{CommissionKindInflationRewards, CommissionKindBlockRevenue} {
			t.Run(fmt.Sprintf("version_%d_kind_%d", version, kind), func(t *testing.T) {
				rent := NewDefaultRentSysvar()
				previousRent := SysvarCache.Rent
				SysvarCache.Rent.Sysvar = &rent
				t.Cleanup(func() { SysvarCache.Rent = previousRent })
				vote, collector, withdrawer, node := voteCommissionTestPubkey(81), voteCommissionTestPubkey(82), voteCommissionTestPubkey(83), voteCommissionTestPubkey(84)
				current := newVoteStateFromVoteInit(VoteInstrVoteInit{NodePubkey: node, AuthorizedVoter: node, AuthorizedWithdrawer: withdrawer, Commission: 25}, SysvarClock{Epoch: 1048})
				versioned := VoteStateVersions{Type: version}
				size := VoteStateV3Size
				if version == VoteStateVersionV1_14_11 {
					versioned.V1_14_11 = *newVoteState1_14_11FromCurrent(current)
					size = VoteStateV2Size
				} else {
					versioned.Current = *current
				}
				data := make([]byte, size)
				require.NoError(t, WriteVersionedVoteStateInPlace(data, &versioned))
				accts := []accounts.Account{
					{Key: a.VoteProgramAddr, Owner: a.NativeLoaderAddr, Lamports: 1, Executable: true},
					{Key: vote, Owner: a.VoteProgramAddr, Lamports: rent.MinimumBalance(VoteStateV4Size), Data: data},
					{Key: collector, Owner: a.SystemProgramAddr, Lamports: rent.MinimumBalance(0)},
					{Key: withdrawer},
				}
				metas := []AccountMeta{{Pubkey: vote, IsWritable: true}, {Pubkey: collector, IsWritable: true}, {Pubkey: withdrawer, IsSigner: true}}
				txCtx := NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64)
				txCtx.Rent = rent
				ft := features.NewFeaturesDefault()
				ft.EnableFeature(features.VoteStateV4, 0)
				ft.EnableFeature(features.CustomCommissionCollector, 0)
				execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *ft}
				instr := binary.LittleEndian.AppendUint32(nil, 17)
				instr = binary.LittleEndian.AppendUint32(instr, kind)
				require.NoError(t, execCtx.ProcessInstruction(instr, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0}))
				after, err := txCtx.Accounts.GetAccount(1)
				require.NoError(t, err)
				require.Len(t, after.Data, VoteStateV4Size)
				result, err := UnmarshalVersionedVoteState(after.Data)
				require.NoError(t, err)
				require.Equal(t, uint32(VoteStateVersionV4), result.Type)
				require.Equal(t, node, result.V4.NodePubkey)
				require.Equal(t, withdrawer, result.V4.AuthorizedWithdrawer)
				require.Equal(t, uint16(2500), result.V4.InflationRewardsCommissionBps)
				require.Equal(t, uint16(10000), result.V4.BlockRevenueCommissionBps)
				require.Zero(t, result.V4.PendingDelegatorRewards)
				require.Nil(t, result.V4.BlsPubkeyCompressed)
				if kind == CommissionKindInflationRewards {
					require.Equal(t, collector, result.V4.InflationRewardsCollector)
					require.Equal(t, node, result.V4.BlockRevenueCollector)
				} else {
					require.Equal(t, vote, result.V4.InflationRewardsCollector)
					require.Equal(t, collector, result.V4.BlockRevenueCollector)
				}
			})
		}
	}
}
