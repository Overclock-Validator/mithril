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
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVoteUpdateCommissionBpsSlot451852192(t *testing.T) {
	var fixture struct {
		Slot         uint64
		Transaction  string
		VoteAccount  accounts.Account `json:"vote_account"`
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
	data, err := os.ReadFile("testdata/slot_451852192_commission_bps.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixture))
	wire, err := base64.StdEncoding.DecodeString(fixture.Transaction)
	require.NoError(t, err)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.NoError(t, tx.VerifySignatures())
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.VoteStateV4, 0)
	ft.EnableFeature(features.CommissionRateInBasisPoints, 0)
	ft.EnableFeature(features.DelayCommissionUpdates, 0)
	txAccts := make([]accounts.Account, len(tx.Message.AccountKeys))
	for i, key := range tx.Message.AccountKeys {
		txAccts[i] = accounts.Account{Key: key, Owner: a.SystemProgramAddr, Lamports: 1}
		if key == fixture.VoteAccount.Key {
			txAccts[i] = fixture.VoteAccount
		}
		if key == a.VoteProgramAddr {
			txAccts[i] = accounts.Account{Key: key, Lamports: 1, Owner: a.NativeLoaderAddr, Executable: true}
		}
	}
	before := txAccts[2].Clone()
	txCtx := NewTransactionCtx(*NewTransactionAccounts(txAccts), 5, 64)
	txCtx.Rent = NewDefaultRentSysvar()
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
	after, err := txCtx.Accounts.GetAccount(2)
	require.NoError(t, err)
	// The accepted instruction changes only the two-byte inflation commission to 5000 bps.
	expected := before.Clone()
	binary.LittleEndian.PutUint16(expected.Data[132:134], 5000)
	require.Equal(t, expected.Data, after.Data)
	require.Equal(t, before.Lamports, after.Lamports)
	require.Equal(t, uint64(2100), execCtx.ComputeMeter.Used())
	require.Equal(t, uint16(5000), execCtx.ModifiedVoteStates[before.Key].V4.InflationRewardsCommissionBps)

	// The full-block replay reproduces the crash hash. Replacing just this
	// commission in its post-reward account state must recover the certified
	// footer hash, proving the missing update explains the entire divergence.
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
	copy(fixedVote.Data[132:134], after.Data[132:134])
	lt.Sub(new(lthash.LtHash).InitWithAcct(&evidence.BrokenVote))
	lt.Add(new(lthash.LtHash).InitWithAcct(fixedVote))
	require.Equal(t, evidence.ExpectedBankhash, bankhash())
}

func TestVoteUpdateCommissionBps(t *testing.T) {
	for _, kind := range []uint32{CommissionKindInflationRewards, CommissionKindBlockRevenue} {
		t.Run(fmt.Sprintf("kind_%d", kind), func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				bps    uint16
				mutate func(*features.Features, []accounts.Account, *[]AccountMeta, *[]byte)
				want   error
			}{
				{name: "zero"},
				{name: "fractional_percent", bps: 123},
				{name: "increase", bps: 9000},
				{name: "full_commission", bps: 10000},
				{name: "above_100_percent", bps: 15000},
				{name: "max_u16", bps: 65535},
				{name: "bps_disabled", mutate: func(f *features.Features, _ []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					f.DisableFeature(features.CommissionRateInBasisPoints)
				}, want: InstrErrInvalidInstructionData},
				{name: "delay_disabled", mutate: func(f *features.Features, _ []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					f.DisableFeature(features.DelayCommissionUpdates)
				}, want: InstrErrInvalidInstructionData},
				{name: "block_revenue_sharing_disabled", mutate: func(f *features.Features, _ []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					f.DisableFeature(features.BlockRevenueSharing)
				}, want: func() error {
					if kind == CommissionKindBlockRevenue {
						return InstrErrInvalidInstructionData
					}
					return nil
				}()},
				{name: "unknown_kind", mutate: func(_ *features.Features, _ []accounts.Account, _ *[]AccountMeta, d *[]byte) {
					binary.LittleEndian.PutUint32((*d)[6:], 2)
				}, want: InstrErrInvalidInstructionData},
				{name: "truncated_bps", mutate: func(_ *features.Features, _ []accounts.Account, _ *[]AccountMeta, d *[]byte) {
					*d = (*d)[:5]
				}, want: InstrErrInvalidInstructionData},
				{name: "truncated_kind", mutate: func(_ *features.Features, _ []accounts.Account, _ *[]AccountMeta, d *[]byte) {
					*d = (*d)[:9]
				}, want: InstrErrInvalidInstructionData},
				{name: "missing_signature", mutate: func(_ *features.Features, _ []accounts.Account, m *[]AccountMeta, _ *[]byte) {
					(*m)[1].IsSigner = false
				}, want: InstrErrMissingRequiredSignature},
				{name: "wrong_signer", mutate: func(_ *features.Features, _ []accounts.Account, m *[]AccountMeta, _ *[]byte) {
					(*m)[1].IsSigner = false
					(*m)[0].IsSigner = true
				}, want: InstrErrMissingRequiredSignature},
				{name: "missing_authority_account", mutate: func(_ *features.Features, _ []accounts.Account, m *[]AccountMeta, _ *[]byte) {
					*m = (*m)[:1]
				}, want: InstrErrMissingRequiredSignature},
				{name: "vote_readonly", mutate: func(_ *features.Features, _ []accounts.Account, m *[]AccountMeta, _ *[]byte) {
					(*m)[0].IsWritable = false
				}, want: InstrErrReadonlyDataModified},
				{name: "wrong_owner", mutate: func(_ *features.Features, a []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					a[1].Owner = a[2].Owner
				}, want: InstrErrInvalidAccountOwner},
				{name: "invalid_vote_data", mutate: func(_ *features.Features, a []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					a[1].Data = []byte{99}
				}, want: InstrErrInvalidAccountData},
				{name: "retired_v0", mutate: func(_ *features.Features, a []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					a[1].Data = make([]byte, VoteStateV4Size)
				}, want: InstrErrInvalidAccountData},
				{name: "v4_without_authorized_voters", mutate: func(_ *features.Features, a []accounts.Account, _ *[]AccountMeta, _ *[]byte) {
					v, err := UnmarshalVersionedVoteState(a[1].Data)
					require.NoError(t, err)
					v.V4.AuthorizedVoters = AuthorizedVoters{}
					require.NoError(t, WriteVersionedVoteStateInPlace(a[1].Data, v))
				}},
				{name: "trailing_bytes", mutate: func(_ *features.Features, _ []accounts.Account, _ *[]AccountMeta, d *[]byte) {
					*d = append(*d, 1, 2, 3)
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ft := features.NewFeaturesDefault()
					for _, gate := range []features.FeatureGate{features.VoteStateV4, features.CommissionRateInBasisPoints, features.DelayCommissionUpdates, features.BlockRevenueSharing} {
						ft.EnableFeature(gate, 0)
					}
					vote, withdrawer := voteCommissionTestPubkey(91), voteCommissionTestPubkey(92)
					bls := [48]byte{42}
					state := VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{
						NodePubkey: voteCommissionTestPubkey(93), AuthorizedWithdrawer: withdrawer,
						InflationRewardsCollector: voteCommissionTestPubkey(94), BlockRevenueCollector: voteCommissionTestPubkey(95),
						InflationRewardsCommissionBps: 1234, BlockRevenueCommissionBps: 5678,
						PendingDelegatorRewards: 999, BlsPubkeyCompressed: &bls,
						EpochCredits: []EpochCredits{{Epoch: 1058, Credits: 10000, PrevCredits: 9000}},
					}}
					state.V4.AuthorizedVoters.AuthorizedVoters.Set(1058, withdrawer)
					data := make([]byte, VoteStateV4Size)
					require.NoError(t, WriteVersionedVoteStateInPlace(data, &state))
					data[len(data)-1] = 42 // Preserve bytes outside the serialized state too.
					accts := []accounts.Account{
						{Key: a.VoteProgramAddr, Owner: a.NativeLoaderAddr, Lamports: 1, Executable: true},
						{Key: vote, Owner: a.VoteProgramAddr, Lamports: 100000000, Data: data},
						{Key: withdrawer, Owner: a.SystemProgramAddr, Lamports: 1},
					}
					metas := []AccountMeta{{Pubkey: vote, IsWritable: true}, {Pubkey: withdrawer, IsSigner: true}}
					instr := binary.LittleEndian.AppendUint32(nil, 18)
					instr = binary.LittleEndian.AppendUint16(instr, tc.bps)
					instr = binary.LittleEndian.AppendUint32(instr, kind)
					if tc.mutate != nil {
						tc.mutate(ft, accts, &metas, &instr)
					}
					before := accts[1].Clone()
					txCtx := NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64)
					txCtx.Rent = NewDefaultRentSysvar()
					execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *ft}
					err := execCtx.ProcessInstruction(instr, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0})
					if tc.want != nil {
						require.ErrorIs(t, err, tc.want)
						require.Equal(t, before.Data, txCtx.Accounts.Accounts[1].Data)
						return
					}
					require.NoError(t, err)
					after, getErr := txCtx.Accounts.GetAccount(1)
					require.NoError(t, getErr)
					expected := before.Clone()
					binary.LittleEndian.PutUint16(expected.Data[132+kind*2:], tc.bps)
					require.Equal(t, expected.Data, after.Data, "only the selected commission changes")
					require.Equal(t, before.Lamports, after.Lamports)
					require.Equal(t, uint64(2100), execCtx.ComputeMeter.Used())
					cached, err := marshalVersionedVoteState(execCtx.ModifiedVoteStates[vote])
					require.NoError(t, err)
					require.Equal(t, after.Data[:len(cached)], cached, "publish the changed vote state to replay")
				})
			}
		})
	}
}

func TestVoteUpdateCommissionBpsMigratesLegacyState(t *testing.T) {
	for _, version := range []uint32{VoteStateVersionV1_14_11, VoteStateVersionCurrent} {
		for _, kind := range []uint32{CommissionKindInflationRewards, CommissionKindBlockRevenue} {
			for _, initialized := range []bool{false, true} {
				t.Run(fmt.Sprintf("version_%d_kind_%d_initialized_%t", version, kind, initialized), func(t *testing.T) {
					rent := NewDefaultRentSysvar()
					vote, withdrawer, node := voteCommissionTestPubkey(81), voteCommissionTestPubkey(82), voteCommissionTestPubkey(83)
					current := newVoteStateFromVoteInit(VoteInstrVoteInit{NodePubkey: node, AuthorizedVoter: node, AuthorizedWithdrawer: withdrawer, Commission: 25}, SysvarClock{Epoch: 1058})
					if !initialized {
						current.AuthorizedVoters = AuthorizedVoters{}
					}
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
						{Key: withdrawer, Owner: a.SystemProgramAddr, Lamports: 1},
					}
					metas := []AccountMeta{{Pubkey: vote, IsWritable: true}, {Pubkey: withdrawer, IsSigner: true}}
					txCtx := NewTransactionCtx(*NewTransactionAccounts(accts), 5, 64)
					txCtx.Rent = rent
					ft := features.NewFeaturesDefault()
					for _, gate := range []features.FeatureGate{features.VoteStateV4, features.CommissionRateInBasisPoints, features.DelayCommissionUpdates, features.BlockRevenueSharing} {
						ft.EnableFeature(gate, 0)
					}
					execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *ft}
					instr := binary.LittleEndian.AppendUint32(nil, 18)
					instr = binary.LittleEndian.AppendUint16(instr, 5678)
					instr = binary.LittleEndian.AppendUint32(instr, kind)
					err := execCtx.ProcessInstruction(instr, InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), []uint64{0})
					after, getErr := txCtx.Accounts.GetAccount(1)
					require.NoError(t, getErr)
					if !initialized {
						require.ErrorIs(t, err, InstrErrUninitializedAccount)
						require.Equal(t, data, after.Data)
						return
					}
					require.NoError(t, err)
					require.Len(t, after.Data, VoteStateV4Size)
					result, err := UnmarshalVersionedVoteState(after.Data)
					require.NoError(t, err)
					require.Equal(t, uint32(VoteStateVersionV4), result.Type)
					require.Equal(t, node, result.V4.NodePubkey)
					require.Equal(t, withdrawer, result.V4.AuthorizedWithdrawer)
					require.Equal(t, vote, result.V4.InflationRewardsCollector)
					require.Equal(t, node, result.V4.BlockRevenueCollector)
					require.Zero(t, result.V4.PendingDelegatorRewards)
					require.Nil(t, result.V4.BlsPubkeyCompressed)
					if kind == CommissionKindInflationRewards {
						require.Equal(t, uint16(5678), result.V4.InflationRewardsCommissionBps)
						require.Equal(t, uint16(10000), result.V4.BlockRevenueCommissionBps)
					} else {
						require.Equal(t, uint16(2500), result.V4.InflationRewardsCommissionBps)
						require.Equal(t, uint16(5678), result.V4.BlockRevenueCommissionBps)
					}
				})
			}
		}
	}
}
