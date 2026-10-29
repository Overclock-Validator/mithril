package replay

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// A deposit can fail after its System transfer succeeds. Replay must publish
// only the fee debit in that case, even when the source is also the fee payer.
func TestProcessTransactionDepositDelegatorRewards(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pending      uint64
		gateDisabled bool
		want         error
		cu           uint64
	}{
		{name: "success", pending: 123, cu: 2250},
		{name: "pending_overflow_after_transfer", pending: math.MaxUint64, want: sealevel.InstrErrArithmeticOverflow, cu: 2250},
		{name: "inactive_feature", gateDisabled: true, want: sealevel.InstrErrInvalidInstructionData, cu: 2100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			slotCtx.VoteTimestampMu = &sync.Mutex{}
			slotCtx.VoteTimestamps = make(map[solana.PublicKey]sealevel.BlockTimestamp)
			slotCtx.DeferVoteCachePublication = true
			for _, gate := range []features.FeatureGate{features.VoteStateV4, features.CommissionRateInBasisPoints, features.CustomCommissionCollector, features.BlockRevenueSharing} {
				slotCtx.Features.EnableFeature(gate, 0)
			}
			if tc.gateDisabled {
				slotCtx.Features.DisableFeature(features.BlockRevenueSharing)
			}
			payer := txfixture.PayerPubkey()
			voteKey := solana.PublicKey{0xD7}
			state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{
				NodePubkey: payer, AuthorizedWithdrawer: payer,
				InflationRewardsCollector: voteKey, BlockRevenueCollector: voteKey,
				PendingDelegatorRewards: tc.pending,
			}}
			state.V4.AuthorizedVoters.AuthorizedVoters.Set(0, payer)
			voteData := make([]byte, sealevel.VoteStateV4Size)
			require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(voteData, &state))
			rent := sealevel.NewDefaultRentSysvar()
			voteBefore := &accounts.Account{Key: voteKey, Owner: addresses.VoteProgramAddr, Data: voteData,
				Lamports: rent.MinimumBalance(uint64(len(voteData))), RentEpoch: math.MaxUint64}
			require.NoError(t, slotCtx.SetAccount(voteKey, voteBefore.Clone()))
			require.NoError(t, slotCtx.SetAccount(addresses.VoteProgramAddr, &accounts.Account{
				Key: addresses.VoteProgramAddr, Owner: addresses.NativeLoaderAddr, Executable: true, Lamports: 1, RentEpoch: math.MaxUint64,
			}))
			payerBefore, err := slotCtx.GetAccount(payer)
			require.NoError(t, err)
			const deposit = uint64(100000)
			data := binary.LittleEndian.AppendUint32(nil, 19)
			data = binary.LittleEndian.AppendUint64(data, deposit)
			tx, err := solana.NewTransaction([]solana.Instruction{solana.NewInstruction(addresses.VoteProgramAddr,
				solana.AccountMetaSlice{solana.Meta(voteKey).WRITE(), solana.Meta(payer).WRITE().SIGNER(), solana.Meta(addresses.SystemProgramAddr)}, data)},
				txfixture.TestBlockhash(), solana.TransactionPayer(payer))
			require.NoError(t, err)
			privateKey := txfixture.PayerPrivateKey()
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key == payer {
					return &privateKey
				}
				return nil
			})
			require.NoError(t, err)
			var sigverify sync.WaitGroup
			feeInfo, usedCU, processErr := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
			sigverify.Wait()
			require.NotNil(t, feeInfo)
			require.Equal(t, uint64(5000), feeInfo.TotalFee)
			require.Equal(t, tc.cu, usedCU)
			payerAfter, err := slotCtx.GetAccount(payer)
			require.NoError(t, err)
			voteAfter, err := slotCtx.GetAccount(voteKey)
			require.NoError(t, err)
			if tc.want != nil {
				require.ErrorIs(t, processErr, tc.want)
				require.Equal(t, voteBefore, voteAfter, "failed transfer effects must not be published")
				payerBefore.Lamports -= feeInfo.TotalFee
				require.Equal(t, payerBefore, payerAfter, "only the fee debit survives rollback")
				require.NotContains(t, slotCtx.ModifiedAccts, voteKey)
				require.Empty(t, slotCtx.PendingVoteCache)
			} else {
				require.NoError(t, processErr)
				require.Equal(t, payerBefore.Lamports-feeInfo.TotalFee-deposit, payerAfter.Lamports)
				require.Equal(t, voteBefore.Lamports+deposit, voteAfter.Lamports)
				state.V4.PendingDelegatorRewards += deposit
				require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(voteBefore.Data, &state))
				require.Equal(t, voteBefore.Data, voteAfter.Data)
				require.Contains(t, slotCtx.ModifiedAccts, voteKey)
				require.Equal(t, tc.pending+deposit, slotCtx.PendingVoteCache[voteKey].V4.PendingDelegatorRewards)
			}
		})
	}
}
