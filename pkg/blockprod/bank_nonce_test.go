package blockprod

import (
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parentAccounts map[solana.PublicKey]*accounts.Account

func (p parentAccounts) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := p[pubkey]; ok {
		return acct.Clone(), nil
	}
	return nil, fmt.Errorf("account %s not found", pubkey)
}

func TestWorkingBankFeesOnlyDurableNonceAdvancesNonceFromParentState(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	nonceKey := solana.PublicKey{0xD7}
	durableNonce := solana.Hash{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     txfixture.PayerPubkey(),
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	// Like a live leader bank, the slot holds no copy of the nonce account;
	// it is only in parent state.
	env.SlotCtx.AccountsDb = &accountsdb.AccountsDb{}
	env.SlotCtx.UnrootedRead = parentAccounts{nonceKey: {
		Key: nonceKey, Lamports: 1_500_000, Owner: addresses.SystemProgramAddr,
		Data: nonceData, RentEpoch: math.MaxUint64,
	}}
	_, err = env.SlotCtx.GetAccount(nonceKey)
	require.Error(t, err)

	tx, err := solana.NewTransaction(
		[]solana.Instruction{
			system.NewAdvanceNonceAccountInstruction(nonceKey, solana.SysVarRecentBlockHashesPubkey, txfixture.PayerPubkey()).Build(),
			// A one-byte loaded-data limit makes this a processable fees-only
			// load failure, which the leader already includes.
			computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(),
			system.NewTransferInstruction(1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build(),
		},
		durableNonce,
		solana.TransactionPayer(txfixture.PayerPubkey()),
	)
	require.NoError(t, err)
	payerKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payerKey })
	require.NoError(t, err)
	preview := replay.LoadAndExecuteTransaction(replay.LoadAndExecuteTransactionInput{
		SlotCtx: env.SlotCtx, Transaction: tx, LeanResult: true,
	})
	require.NotNil(t, preview.ProcessingResult.TransactionError)
	require.Equal(t, replay.TransactionErrorMaxLoadedAccountsDataSizeExceeded, preview.ProcessingResult.TransactionError.ErrorType)

	result, reason := env.Bank.ForgeTransaction(tx, 0)
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	require.Contains(t, env.SlotCtx.ModifiedAccts, nonceKey, "replay advances the nonce of a failed durable-nonce transaction")
	advanced, err := env.SlotCtx.GetAccount(nonceKey)
	require.NoError(t, err)
	state, err := sealevel.UnmarshalNonceStateVersions(advanced.Data)
	require.NoError(t, err)
	assert.NotEqual(t, durableNonce, solana.Hash(state.State().DurableNonce))
}
