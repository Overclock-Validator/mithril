package replay

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parentStateReader map[solana.PublicKey]*accounts.Account

func (r parentStateReader) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := r[pubkey]; ok {
		return acct.Clone(), nil
	}
	return nil, errors.New("account not found")
}

func TestHandleFailedTxAdvancesNonceReadFromParentState(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	previousRecent := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	emptyRecent := sealevel.SysvarRecentBlockhashes{}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &emptyRecent
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = previousRecent }()

	authority := txfixture.PayerPubkey()
	nonceKey := solana.PublicKey{0xD6}
	durableNonce := [32]byte{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     authority,
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	// Like a leader bank, the slot has not written the nonce account yet; only
	// parent state has it.
	parent := parentStateReader{nonceKey: {
		Key: nonceKey, Lamports: 1_500_000, Owner: addresses.SystemProgramAddr,
		Data: nonceData, RentEpoch: math.MaxUint64,
	}}
	slotCtx.UnrootedRead = parent
	_, err = slotCtx.GetAccount(nonceKey)
	require.Error(t, err)
	slotCtx.LastBlockhash = [32]byte{0x77}

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	tx.Message.RecentBlockhash = durableNonce
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	advance := sealevel.Instruction{
		ProgramId: addresses.SystemProgramAddr,
		Accounts: []sealevel.AccountMeta{
			{Pubkey: nonceKey, IsWritable: true},
			{Pubkey: authority, IsSigner: true},
		},
		Data: data,
	}

	feeInfo, _ := handleFailedTx(slotCtx, tx, []sealevel.Instruction{advance}, &sealevel.ComputeBudgetLimits{},
		sealevel.InstrErrInvalidArgument, nil)
	require.NotNil(t, feeInfo)
	require.Contains(t, slotCtx.ModifiedAccts, nonceKey, "a failed durable-nonce transaction advances its nonce")
	advanced, err := slotCtx.GetAccount(nonceKey)
	require.NoError(t, err)
	state, err := sealevel.UnmarshalNonceStateVersions(advanced.Data)
	require.NoError(t, err)
	assert.NotEqual(t, durableNonce, [32]byte(state.State().DurableNonce))
	assert.Equal(t, uint64(1_500_000), advanced.Lamports)
	original, err := parent.GetAccount(slotCtx.Slot, nonceKey)
	require.NoError(t, err)
	assert.Equal(t, nonceData, original.Data, "parent state is not modified")
}

func TestHandleFailedTxSkipsNonceMissingEverywhere(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	previousRecent := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	emptyRecent := sealevel.SysvarRecentBlockhashes{}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &emptyRecent
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = previousRecent }()
	slotCtx.UnrootedRead = parentStateReader{}
	nonceKey := solana.PublicKey{0xD8}

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	tx.Message.RecentBlockhash = [32]byte{0xAA}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	advance := sealevel.Instruction{
		ProgramId: addresses.SystemProgramAddr,
		Accounts: []sealevel.AccountMeta{
			{Pubkey: nonceKey, IsWritable: true},
			{Pubkey: txfixture.PayerPubkey(), IsSigner: true},
		},
		Data: data,
	}

	feeInfo, _ := handleFailedTx(slotCtx, tx, []sealevel.Instruction{advance}, &sealevel.ComputeBudgetLimits{},
		sealevel.InstrErrInvalidArgument, nil)
	require.NotNil(t, feeInfo, "the fee is still charged")
	assert.NotContains(t, slotCtx.ModifiedAccts, nonceKey)
	_, err = slotCtx.GetAccount(nonceKey)
	assert.Error(t, err, "no nonce account is created")
}
