package sealevel

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func marshalNonceCurrentState(t *testing.T, durableNonce [32]byte, authority solana.PublicKey) []byte {
	t.Helper()
	nsv := &NonceStateVersions{
		Type: NonceVersionCurrent,
		Current: NonceData{
			IsInitialized: true,
			Authority:     authority,
			DurableNonce:  durableNonce,
			FeeCalculator: FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	data, err := nsv.Marshal()
	require.NoError(t, err)
	return data
}

func makeAdvanceNonceAccountInstr(noncePk, authorityPk solana.PublicKey) Instruction {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, SystemProgramInstrTypeAdvanceNonceAccount)
	return Instruction{
		ProgramId: a.SystemProgramAddr,
		Accounts: []AccountMeta{
			{Pubkey: noncePk, IsSigner: false, IsWritable: true},
			{Pubkey: authorityPk, IsSigner: true, IsWritable: false},
		},
		Data: data,
	}
}

// withEmptyRecentBlockhashesSysvar swaps the global cache for an empty
// queue and returns a deferrable restore func. IsBlockhashAgeValid then
// returns false for any input, forcing the durable-nonce path.
func withEmptyRecentBlockhashesSysvar(t *testing.T) func() {
	t.Helper()
	prev := SysvarCache.RecentBlockHashes.Sysvar
	empty := SysvarRecentBlockhashes{}
	SysvarCache.RecentBlockHashes.Sysvar = &empty
	return func() { SysvarCache.RecentBlockHashes.Sysvar = prev }
}

func publicKeyForTest(kind byte, seed byte) solana.PublicKey {
	var pk solana.PublicKey
	pk[0] = kind
	pk[31] = seed
	return pk
}

func TestIsTransactionAgeValid_NonceInMemAccounts_ReturnsTrue(t *testing.T) {
	defer withEmptyRecentBlockhashesSysvar(t)()

	authority := publicKeyForTest('A', 1)
	noncePk := publicKeyForTest('N', 2)
	durable := [32]byte{0xAA}

	tx := &solana.Transaction{Message: solana.Message{RecentBlockhash: durable}}
	instrs := []Instruction{makeAdvanceNonceAccountInstr(noncePk, authority)}

	mem := accounts.NewMemAccounts()
	require.NoError(t, mem.SetAccountWithoutLock(noncePk, &accounts.Account{
		Key:   noncePk,
		Owner: a.SystemProgramAddr,
		Data:  marshalNonceCurrentState(t, durable, authority),
	}))

	slotCtx := &SlotCtx{Accounts: mem, LastBlockhash: [32]byte{0x77}}
	assert.True(t, IsTransactionAgeValid(tx, instrs, slotCtx))
}

func TestIsTransactionAgeValid_NonceMissing_NilAccountsDb_ReturnsFalse(t *testing.T) {
	defer withEmptyRecentBlockhashesSysvar(t)()

	tx := &solana.Transaction{Message: solana.Message{RecentBlockhash: [32]byte{0xAA}}}
	instrs := []Instruction{makeAdvanceNonceAccountInstr(publicKeyForTest('N', 2), publicKeyForTest('A', 1))}

	slotCtx := &SlotCtx{
		Accounts:      accounts.NewMemAccounts(),
		LastBlockhash: [32]byte{0x77},
	}

	require.NotPanics(t, func() {
		assert.False(t, IsTransactionAgeValid(tx, instrs, slotCtx))
	})
}

func TestTransactionAgeUsesBankRecentBlockhashQueue(t *testing.T) {
	previous := SysvarCache.RecentBlockHashes
	t.Cleanup(func() { SysvarCache.RecentBlockHashes = previous })
	globalHash := [32]byte{0xA1}
	globalRecent := SysvarRecentBlockhashes{{Blockhash: globalHash}}
	SysvarCache.RecentBlockHashes.Sysvar = &globalRecent

	bankHash := [32]byte{0xB2}
	bankRecent := SysvarRecentBlockhashes{{Blockhash: bankHash}}
	bankSnapshot, err := NewBankSysvars(42, &accounts.Account{
		Key: SysvarRecentBlockHashesAddr, Lamports: 1, Data: bankRecent.MustMarshal(),
	})
	require.NoError(t, err)
	slotCtx := &SlotCtx{Slot: 42, LatestEvictedBlockhash: [32]byte{0xC3}}
	require.NoError(t, slotCtx.PublishBankSysvars(bankSnapshot))

	require.True(t, IsTransactionAgeValid(&solana.Transaction{
		Message: solana.Message{RecentBlockhash: bankHash},
	}, nil, slotCtx))
	require.False(t, IsTransactionAgeValid(&solana.Transaction{
		Message: solana.Message{RecentBlockhash: globalHash},
	}, nil, slotCtx), "a conflicting process-global queue must not leak into this bank")
	require.True(t, IsTransactionAgeValid(&solana.Transaction{
		Message: solana.Message{RecentBlockhash: slotCtx.LatestEvictedBlockhash},
	}, nil, slotCtx), "the bank-pinned 151st hash remains valid")
}
