package txverify

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSanitizeTransactionUniversalSignatureLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(limitTestVersionName(version), func(t *testing.T) {
			tx := limitTestTransaction(t, version, MaxSignaturesPerTransaction, 0, 0)
			require.NoError(t, SanitizeTransaction(tx))

			tx = limitTestTransaction(t, version, MaxSignaturesPerTransaction+1, 0, 0)
			err := SanitizeTransaction(tx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "too many signatures")
		})
	}
}

func TestSanitizeTransactionUniversalInstructionLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(limitTestVersionName(version), func(t *testing.T) {
			tx := limitTestTransaction(t, version, 1, MaxInstructionsPerMessage, 0)
			require.NoError(t, SanitizeTransaction(tx))

			tx = limitTestTransaction(t, version, 1, MaxInstructionsPerMessage+1, 0)
			err := SanitizeTransaction(tx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "too many instructions")
		})
	}
}

func TestSanitizeTransactionUniversalAccountsPerInstructionLimit(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0} {
		t.Run(limitTestVersionName(version), func(t *testing.T) {
			tx := limitTestTransaction(t, version, 1, 1, MaxAccountsPerInstruction)
			require.NoError(t, SanitizeTransaction(tx))

			tx = limitTestTransaction(t, version, 1, 1, MaxAccountsPerInstruction+1)
			err := SanitizeTransaction(tx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "too many accounts")
		})
	}
}

func limitTestTransaction(
	t *testing.T,
	version solana.MessageVersion,
	numSignatures int,
	numInstructions int,
	accountsPerInstruction int,
) *solana.Transaction {
	t.Helper()
	require.Contains(t, []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0}, version)
	require.Greater(t, numSignatures, 0)

	numKeys := numSignatures
	if numInstructions > 0 && numKeys < 2 {
		numKeys = 2
	}
	keys := make(solana.PublicKeySlice, numKeys)
	for i := range keys {
		keys[i][0] = byte(i + 1)
	}
	header := solana.MessageHeader{
		NumRequiredSignatures:     uint8(numSignatures),
		NumReadonlySignedAccounts: uint8(numSignatures - 1),
	}
	if numKeys > numSignatures {
		header.NumReadonlyUnsignedAccounts = uint8(numKeys - numSignatures)
	}

	instructions := make([]solana.CompiledInstruction, numInstructions)
	for i := range instructions {
		instructions[i].ProgramIDIndex = uint16(numKeys - 1)
		instructions[i].Accounts = make([]uint16, accountsPerInstruction)
	}
	msg := solana.Message{
		Header:          header,
		AccountKeys:     keys,
		RecentBlockhash: solana.Hash{},
		Instructions:    instructions,
	}
	_, err := msg.SetVersion(version)
	require.NoError(t, err)
	return &solana.Transaction{
		Message:    msg,
		Signatures: make([]solana.Signature, numSignatures),
	}
}

func limitTestVersionName(version solana.MessageVersion) string {
	if version == solana.MessageVersionV0 {
		return "v0"
	}
	return "legacy"
}
