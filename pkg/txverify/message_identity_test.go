package txverify

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func identitySignedTransaction(t *testing.T, version solana.MessageVersion) *solana.Transaction {
	t.Helper()
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	tx := &solana.Transaction{Message: solana.Message{
		Header:          solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys:     []solana.PublicKey{solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey)), {2}},
		RecentBlockhash: solana.Hash{3},
		Instructions:    []solana.CompiledInstruction{{ProgramIDIndex: 1, Accounts: []uint16{0}, Data: []byte{4}}},
	}}
	_, err := tx.Message.SetVersion(version)
	require.NoError(t, err)
	msg, err := MessageBytes(tx)
	require.NoError(t, err)
	tx.Signatures = []solana.Signature{solana.SignatureFromBytes(ed25519.Sign(key, msg))}
	return tx
}

func TestVerifiedMessageIdentityCanonicalVersionsAndFailures(t *testing.T) {
	wire, err := hex.DecodeString(rustV1FeeHeapTransaction)
	require.NoError(t, err)
	v1, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	txs := []*solana.Transaction{identitySignedTransaction(t, solana.MessageVersionLegacy), identitySignedTransaction(t, solana.MessageVersionV0), v1}
	bad := identitySignedTransaction(t, solana.MessageVersionLegacy)
	bad.Signatures[0][0] ^= 1
	txs = append(txs, bad, nil)
	errs := make([]error, len(txs))
	ids := make([]VerifiedMessageIdentity, len(txs))
	var verifier BatchVerifier
	verifier.VerifyWithMessageIdentities(txs, errs, ids)
	for i := range txs {
		id, ok := ids[i].ForTransaction(txs[i])
		if i >= 3 {
			require.Error(t, errs[i])
			require.False(t, ok)
			continue
		}
		require.NoError(t, errs[i])
		require.True(t, ok)
		want, err := txstatus.IdentityForTransaction(txs[i])
		require.NoError(t, err)
		require.Equal(t, want, id)
	}
	// Scratch reuse cannot invalidate a prior successful request, and reusing
	// an output lane for failure must not leave a usable old identity behind.
	saved := ids[0]
	verifier.VerifyWithMessageIdentities([]*solana.Transaction{bad}, errs[:1], ids[:1])
	_, ok := saved.ForTransaction(txs[0])
	require.True(t, ok)
	_, ok = ids[0].ForTransaction(txs[0])
	require.False(t, ok)
	copyTx := *txs[0]
	_, ok = saved.ForTransaction(&copyTx)
	require.False(t, ok)
	txs[0].Message.RecentBlockhash[0] ^= 1
	_, ok = saved.ForTransaction(txs[0])
	require.False(t, ok)
}
