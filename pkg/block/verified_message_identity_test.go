package block

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVerifiedIdentityCacheOwnsStorageAndDoesNotSerializeTrust(t *testing.T) {
	tx := identityTestTransaction(1)
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	tx.Message.AccountKeys[0] = solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
	msg, err := tx.Message.MarshalBinary()
	require.NoError(t, err)
	tx.Signatures[0] = solana.SignatureFromBytes(ed25519.Sign(key, msg))
	blk := &Block{Transactions: []*solana.Transaction{tx}}
	ids := make([]txverify.VerifiedMessageIdentity, 1)
	errs := make([]error, 1)
	var verifier txverify.BatchVerifier
	verifier.VerifyWithMessageIdentities(blk.Transactions, errs, ids)
	require.NoError(t, errs[0])
	require.NoError(t, blk.CacheVerifiedTransactionMessageIdentities(ids))
	cached := blk.transactionDerivedState.messageIdentities
	require.NotNil(t, cached, "adoption must populate the cache before the first admission lookup")
	clear(ids)
	got, err := blk.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	require.Same(t, cached, got)
	copyBlock := *blk
	got, err = copyBlock.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	require.Same(t, cached, got)
	wire, err := json.Marshal(blk)
	require.NoError(t, err)
	var decoded Block
	require.NoError(t, json.Unmarshal(wire, &decoded))
	require.Nil(t, decoded.transactionDerivedState)
}
