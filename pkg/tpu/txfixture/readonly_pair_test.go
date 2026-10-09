package txfixture

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Reproduce the full preloaded four-slot experiment without RPC, funds or sends.
func TestReadonlyPair200KDistinctMessages(t *testing.T) {
	keys := make([]ed25519.PrivateKey, 8)
	for i := range keys {
		seed := sha256.Sum256([]byte{byte(i), 73})
		keys[i] = ed25519.NewKeyFromSeed(seed[:])
	}
	pool := make([]solana.PublicKey, ReadonlyPairPoolSize)
	for i := range pool {
		pool[i] = solana.PublicKey{byte(i + 1), 77}
	}
	seen := make(map[[32]byte]struct{}, 200000)
	for i := 0; i < 200000; i++ {
		hash, index := solana.Hash{1}, i
		if i >= 120000 {
			hash, index = solana.Hash{2}, i-120000
		}
		wire, err := ReadonlyPairWire(keys[index%8], hash, pool, index/8)
		require.NoError(t, err)
		require.Len(t, wire, 198)
		h := sha256.Sum256(wire[65:])
		if _, ok := seen[h]; ok {
			t.Fatalf("duplicate message %d", i)
		}
		seen[h] = struct{}{}
		if i%ReadonlyPairCapacity == 0 || i == 119999 || i == 120000 || i == 199999 {
			tx, err := solana.TransactionFromBytes(wire)
			require.NoError(t, err)
			require.Len(t, tx.Signatures, 1)
			require.Empty(t, tx.Message.Instructions)
			require.Equal(t, hash, tx.Message.RecentBlockhash)
			require.True(t, ed25519.Verify(ed25519.PublicKey(tx.Message.AccountKeys[0][:]), wire[65:], wire[1:65]))
		}
	}
}

func TestReadonlyPairRejectsInvalidInputs(t *testing.T) {
	key := ed25519.PrivateKey(PayerPrivateKey())
	pool := make([]solana.PublicKey, ReadonlyPairPoolSize)
	for i := range pool {
		pool[i] = solana.PublicKey{byte(i + 1), 77}
	}
	for _, ordinal := range []int{-1, ReadonlyPairCapacity} {
		_, err := ReadonlyPairWire(key, TestBlockhash(), pool, ordinal)
		require.Error(t, err)
	}
	_, err := ReadonlyPairWire(nil, TestBlockhash(), pool, 0)
	require.Error(t, err)
	_, err = ReadonlyPairWire(key, TestBlockhash(), nil, 0)
	require.Error(t, err)
	pool[0] = solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
	_, err = ReadonlyPairWire(key, TestBlockhash(), pool, 0)
	require.Error(t, err)
	pool[0], pool[1] = solana.PublicKey{9}, solana.PublicKey{9}
	_, err = ReadonlyPairWire(key, TestBlockhash(), pool, 0)
	require.Error(t, err)
}
