package txfixture

import (
	"crypto/ed25519"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

const ReadonlyPairPoolSize = 128
const ReadonlyPairCapacity = ReadonlyPairPoolSize * (ReadonlyPairPoolSize - 1)

// ReadonlyPairWire builds the 198-byte, single-signature, zero-instruction
// workload used for leader packing tests. Varying ordered pairs of existing
// readonly accounts gives distinct messages without adding instructions or
// forcing a lookup of a new nonexistent account for every transaction.
// Reusing an ordinal requires a different payer or recent blockhash.
func ReadonlyPairWire(key ed25519.PrivateKey, hash solana.Hash, pool []solana.PublicKey, ordinal int) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize || len(pool) != ReadonlyPairPoolSize || ordinal < 0 || ordinal >= ReadonlyPairCapacity {
		return nil, fmt.Errorf("invalid readonly-pair fixture key, pool or ordinal")
	}
	n := ordinal * 7919 % ReadonlyPairCapacity
	a, b := n/(ReadonlyPairPoolSize-1), n%(ReadonlyPairPoolSize-1)
	if b >= a {
		b++
	}
	payer := solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
	if payer == pool[a] || payer == pool[b] || pool[a] == pool[b] {
		return nil, fmt.Errorf("readonly-pair accounts must be distinct")
	}
	message := make([]byte, 0, 133)
	message = append(message, 1, 0, 2, 3)
	message = append(message, payer[:]...)
	message = append(message, pool[a][:]...)
	message = append(message, pool[b][:]...)
	message = append(message, hash[:]...)
	message = append(message, 0)
	wire := make([]byte, 0, 198)
	wire = append(wire, 1)
	wire = append(wire, ed25519.Sign(key, message)...)
	return append(wire, message...), nil
}
