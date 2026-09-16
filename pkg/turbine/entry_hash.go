package turbine

import (
	"crypto/sha256"

	"github.com/Overclock-Validator/mithril/pkg/merkletree"
	"github.com/gagliardetto/solana-go"
)

const alpenglowHashesPerTick = ^uint64(0) // Alpenglow low-power mode (hashes_per_tick unset)

// NextAlpenglowEntryHash computes the hash for the next ledger entry in Alpenglow
// low-power mode. This is not PoH timing consensus — only the entry hash chain
// validators still verify (Alpentick + tx batches, num_hashes=1).
func NextAlpenglowEntryHash(start solana.Hash, numHashes uint64, txns []solana.Transaction) solana.Hash {
	if numHashes == 0 && len(txns) == 0 {
		return start
	}
	var state alpenglowEntryState
	state.reset(start)
	state.advance(numHashes - 1)
	if len(txns) == 0 {
		return state.tick().hash
	}
	return state.record(hashTransactions(txns)).hash
}

// AlpentickHash returns the ending-tick entry hash after all prior entries in the slot.
func AlpentickHash(priorEntryHash solana.Hash) solana.Hash {
	return NextAlpenglowEntryHash(priorEntryHash, 1, nil)
}

type alpenglowEntry struct {
	numHashes uint64
	hash      solana.Hash
}

type alpenglowEntryState struct {
	tip                      solana.Hash
	numHashes                uint64
	remainingHashesUntilTick uint64
	hashesPerTick            uint64
}

func (s *alpenglowEntryState) reset(start solana.Hash) {
	s.tip = start
	s.numHashes = 0
	s.hashesPerTick = alpenglowHashesPerTick
	s.remainingHashesUntilTick = alpenglowHashesPerTick
}

func (s *alpenglowEntryState) advance(maxNumHashes uint64) {
	numHashes := maxNumHashes
	if s.remainingHashesUntilTick-1 < numHashes {
		numHashes = s.remainingHashesUntilTick - 1
	}
	for i := uint64(0); i < numHashes; i++ {
		s.tip = sha256Hash(s.tip[:])
	}
	s.numHashes += numHashes
	s.remainingHashesUntilTick -= numHashes
}

func (s *alpenglowEntryState) tick() alpenglowEntry {
	s.tip = sha256Hash(s.tip[:])
	s.numHashes++
	s.remainingHashesUntilTick--
	numHashes := s.numHashes
	s.remainingHashesUntilTick = s.hashesPerTick
	s.numHashes = 0
	return alpenglowEntry{numHashes: numHashes, hash: s.tip}
}

func (s *alpenglowEntryState) record(mixin solana.Hash) alpenglowEntry {
	s.tip = hashv([][]byte{s.tip[:], mixin[:]})
	numHashes := s.numHashes + 1
	s.numHashes = 0
	s.remainingHashesUntilTick--
	return alpenglowEntry{numHashes: numHashes, hash: s.tip}
}

func hashTransactions(txns []solana.Transaction) solana.Hash {
	sigs := make([][]byte, 0, len(txns))
	for i := range txns {
		for j := range txns[i].Signatures {
			sigs = append(sigs, txns[i].Signatures[j][:])
		}
	}
	return hashSignatures(sigs)
}

func hashSignatures(signatures [][]byte) solana.Hash {
	return solana.Hash(merkletree.HashRoot(signatures))
}

func sha256Hash(data []byte) solana.Hash {
	sum := sha256.Sum256(data)
	return solana.Hash(sum)
}
