package txstatus

import (
	"fmt"

	"github.com/gagliardetto/solana-go"
	"github.com/zeebo/blake3"
)

const transactionMessageHashDomain = "solana-tx-message-v1"

// TransactionMessageIdentity is the immutable identity used by Agave's
// AlreadyProcessed checks. Signatures are deliberately excluded.
type TransactionMessageIdentity struct {
	MessageHash     [32]byte
	RecentBlockhash solana.Hash
}

// TransactionMessageHash hashes a transaction's canonical message bytes.
func TransactionMessageHash(tx *solana.Transaction) ([32]byte, error) {
	var messageHash [32]byte
	if tx == nil {
		return messageHash, fmt.Errorf("transaction is nil")
	}
	message, err := tx.Message.MarshalBinary()
	if err != nil {
		return messageHash, fmt.Errorf("serialize transaction message: %w", err)
	}

	return HashCanonicalMessage(message), nil
}

// HashCanonicalMessage hashes the exact canonical bytes used for transaction
// signature verification, including any message-version prefix.
func HashCanonicalMessage(message []byte) [32]byte {
	var messageHash [32]byte
	hasher := blake3.New()
	_, _ = hasher.Write([]byte(transactionMessageHashDomain))
	_, _ = hasher.Write(message)
	hasher.Sum(messageHash[:0])
	return messageHash
}

// IdentityForTransaction captures both components needed for a status-cache
// lookup so later phases never need to inspect or reserialize the message.
func IdentityForTransaction(tx *solana.Transaction) (TransactionMessageIdentity, error) {
	messageHash, err := TransactionMessageHash(tx)
	if err != nil {
		return TransactionMessageIdentity{}, err
	}
	return TransactionMessageIdentity{
		MessageHash:     messageHash,
		RecentBlockhash: tx.Message.RecentBlockhash,
	}, nil
}
