package txverify

import (
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// VerificationObservation attaches an outcome to an attempt observed before
// Ed25519 evaluation.
type VerificationObservation interface {
	RecordResult(valid bool)
}

// VerificationObserver receives the exact inputs that VerifyTransaction
// presents to Ed25519. BeginVerification runs immediately before evaluation
// and its result hook immediately afterward, so invalid signatures do not
// cause later, unattempted signatures to be reported. Implementations are
// intended for opt-in measurement only.
type VerificationObserver interface {
	ObserveTransaction(signatures, messageBytes int)
	BeginVerification(publicKey solana.PublicKey, signature solana.Signature, message []byte) VerificationObservation
}

func MessageBytes(tx *solana.Transaction) ([]byte, error) {
	if tx == nil {
		return nil, fmt.Errorf("nil transaction")
	}
	msg, err := tx.Message.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if tx.Message.IsVersioned() {
		if len(msg) == 0 {
			return nil, fmt.Errorf("empty versioned message")
		}
		version := byte(tx.Message.GetVersion())
		if version == 0 {
			msg[0] = 0x80
		} else {
			msg[0] = 0x7f + version
		}
	}
	return msg, nil
}

func VerifyTransaction(tx *solana.Transaction) error {
	return VerifyTransactionWithObserver(tx, nil)
}

// VerifyTransactionWithObserver verifies the same predicate as
// VerifyTransaction while reporting the transaction shape and exact attempted
// inputs to observer. A nil observer has no telemetry side effects.
func VerifyTransactionWithObserver(tx *solana.Transaction, observer VerificationObserver) error {
	msg, err := MessageBytes(tx)
	if err != nil {
		return err
	}

	signers := tx.Message.Signers()
	if len(signers) != len(tx.Signatures) {
		return fmt.Errorf("got %d signers, but %d signatures", len(signers), len(tx.Signatures))
	}
	if observer != nil {
		observer.ObserveTransaction(len(tx.Signatures), len(msg))
	}

	for i, sig := range tx.Signatures {
		var observation VerificationObservation
		if observer != nil {
			observation = observer.BeginVerification(signers[i], sig, msg)
		}
		valid := sig.Verify(signers[i], msg)
		if observation != nil {
			observation.RecordResult(valid)
		}
		if !valid {
			return fmt.Errorf("invalid signature by %s", signers[i])
		}
	}
	return nil
}
