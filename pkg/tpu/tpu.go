package tpu

import (
	"crypto/ed25519"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
	"github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

func ParseTx(p []byte) (tx *solana.Transaction, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("ParseTx panic")
		}
	}()

	tx, err = solana.TransactionFromDecoder(bin.NewBinDecoder(p))
	if err != nil {
		return nil, err
	}
	return
}

// VerifyPacket parses and signature-verifies one TPU wire packet.
func VerifyPacket(data []byte) bool {
	tx, err := ParseTx(data)
	return err == nil && VerifyTxSig(tx)
}

func VerifyTxSig(tx *solana.Transaction) (ok bool) {
	msg, err := tx.Message.MarshalBinary()
	if err != nil {
		return false
	}

	signers := ExtractSigners(tx)

	if len(signers) != len(tx.Signatures) {
		return false
	}

	telemetryEnabled := sigverifytelemetry.Enabled()
	if telemetryEnabled {
		sigverifytelemetry.RecordTransaction(sigverifytelemetry.SourceTPU, len(tx.Signatures), len(msg))
	}
	for i, sig := range tx.Signatures {
		var observation sigverifytelemetry.VerificationAttempt
		if telemetryEnabled {
			observation, _, _, _ = sigverifytelemetry.BeginVerification(
				sigverifytelemetry.SourceTPU,
				[32]byte(signers[i]),
				[64]byte(sig),
				msg,
			)
		}
		valid := ed25519.Verify(signers[i][:], msg, sig[:])
		if telemetryEnabled {
			observation.RecordResult(valid)
		}
		if !valid {
			return false
		}
	}

	return true
}

func ExtractSigners(tx *solana.Transaction) []solana.PublicKey {
	signers := make([]solana.PublicKey, 0, len(tx.Signatures))
	for _, acc := range tx.Message.AccountKeys {
		if tx.IsSigner(acc) {
			signers = append(signers, acc)
		}
	}
	return signers
}
