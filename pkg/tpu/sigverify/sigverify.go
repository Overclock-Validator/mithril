package sigverify

import (
	"crypto/ed25519"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
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

// VerifyPacket parses a wire transaction and verifies its signatures against
// static account keys. Unparseable packets and invalid signatures are discarded.
// Address lookup tables are not resolved.
func VerifyPacket(data []byte) bool {
	tx, err := ParseTx(data)
	if err != nil {
		return false
	}
	return VerifyTransaction(tx)
}

func VerifyTransaction(tx *solana.Transaction) bool {
	return VerifyTransactionInDispatch(tx, sigverifytelemetry.DispatchJob{})
}

// VerifyTransactionInDispatch verifies the same predicate while correlating
// each attempted signature with one replay/TPU worker claim. The zero dispatch
// value preserves the direct-call behavior.
func VerifyTransactionInDispatch(tx *solana.Transaction, dispatch sigverifytelemetry.DispatchJob) bool {
	required := int(tx.Message.Header.NumRequiredSignatures)
	if required == 0 || len(tx.Signatures) != required || required > len(tx.Message.AccountKeys) {
		return false
	}

	// Keep TPU, replay, and turbine on one definition of the bytes covered by
	// a transaction signature. In particular, versioned messages include the
	// high-bit version prefix.
	msg, err := txverify.MessageBytes(tx)
	if err != nil {
		return false
	}

	keys := tx.Message.AccountKeys
	telemetryEnabled := sigverifytelemetry.Enabled()
	if telemetryEnabled {
		sigverifytelemetry.RecordTransaction(sigverifytelemetry.SourceTPU, required, len(msg))
	}
	for i := 0; i < required; i++ {
		var observation sigverifytelemetry.VerificationAttempt
		if telemetryEnabled {
			observation, _, _, _ = sigverifytelemetry.BeginVerificationInDispatch(
				sigverifytelemetry.SourceTPU,
				dispatch,
				i,
				[32]byte(keys[i]),
				[64]byte(tx.Signatures[i]),
				msg,
			)
		}
		valid := ed25519.Verify(keys[i][:], msg, tx.Signatures[i][:])
		if telemetryEnabled {
			observation.RecordResult(valid)
		}
		if !valid {
			return false
		}
	}
	return true
}
