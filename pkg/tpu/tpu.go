package tpu

import (
	"github.com/Overclock-Validator/mithril/pkg/tpu/sigverify"
	"github.com/gagliardetto/solana-go"
)

func ParseTx(p []byte) (*solana.Transaction, error) {
	return sigverify.ParseTx(p)
}

// VerifyPacket parses and signature-verifies one TPU wire packet.
func VerifyPacket(data []byte) bool {
	return sigverify.VerifyPacket(data)
}

// VerifyTxSig reports whether every required signature on tx is valid.
//
// It delegates rather than reimplementing. The hand-rolled version this
// replaces marshalled the message itself and so omitted the version-byte
// fixup that txverify.MessageBytes applies, which meant a correctly signed
// versioned transaction never verified here.
func VerifyTxSig(tx *solana.Transaction) (ok bool) {
	return sigverify.VerifyTransaction(tx)
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
