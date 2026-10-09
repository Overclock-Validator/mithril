package sigverify

import (
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// TxV1Enabled reports whether the bank at TPU admission has activated
// SIMD-0385. A nil callback fails closed for v1 and still permits legacy/v0.
type TxV1Enabled func() bool

func ParseTx(p []byte) (tx *solana.Transaction, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("ParseTx panic")
		}
	}()

	if _, err = wire.Sanitize(p); err != nil {
		return nil, err
	}

	// TPU packets are whole transactions, not entries in a larger stream.
	// TransactionFromBytes is strict for v1; wire.Sanitize above additionally
	// enforces exact EOF for legacy and v0.
	tx, err = solana.TransactionFromBytes(p)
	if err != nil {
		return nil, err
	}
	return
}

// VerifyPacket parses a wire transaction and verifies its signatures against
// static account keys. Unparseable packets and invalid signatures are discarded.
// Address lookup tables are not resolved.
//
// Prefer BatchVerifier: a transaction carries one or two signatures, so
// verifying packets one at a time leaves most of a vector group idle.
func VerifyPacket(data []byte) bool {
	return VerifyPacketWithTxV1(data, nil)
}

// VerifyPacketWithTxV1 verifies one TPU packet under the current bank's v1
// feature policy.
func VerifyPacketWithTxV1(data []byte, txV1Enabled TxV1Enabled) bool {
	allowV1 := false
	if len(data) > 0 && data[0] == 0x81 {
		allowV1 = txV1Enabled != nil && txV1Enabled()
		if !allowV1 {
			return false
		}
	}
	tx, err := ParseTx(data)
	if err != nil {
		return false
	}
	return (tx.Message.GetVersion() != solana.MessageVersionV1 || allowV1) && verifyTransaction(tx)
}

func VerifyTransaction(tx *solana.Transaction) bool {
	return VerifyTransactionWithTxV1(tx, nil)
}

// VerifyTransactionWithTxV1 structurally validates and verifies an in-memory
// transaction under the current bank's v1 feature policy.
func VerifyTransactionWithTxV1(tx *solana.Transaction, txV1Enabled TxV1Enabled) bool {
	if !admissible(tx) {
		return false
	}
	if !transactionVersionAllowed(tx, txV1Enabled) {
		return false
	}
	return verifyTransaction(tx)
}

func verifyTransaction(tx *solana.Transaction) bool {
	// Signature checking goes through txverify rather than being reimplemented
	// here. This path used to marshal the message itself and so omitted the
	// version-byte fixup, which meant a correctly signed versioned transaction
	// was dropped at ingest.
	return txverify.VerifyTransaction(tx) == nil
}

func transactionVersionAllowed(tx *solana.Transaction, txV1Enabled TxV1Enabled) bool {
	if tx == nil || tx.Message.GetVersion() != solana.MessageVersionV1 {
		return true
	}
	return txV1Enabled != nil && txV1Enabled()
}

// admissible rejects shapes TPU must not admit regardless of cryptography: a
// transaction that requires no signatures at all, a signature list that
// disagrees with the header, or a header claiming more signers than the account
// table can supply.
func admissible(tx *solana.Transaction) bool {
	if tx == nil {
		return false
	}
	required := int(tx.Message.Header.NumRequiredSignatures)
	return required > 0 &&
		len(tx.Signatures) == required &&
		required <= len(tx.Message.AccountKeys)
}

// BatchVerifier parses and verifies many packets per call. It is reusable
// caller-owned scratch and is not safe for concurrent use; give each worker
// its own.
type BatchVerifier struct {
	inner       txverify.BatchVerifier
	txs         []*solana.Transaction
	errs        []error
	txV1Enabled TxV1Enabled
}

// SetTxV1Enabled installs the current-bank feature callback. A nil callback
// makes v1 admission fail closed.
func (v *BatchVerifier) SetTxV1Enabled(enabled TxV1Enabled) {
	v.txV1Enabled = enabled
}

// Verify writes a verdict for each packet into ok, which must be at least as
// long as packets. A packet that fails to parse, or whose shape is
// inadmissible, is reported false without consuming a signature lane.
func (v *BatchVerifier) Verify(packets [][]byte, ok []bool) {
	clear(v.txs)
	v.txs = v.txs[:0]
	allowV1 := v.txV1Enabled != nil && v.txV1Enabled()
	for _, data := range packets {
		if len(data) > 0 && data[0] == 0x81 && !allowV1 {
			v.txs = append(v.txs, nil)
			continue
		}
		tx, err := ParseTx(data)
		if err != nil || !admissible(tx) || (tx.Message.GetVersion() == solana.MessageVersionV1 && !allowV1) {
			// A nil entry keeps the packet's position so verdicts line up, and
			// the batch verifier reports it failed without adding lanes.
			v.txs = append(v.txs, nil)
			continue
		}
		v.txs = append(v.txs, tx)
	}

	if cap(v.errs) < len(v.txs) {
		v.errs = make([]error, len(v.txs))
	}
	v.errs = v.errs[:len(v.txs)]
	v.inner.Verify(v.txs, v.errs)

	for i := range v.txs {
		ok[i] = v.txs[i] != nil && v.errs[i] == nil
	}
}
