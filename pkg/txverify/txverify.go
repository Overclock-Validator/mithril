package txverify

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/gagliardetto/solana-go"
)

const (
	MaxLegacyTransactionSize    = 1232
	MaxSignaturesPerTransaction = 12
	MaxInstructionsPerMessage   = 64
	MaxAccountsPerInstruction   = 255
)

// MessageBytes returns the exact bytes a transaction's signatures are computed
// over.
//
// Version prefixes are part of the signed message. Keep this helper as the one
// signature-verification boundary so legacy, v0, and SIMD-0385 v1 transactions
// all use the codec's canonical message encoding.
func MessageBytes(tx *solana.Transaction) ([]byte, error) {
	if tx == nil {
		return nil, fmt.Errorf("nil transaction")
	}
	return tx.Message.MarshalBinary()
}

// SanitizeTransaction applies solana-go's Agave-compatible structural
// sanitizer. Mithril resolves v0 address-table keys in place before execution;
// solana-go's sanitizer expects the original static-key slice, so present a
// shallow static-key view for that one state. V1 has no lookup tables.
func SanitizeTransaction(tx *solana.Transaction) error {
	if tx == nil {
		return fmt.Errorf("nil transaction")
	}
	// Agave's transaction-view sanitizer applies these limits to every message
	// version. solana-go enforces them for v1, whose fixed-width encoding needs
	// them to decode, but legacy and v0 use compact lengths and therefore need
	// the same consensus limits enforced explicitly here.
	if len(tx.Signatures) > MaxSignaturesPerTransaction {
		return fmt.Errorf("too many signatures: %d > max %d", len(tx.Signatures), MaxSignaturesPerTransaction)
	}
	if len(tx.Message.Instructions) > MaxInstructionsPerMessage {
		return fmt.Errorf("too many instructions: %d > max %d", len(tx.Message.Instructions), MaxInstructionsPerMessage)
	}
	for i := range tx.Message.Instructions {
		if len(tx.Message.Instructions[i].Accounts) > MaxAccountsPerInstruction {
			return fmt.Errorf("instruction %d: too many accounts (%d), max %d", i, len(tx.Message.Instructions[i].Accounts), MaxAccountsPerInstruction)
		}
	}
	var err error
	if tx.Message.GetVersion() != solana.MessageVersionV0 || !tx.Message.IsResolved() {
		err = tx.Sanitize()
	} else {
		dynamicKeys := tx.Message.AddressTableLookups.NumLookups()
		if dynamicKeys > len(tx.Message.AccountKeys) {
			return fmt.Errorf("resolved v0 transaction has %d lookup keys but only %d total keys", dynamicKeys, len(tx.Message.AccountKeys))
		}
		staticKeys := len(tx.Message.AccountKeys) - dynamicKeys
		view := *tx
		view.Message = tx.Message
		view.Message.AccountKeys = tx.Message.AccountKeys[:staticKeys]
		err = view.Sanitize()
	}
	if err != nil {
		return err
	}

	wireSize, err := TransactionWireSize(tx)
	if err != nil {
		return err
	}
	limit := MaxLegacyTransactionSize
	if tx.Message.GetVersion() == solana.MessageVersionV1 {
		limit = solana.MaxTransactionSizeV1
	}
	if wireSize > limit {
		return fmt.Errorf("transaction wire size %d exceeds version %d limit %d", wireSize, tx.Message.GetVersion(), limit)
	}
	return nil
}

// TransactionWireSize returns the canonical encoded transaction size without
// allocating or serializing. Replay uses it as a final consensus boundary for
// blocks arriving from sources that do not retain their original wire bytes.
func TransactionWireSize(tx *solana.Transaction) (int, error) {
	if tx == nil {
		return 0, fmt.Errorf("nil transaction")
	}

	msg := &tx.Message
	if msg.GetVersion() == solana.MessageVersionV1 {
		size := 1 + 3 + 4 + 32 + 1 + 1 + 32*len(msg.AccountKeys) + msg.TransactionConfig.Size()
		size += 4 * len(msg.Instructions)
		for i := range msg.Instructions {
			size += len(msg.Instructions[i].Accounts) + len(msg.Instructions[i].Data)
		}
		return size + 64*len(tx.Signatures), nil
	}

	if msg.GetVersion() != solana.MessageVersionLegacy && msg.GetVersion() != solana.MessageVersionV0 {
		return 0, fmt.Errorf("unsupported message version %d", msg.GetVersion())
	}

	signatureLen, err := compactU16Size(len(tx.Signatures))
	if err != nil {
		return 0, fmt.Errorf("signature count: %w", err)
	}
	numStaticKeys := len(msg.AccountKeys)
	if msg.GetVersion() == solana.MessageVersionV0 && msg.IsResolved() {
		numStaticKeys -= msg.AddressTableLookups.NumLookups()
		if numStaticKeys < 0 {
			return 0, fmt.Errorf("resolved v0 lookup count exceeds account key count")
		}
	}
	accountKeyLen, err := compactU16Size(numStaticKeys)
	if err != nil {
		return 0, fmt.Errorf("account key count: %w", err)
	}
	instructionLen, err := compactU16Size(len(msg.Instructions))
	if err != nil {
		return 0, fmt.Errorf("instruction count: %w", err)
	}

	size := signatureLen + 64*len(tx.Signatures) + 3 + accountKeyLen + 32*numStaticKeys + 32 + instructionLen
	if msg.GetVersion() == solana.MessageVersionV0 {
		size++ // 0x80 message-version prefix
	}
	for i := range msg.Instructions {
		accountsLen, err := compactU16Size(len(msg.Instructions[i].Accounts))
		if err != nil {
			return 0, fmt.Errorf("instruction %d account count: %w", i, err)
		}
		dataLen, err := compactU16Size(len(msg.Instructions[i].Data))
		if err != nil {
			return 0, fmt.Errorf("instruction %d data length: %w", i, err)
		}
		size += 1 + accountsLen + len(msg.Instructions[i].Accounts) + dataLen + len(msg.Instructions[i].Data)
	}
	if msg.GetVersion() == solana.MessageVersionV0 {
		lookupLen, err := compactU16Size(len(msg.AddressTableLookups))
		if err != nil {
			return 0, fmt.Errorf("address table lookup count: %w", err)
		}
		size += lookupLen
		for i := range msg.AddressTableLookups {
			lookup := &msg.AddressTableLookups[i]
			writableLen, err := compactU16Size(len(lookup.WritableIndexes))
			if err != nil {
				return 0, fmt.Errorf("address table lookup %d writable count: %w", i, err)
			}
			readonlyLen, err := compactU16Size(len(lookup.ReadonlyIndexes))
			if err != nil {
				return 0, fmt.Errorf("address table lookup %d readonly count: %w", i, err)
			}
			size += 32 + writableLen + len(lookup.WritableIndexes) + readonlyLen + len(lookup.ReadonlyIndexes)
		}
	}
	return size, nil
}

func compactU16Size(value int) (int, error) {
	switch {
	case value < 0 || value > 0xffff:
		return 0, fmt.Errorf("value %d does not fit compact-u16", value)
	case value <= 0x7f:
		return 1, nil
	case value <= 0x3fff:
		return 2, nil
	default:
		return 3, nil
	}
}

// VerifyTransaction verifies one transaction's signatures.
//
// Prefer BatchVerifier where more than a few transactions are available: a
// transaction carries one or two signatures, and verifying one or two at a time
// leaves most of a vector group idle.
func VerifyTransaction(tx *solana.Transaction) error {
	if err := SanitizeTransaction(tx); err != nil {
		return err
	}
	msg, err := MessageBytes(tx)
	if err != nil {
		return err
	}

	signers := tx.Message.Signers()
	if len(signers) != len(tx.Signatures) {
		return fmt.Errorf("got %d signers, but %d signatures", len(signers), len(tx.Signatures))
	}

	for i := range tx.Signatures {
		if !sigverify.VerifyOne((*[32]byte)(&signers[i]), msg, tx.Signatures[i][:]) {
			return fmt.Errorf("invalid signature by %s", signers[i])
		}
	}
	return nil
}

// BatchVerifier verifies many transactions per call. It is reusable
// caller-owned scratch and is not safe for concurrent use; give each worker
// its own.
type BatchVerifier struct {
	batch sigverify.Batch
	// counts[i] is how many signature lanes transaction i contributed, which
	// is zero when it failed a precheck. It is what maps a lane verdict back
	// to the transaction that produced it.
	counts []int
	// signers[i] is retained so a failure can name the signer without
	// recomputing Signers(), which allocates.
	signers [][]solana.PublicKey
}

// Verify checks every transaction in txs and writes a per-transaction result
// into errs, which must be the same length as txs. A nil entry means that
// transaction verified.
//
// Every transaction gets an independent verdict: one bad transaction does not
// mask the others, so a caller can report precisely which one failed.
func (v *BatchVerifier) Verify(txs []*solana.Transaction, errs []error) {
	if len(errs) != len(txs) {
		panic("txverify: errs and txs length mismatch")
	}
	v.batch.Reset()
	v.counts = v.counts[:0]
	clear(v.signers)
	v.signers = v.signers[:0]

	for i, tx := range txs {
		errs[i] = nil
		signers, msg, err := prepare(tx)
		if err != nil {
			errs[i] = err
			v.counts = append(v.counts, 0)
			v.signers = append(v.signers, nil)
			continue
		}
		for j := range tx.Signatures {
			v.batch.Add((*[32]byte)(&signers[j]), msg, tx.Signatures[j][:])
		}
		v.counts = append(v.counts, len(tx.Signatures))
		v.signers = append(v.signers, signers)
	}

	if v.batch.Verify() {
		return
	}

	lane := 0
	for i, count := range v.counts {
		for j := 0; j < count; j++ {
			if !v.batch.OK(lane+j) && errs[i] == nil {
				errs[i] = fmt.Errorf("invalid signature by %s", v.signers[i][j])
			}
		}
		lane += count
	}
}

// prepare runs the checks that must precede verification and that determine a
// transaction's result on their own.
func prepare(tx *solana.Transaction) ([]solana.PublicKey, []byte, error) {
	if err := SanitizeTransaction(tx); err != nil {
		return nil, nil, err
	}
	msg, err := MessageBytes(tx)
	if err != nil {
		return nil, nil, err
	}
	signers := tx.Message.Signers()
	if len(signers) != len(tx.Signatures) {
		return nil, nil, fmt.Errorf("got %d signers, but %d signatures", len(signers), len(tx.Signatures))
	}
	return signers, msg, nil
}
