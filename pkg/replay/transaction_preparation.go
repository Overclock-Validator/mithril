package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// TransactionPreparer binds static transaction preparation to an immutable bank
// feature snapshot. Its source must remain immutable for the bank's lifetime.
// Prepared messages must also remain immutable, including their backing bytes.
// Account contents, transaction age and duplicate status are never cached here.
type TransactionPreparer struct {
	source *features.Features
	feats  *features.Features
	key    [32]byte
}

// PreparedTransaction contains only message/feature-derived data. Private fields
// prevent callers from substituting an unchecked hash, cost or instruction list.
type PreparedTransaction struct {
	tx               *solana.Transaction
	key              [32]byte
	hash             [32]byte
	cost             costmodel.TransactionCost
	instrs           []sealevel.Instruction
	instructionAccts [][]sealevel.InstructionAccount
	accountMetas     []*solana.AccountMeta
	limits           *sealevel.ComputeBudgetLimits
}

func NewTransactionPreparer(f *features.Features) *TransactionPreparer {
	if f == nil {
		return nil
	}
	clone := f.Clone()
	gates := make([]features.FeatureGate, 0, len(*clone))
	for gate := range *clone {
		gates = append(gates, gate)
	}
	sort.Slice(gates, func(i, j int) bool {
		if gates[i].Address != gates[j].Address {
			return string(gates[i].Address[:]) < string(gates[j].Address[:])
		}
		return gates[i].Name < gates[j].Name
	})
	h := sha256.New()
	var value [8]byte
	for _, gate := range gates {
		info := (*clone)[gate]
		h.Write(gate.Address[:])
		binary.LittleEndian.PutUint64(value[:], uint64(len(gate.Name)))
		h.Write(value[:])
		h.Write([]byte(gate.Name))
		if info.Enabled {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		binary.LittleEndian.PutUint64(value[:], info.ActivationSlot)
		h.Write(value[:])
	}
	p := &TransactionPreparer{source: f, feats: clone}
	copy(p.key[:], h.Sum(nil))
	return p
}

// Prepare returns nil on a static validation failure. Callers retain their normal
// processing path in that case, preserving its error classification and ordering.
func (p *TransactionPreparer) Prepare(tx *solana.Transaction) *PreparedTransaction {
	if p == nil || tx == nil {
		return nil
	}
	if tx.Message.GetVersion() == solana.MessageVersionV1 && !p.feats.IsActive(features.EnableTxV1) {
		return nil
	}
	if txverify.SanitizeTransaction(tx) != nil {
		return nil
	}
	if p.feats.IsActive(features.StaticInstructionLimit) && len(tx.Message.Instructions) > maxInstrTraceCapacity {
		return nil
	}
	instrs, instructionAccts, metas, err := instrsAndAcctMetasFromTx(tx, p.feats)
	if err != nil {
		return nil
	}
	limits, err := sealevel.ComputeBudgetLimitsForTransaction(tx, instrs, p.feats)
	if err != nil {
		return nil
	}
	hash, err := TransactionMessageHash(tx)
	if err != nil {
		return nil
	}
	return &PreparedTransaction{tx: tx, key: p.key, hash: hash,
		cost:   costmodel.EstimatePreparedTransactionCost(tx, instrs, limits, p.feats),
		instrs: instrs, instructionAccts: instructionAccts, accountMetas: metas, limits: limits}
}

func (p *TransactionPreparer) Matches(prepared *PreparedTransaction, tx *solana.Transaction, f *features.Features) bool {
	return p != nil && p.source == f && prepared != nil && prepared.tx == tx && p.key == prepared.key
}

func (p *PreparedTransaction) MessageHash() [32]byte { return p.hash }

// Cost returns the estimate; its writable-account slice is read-only.
func (p *PreparedTransaction) Cost() costmodel.TransactionCost { return p.cost }

// LoadAndExecute reuses preparation only for the same immutable message and a
// matching feature snapshot. All bank-dependent checks still run on every call.
func (p *TransactionPreparer) LoadAndExecute(input LoadAndExecuteTransactionInput, prepared *PreparedTransaction) LoadAndExecuteTransactionOutput {
	if input.SlotCtx == nil || p == nil || p.source != input.SlotCtx.Features || !p.Matches(prepared, input.Transaction, input.SlotCtx.Features) {
		return LoadAndExecuteTransaction(input)
	}
	return loadAndExecuteTransaction(input, prepared)
}

// PayerCanFund keeps strict leader admission, using current payer state while
// sharing the already-validated instructions and compute limits.
func (p *TransactionPreparer) PayerCanFund(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, prepared *PreparedTransaction) error {
	if slotCtx == nil || !p.Matches(prepared, tx, slotCtx.Features) {
		return fees.PayerCanFund(slotCtx, tx)
	}
	_, err := fees.ValidateTransactionFeePayer(slotCtx, tx, prepared.instrs, prepared.limits)
	return err
}
