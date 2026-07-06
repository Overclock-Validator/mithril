package replay

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/zeebo/blake3"
)

// The slot digest is the trailing verifier's compact record of what replay
// produced for one slot: per transaction, a 128-bit hash over exactly the
// fields the RPC-mode divergence checks compare (fee, success/failure status,
// pre-balances always, post-balances only on success), plus the mask of
// account indices mithril considers non-comparable (native programs and
// internal dummy placeholders). The verifier recomputes the same hash from
// RPC transaction metadata using the recorded mask — a mismatch is an
// execution divergence localized to the exact transaction.
//
// Compute units are deliberately excluded: CU divergence is a known benign,
// non-failing difference and would false-halt the fold pipeline.

// txExecRecord captures one transaction's externally-comparable results for
// the trailing verifier. Pre is recorded for every transaction; Post only for
// successful ones (mirroring the RPC-mode divergence checks). SkipMask bit i
// set = account index i is not comparable (native program or dummy
// placeholder).
//
// The registry lives entirely in pkg/replay — the VM and sealevel execution
// context are deliberately untouched; capture is a read-only observation at
// the ProcessTransaction boundary, active only while the verifier runs.
type txExecRecord struct {
	Fee      uint64
	Failed   bool
	Pre      []uint64
	Post     []uint64
	SkipMask []byte
}

// captureRegistry holds ONE replay run's per-transaction execution captures. It
// is instance state — published in activeCapture only for the duration of a
// verifier-enabled run and unpublished on exit — so separate runs (sequential
// recovery re-replays, tests, simulations) never share or consume each other's
// records. A nil activeCapture means capture is off; the record path then
// no-ops, replacing the old sticky package-global enable flag.
type captureRegistry struct {
	mu    sync.Mutex
	slots map[uint64]map[solana.Signature]*txExecRecord
}

var activeCapture atomic.Pointer[captureRegistry]

// beginTxCapture publishes a fresh run-local capture registry and returns a
// stop function that unpublishes exactly this one (defer it for a
// verifier-enabled run).
func beginTxCapture() (stop func()) {
	reg := &captureRegistry{slots: make(map[uint64]map[solana.Signature]*txExecRecord)}
	activeCapture.Store(reg)
	return func() { activeCapture.CompareAndSwap(reg, nil) }
}

// txCaptureActive reports whether a run is capturing — cheap enough to gate the
// hot-path capture points so balances are only walked when the verifier runs.
func txCaptureActive() bool { return activeCapture.Load() != nil }

// recordTxExecCapture stores a transaction's execution record in the active
// run's registry (nil-safe; no-ops when no run is capturing). Old slots are
// janitored so an unwound or abandoned slot cannot leak its capture set.
func recordTxExecCapture(slot uint64, sig solana.Signature, rec *txExecRecord) {
	if rec == nil {
		return
	}
	reg := activeCapture.Load()
	if reg == nil {
		return
	}
	reg.mu.Lock()
	set := reg.slots[slot]
	if set == nil {
		set = make(map[solana.Signature]*txExecRecord)
		reg.slots[slot] = set
		for s := range reg.slots {
			if s+256 < slot {
				delete(reg.slots, s)
			}
		}
	}
	set[sig] = rec
	reg.mu.Unlock()
}

// takeTxCaptures removes and returns a slot's capture set from the active run.
func takeTxCaptures(slot uint64) map[solana.Signature]*txExecRecord {
	reg := activeCapture.Load()
	if reg == nil {
		return nil
	}
	reg.mu.Lock()
	set := reg.slots[slot]
	delete(reg.slots, slot)
	reg.mu.Unlock()
	return set
}

// TxDigest is one transaction's comparable execution result (~26-40 bytes).
type TxDigest struct {
	SigPrefix  [8]byte
	RecordHash [16]byte
	NumAccts   uint16
	SkipMask   []byte // ceil(NumAccts/8) bytes; bit set = index not comparable
}

// SlotDigest is one replayed (or skipped) slot's verification record.
type SlotDigest struct {
	Slot      uint64
	Blockhash solana.Hash // the block's PoH blockhash — sibling disambiguation vs RPC
	Skipped   bool
	Txs       []TxDigest
}

// txRecordHash hashes the comparable fields of one transaction. Layout:
// sig(64) ‖ fee u64le ‖ status byte (0 ok / 1 failed) ‖ numAccts u16le ‖
// skipMask ‖ pre[i] u64le for unmasked i ‖ (post[i] u64le for unmasked i, only
// when status == 0). The mask is inside the hash so the verifier must use the
// identical mask, and masked balances contribute nothing.
func txRecordHash(sig solana.Signature, fee uint64, failed bool, numAccts uint16, skipMask []byte, pre, post []uint64) [16]byte {
	h := blake3.New()
	var u64 [8]byte
	_, _ = h.Write(sig[:])
	binary.LittleEndian.PutUint64(u64[:], fee)
	_, _ = h.Write(u64[:])
	status := byte(0)
	if failed {
		status = 1
	}
	_, _ = h.Write([]byte{status})
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], numAccts)
	_, _ = h.Write(u16[:])
	_, _ = h.Write(skipMask)
	writeUnmasked := func(vals []uint64) {
		for i := 0; i < int(numAccts) && i < len(vals); i++ {
			if maskBit(skipMask, i) {
				continue
			}
			binary.LittleEndian.PutUint64(u64[:], vals[i])
			_, _ = h.Write(u64[:])
		}
	}
	writeUnmasked(pre)
	if !failed {
		writeUnmasked(post)
	}
	var out [16]byte
	sum := h.Sum(nil)
	copy(out[:], sum[:16])
	return out
}

func maskBit(mask []byte, i int) bool {
	if i/8 >= len(mask) {
		return false
	}
	return mask[i/8]&(1<<(i%8)) != 0
}

func setMaskBit(mask []byte, i int) {
	if i/8 < len(mask) {
		mask[i/8] |= 1 << (i % 8)
	}
}

// buildSlotDigest assembles the slot's digest from the per-transaction records
// captured during execution, in block transaction order. Transactions with no
// record (should not happen) hash as "mithril produced nothing" — a guaranteed
// verifier mismatch, which is the correct fail-closed outcome.
func buildSlotDigest(block *b.Block) *SlotDigest {
	records := takeTxCaptures(block.Slot)
	d := &SlotDigest{
		Slot:      block.Slot,
		Blockhash: solana.Hash(block.Blockhash),
		Txs:       make([]TxDigest, 0, len(block.Transactions)),
	}
	for _, tx := range block.Transactions {
		if len(tx.Signatures) == 0 {
			d.Txs = append(d.Txs, TxDigest{})
			continue
		}
		sig := tx.Signatures[0]
		rec := records[sig]
		td := TxDigest{}
		copy(td.SigPrefix[:], sig[:8])
		if rec != nil {
			td.NumAccts = uint16(len(rec.Pre))
			td.SkipMask = rec.SkipMask
			td.RecordHash = txRecordHash(sig, rec.Fee, rec.Failed, td.NumAccts, rec.SkipMask, rec.Pre, rec.Post)
		}
		d.Txs = append(d.Txs, td)
	}
	return d
}
