package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// The trailing verifier is the execution-correctness oracle of the fold
// pipeline. Alpenglow certificates attest a block's DATA (block_id), never
// the results of executing it — so after TowerBFT's voted-bankhash check was
// removed, nothing in consensus can catch a mithril-side execution bug. The
// verifier re-derives each executed slot's per-transaction results from RPC
// getBlock metadata (finalized commitment) a configurable lag behind the tip
// and compares against the slot digests replay recorded. The fold watermark
// is min(finality, verified): nothing reaches durable state unverified.
//
// Failure semantics are deliberately asymmetric:
//   - a same-block digest mismatch is an execution divergence -> halt +
//     persist evidence; deterministic divergence is NOT self-healing, so the
//     node-level recovery loop must not auto-retry it
//   - a different blockhash at the same slot is a SIBLING question (fork
//     identity), owned by the certificate gate/switch — requeue, never halt
//   - RPC outages just stall the watermark; with Required=true the fold
//     stalls with it until the OverCap halt (designed fail-closed backpressure)

// VerifierConfig configures the trailing verifier ([verifier] section).
type VerifierConfig struct {
	Enabled  bool
	LagSlots uint64 // don't attempt verification until executedTip - LagSlots
	MaxRPS   int    // verifier's own RPC budget (never shares block fetch)
	Required bool   // gate folds on the verified watermark
}

// TrailingVerifierDefaults returns the default configuration: enabled,
// required, 32-slot lag, 8 requests/second.
func TrailingVerifierDefaults() VerifierConfig {
	return VerifierConfig{Enabled: true, LagSlots: 32, MaxRPS: 8, Required: true}
}

// TrailingVerifierCfg is the active configuration ([verifier] section), set by
// node startup before ReplayBlocks.
var TrailingVerifierCfg = TrailingVerifierDefaults()

// recordReplayDivergenceEvidence persists a confirmed execution divergence to
// the state file (written on shutdown), so restarts refuse to fold at or past
// the disputed slot until the operator clears the evidence after triage.
func recordReplayDivergenceEvidence(st *state.MithrilState, d *ReplayDivergence) {
	if st == nil || d == nil {
		return
	}
	for _, ev := range st.ReplayDivergenceEvidence {
		if ev.Slot == d.Slot && ev.TxIndex == d.TxIndex && ev.Kind == d.Kind {
			return
		}
	}
	st.ReplayDivergenceEvidence = append(st.ReplayDivergenceEvidence, state.ReplayDivergenceRecord{
		Slot:        d.Slot,
		TxIndex:     d.TxIndex,
		TxSignature: d.TxSignature,
		Kind:        d.Kind,
		Detail:      d.Detail,
		RecordedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	mlog.Log.Errorf("replay divergence evidence recorded for slot %d (%s) — folds blocked at that slot until cleared", d.Slot, d.Kind)
}

// ReplayDivergence identifies the first verified execution mismatch.
type ReplayDivergence struct {
	Slot        uint64
	TxIndex     int
	TxSignature string
	Kind        string // "tx_record", "tx_count", "skip_mismatch", "missing_record"
	Detail      string
}

func (d *ReplayDivergence) Error() string {
	return fmt.Sprintf("replay divergence at slot %d tx %d (%s): %s — %s", d.Slot, d.TxIndex, d.TxSignature, d.Kind, d.Detail)
}

// verifierTx is one transaction's externally-attested record extracted from
// RPC metadata.
type verifierTx struct {
	Sig    solana.Signature
	Fee    uint64
	Failed bool
	Pre    []uint64
	Post   []uint64
}

// verifiedBlock is the extracted finalized view of one slot.
type verifiedBlock struct {
	Blockhash solana.Hash
	Txs       []verifierTx
}

// blockVerificationSource fetches the finalized attested view of a slot.
// Returns rpcclient.SlotSkipped for finalized-skipped slots; any other error
// is transient (backoff + retry). Faked in tests.
type blockVerificationSource interface {
	FetchFinalized(slot uint64) (*verifiedBlock, error)
}

// rpcVerificationSource adapts an RpcClient to blockVerificationSource.
type rpcVerificationSource struct {
	rpcc *rpcclient.RpcClient
}

func (r *rpcVerificationSource) FetchFinalized(slot uint64) (*verifiedBlock, error) {
	result, err := r.rpcc.GetBlockFinalizedOnce(slot)
	if err != nil {
		return nil, err
	}
	vb := &verifiedBlock{Blockhash: result.Blockhash, Txs: make([]verifierTx, 0, len(result.Transactions))}
	for i := range result.Transactions {
		rtx := &result.Transactions[i]
		parsed, perr := rtx.GetTransaction()
		if perr != nil || parsed == nil || len(parsed.Signatures) == 0 || rtx.Meta == nil {
			return nil, fmt.Errorf("slot %d tx %d: unparseable RPC transaction/meta", slot, i)
		}
		vb.Txs = append(vb.Txs, verifierTx{
			Sig:    parsed.Signatures[0],
			Fee:    rtx.Meta.Fee,
			Failed: rtx.Meta.Err != nil,
			Pre:    rtx.Meta.PreBalances,
			Post:   rtx.Meta.PostBalances,
		})
	}
	return vb, nil
}

type pendingDigest struct {
	digest      *SlotDigest
	attempts    int
	skipHits    int // cross-checks confirming an RPC skip disagreement
	siblingHits int // consecutive different-blockhash observations
	nextTry     time.Time
}

// TrailingVerifier verifies executed slots against RPC metadata in the
// background and exposes the contiguous verified watermark.
type TrailingVerifier struct {
	cfg VerifierConfig
	src blockVerificationSource

	mu          sync.Mutex
	pending     map[uint64]*pendingDigest
	order       []uint64 // ascending recorded slots not yet verified
	firstSlot   uint64   // first slot ever recorded (watermark floor anchor)
	verified    uint64   // all recorded slots <= verified are verified
	executedTip uint64
	failure     *ReplayDivergence

	verifiedCount uint64
	requeues      uint64
}

func newTrailingVerifier(src blockVerificationSource, cfg VerifierConfig) *TrailingVerifier {
	if cfg.LagSlots == 0 {
		cfg.LagSlots = 32
	}
	if cfg.MaxRPS <= 0 {
		cfg.MaxRPS = 8
	}
	return &TrailingVerifier{
		cfg:     cfg,
		src:     src,
		pending: make(map[uint64]*pendingDigest),
	}
}

// Record registers an executed slot's digest for verification.
func (v *TrailingVerifier) Record(d *SlotDigest) {
	if v == nil || d == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.firstSlot == 0 || d.Slot < v.firstSlot {
		v.firstSlot = d.Slot
		if v.verified == 0 {
			v.verified = d.Slot - 1
		}
	}
	if _, exists := v.pending[d.Slot]; !exists {
		v.order = append(v.order, d.Slot)
	}
	v.pending[d.Slot] = &pendingDigest{digest: d}
	if d.Slot > v.executedTip {
		v.executedTip = d.Slot
	}
}

// RecordSkip registers a slot replay treated as skipped.
func (v *TrailingVerifier) RecordSkip(slot uint64) {
	v.Record(&SlotDigest{Slot: slot, Skipped: true})
}

// SetExecutedTip advances the tip used for the verification lag.
func (v *TrailingVerifier) SetExecutedTip(slot uint64) {
	if v == nil {
		return
	}
	v.mu.Lock()
	if slot > v.executedTip {
		v.executedTip = slot
	}
	v.mu.Unlock()
}

// VerifiedWatermark returns the highest slot V such that every recorded slot
// <= V verified clean. Before anything is recorded it returns 0 (nothing is
// foldable yet anyway). Nil receiver (verifier disabled) = no gating.
func (v *TrailingVerifier) VerifiedWatermark() uint64 {
	if v == nil {
		return ^uint64(0)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.verified
}

// Failure returns the first confirmed divergence (nil if none).
func (v *TrailingVerifier) Failure() *ReplayDivergence {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.failure
}

// PruneThrough drops verified bookkeeping for slots <= slot (post-fold).
func (v *TrailingVerifier) PruneThrough(slot uint64) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	kept := v.order[:0]
	for _, s := range v.order {
		if s <= slot {
			delete(v.pending, s)
			continue
		}
		kept = append(kept, s)
	}
	v.order = kept
}

// Run drives verification until ctx is done. One slot per permit, oldest
// first, capped at MaxRPS.
func (v *TrailingVerifier) Run(ctx context.Context) {
	interval := time.Second / time.Duration(v.cfg.MaxRPS)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.verifyNext()
		}
	}
}

// verifyNext picks the oldest eligible pending slot and verifies it.
func (v *TrailingVerifier) verifyNext() {
	v.mu.Lock()
	if v.failure != nil {
		v.mu.Unlock()
		return
	}
	var slot uint64
	var pd *pendingDigest
	now := time.Now()
	for _, s := range v.order {
		cand := v.pending[s]
		if cand == nil {
			continue
		}
		if v.executedTip < v.cfg.LagSlots || s > v.executedTip-v.cfg.LagSlots {
			break // too fresh; order is ascending so nothing later is eligible
		}
		if now.Before(cand.nextTry) {
			continue
		}
		slot, pd = s, cand
		break
	}
	v.mu.Unlock()
	if pd == nil {
		return
	}

	result, err := v.src.FetchFinalized(slot)
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.pending[slot] != pd { // pruned concurrently
		return
	}

	switch {
	case err == rpcclient.SlotSkipped:
		if pd.digest.Skipped {
			v.markVerifiedLocked(slot)
			return
		}
		// We executed a block; RPC (finalized) says skipped. Confirm across
		// retries before declaring divergence — transient RPC views exist.
		pd.skipHits++
		if pd.skipHits >= 3 {
			v.failure = &ReplayDivergence{Slot: slot, TxIndex: -1, Kind: "skip_mismatch",
				Detail: "replay executed a block but RPC finalized view reports the slot skipped"}
			return
		}
		pd.attempts++
		pd.nextTry = time.Now().Add(5 * time.Second)
		return
	case err != nil:
		// Transient (not yet finalized on the RPC view, outage, etc.): back off.
		pd.attempts++
		pd.nextTry = time.Now().Add(backoffFor(pd.attempts))
		return
	}

	if pd.digest.Skipped {
		// We skipped; RPC has a finalized block. Confirm, then diverge.
		pd.skipHits++
		if pd.skipHits >= 3 {
			v.failure = &ReplayDivergence{Slot: slot, TxIndex: -1, Kind: "skip_mismatch",
				Detail: "replay skipped the slot but RPC finalized view has a block"}
			return
		}
		pd.attempts++
		pd.nextTry = time.Now().Add(5 * time.Second)
		return
	}

	// Sibling check: a different blockhash is a fork-identity question owned
	// by the certificate gate, NOT an execution divergence. Requeue.
	if result.Blockhash != pd.digest.Blockhash {
		pd.siblingHits++
		v.requeues++
		if pd.siblingHits%10 == 0 {
			mlog.Log.Warnf("trailing verifier: slot %d blockhash differs from RPC finalized view (%d observations) — executed a non-canonical sibling? holding the fold watermark; the certificate gate owns identity",
				slot, pd.siblingHits)
		}
		pd.attempts++
		pd.nextTry = time.Now().Add(backoffFor(pd.attempts))
		return
	}

	if div := compareSlotDigest(pd.digest, result); div != nil {
		v.failure = div
		return
	}
	v.markVerifiedLocked(slot)
}

func backoffFor(attempts int) time.Duration {
	d := time.Duration(attempts) * 2 * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// markVerifiedLocked marks slot verified (pending -> nil) and advances the
// watermark over the contiguous verified prefix of recorded slots.
func (v *TrailingVerifier) markVerifiedLocked(slot uint64) {
	if _, ok := v.pending[slot]; !ok {
		return
	}
	v.pending[slot] = nil
	v.verifiedCount++
	for len(v.order) > 0 {
		s := v.order[0]
		if pd, ok := v.pending[s]; ok && pd != nil {
			break // oldest recorded slot still unverified
		}
		delete(v.pending, s)
		v.order = v.order[1:]
		if s > v.verified {
			v.verified = s
		}
	}
}

// compareSlotDigest recomputes each transaction's record hash from the
// attested view (using the recorded comparability mask) and compares.
func compareSlotDigest(d *SlotDigest, vb *verifiedBlock) *ReplayDivergence {
	if len(vb.Txs) != len(d.Txs) {
		return &ReplayDivergence{Slot: d.Slot, TxIndex: -1, Kind: "tx_count",
			Detail: fmt.Sprintf("replay executed %d transactions, attested block has %d", len(d.Txs), len(vb.Txs))}
	}
	for i := range vb.Txs {
		vt := &vb.Txs[i]
		td := &d.Txs[i]
		var sigPrefix [8]byte
		copy(sigPrefix[:], vt.Sig[:8])
		if sigPrefix != td.SigPrefix {
			return &ReplayDivergence{Slot: d.Slot, TxIndex: i, TxSignature: vt.Sig.String(), Kind: "tx_record",
				Detail: "transaction order/signature differs from attested block"}
		}
		if td.RecordHash == ([16]byte{}) {
			return &ReplayDivergence{Slot: d.Slot, TxIndex: i, TxSignature: vt.Sig.String(), Kind: "missing_record",
				Detail: "replay captured no execution record for this transaction"}
		}
		got := txRecordHash(vt.Sig, vt.Fee, vt.Failed, td.NumAccts, td.SkipMask, vt.Pre, vt.Post)
		if got != td.RecordHash {
			return &ReplayDivergence{Slot: d.Slot, TxIndex: i, TxSignature: vt.Sig.String(), Kind: "tx_record",
				Detail: fmt.Sprintf("fee/status/balance record differs from attested metadata (fee=%d failed=%v accts=%d)", vt.Fee, vt.Failed, td.NumAccts)}
		}
	}
	return nil
}
