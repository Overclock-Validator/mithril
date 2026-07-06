package alpenglow

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
)

type ChainConfig struct {
	// RequireVerifiedCertificates keeps unverified Votor certificates out of
	// the decision path. Observer tooling can disable this while the verifier is
	// still being brought up, but validator-grade replay should keep it enabled.
	RequireVerifiedCertificates bool
	// RequireStakeVerifiedCertificates requires certificates to have been
	// checked against the epoch validator stake set before they can drive chain
	// decisions. This is useful while aggregate BLS verification is being wired
	// in, because it still keeps malformed or under-staked certs out of the
	// resolver.
	RequireStakeVerifiedCertificates bool
}

func DefaultChainConfig() ChainConfig {
	return ChainConfig{
		RequireVerifiedCertificates: true,
	}
}

type ChainDecisionKind string

const (
	ChainDecisionKindBlock    ChainDecisionKind = "block"
	ChainDecisionKindSkip     ChainDecisionKind = "skip"
	ChainDecisionKindConflict ChainDecisionKind = "conflict"
)

type ChainDecision struct {
	Slot            uint64                `json:"slot"`
	Kind            ChainDecisionKind     `json:"kind"`
	Block           BlockID               `json:"block,omitempty"`
	Observed        bool                  `json:"observed,omitempty"`
	ParentSlot      uint64                `json:"parent_slot,omitempty"`
	ParentHash      solana.Hash           `json:"parent_hash,omitempty"`
	CertificateType CertificateType       `json:"certificate_type,omitempty"`
	Indirect        bool                  `json:"indirect,omitempty"`
	ViaFinalized    BlockID               `json:"via_finalized,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	Candidates      []ChainBlockCandidate `json:"candidates,omitempty"`
}

type ChainBlockCandidate struct {
	Block           BlockID         `json:"block"`
	Observed        bool            `json:"observed,omitempty"`
	ParentSlot      uint64          `json:"parent_slot,omitempty"`
	ParentHash      solana.Hash     `json:"parent_hash,omitempty"`
	CertificateType CertificateType `json:"certificate_type,omitempty"`
}

type ChainPath struct {
	AnchorSlot uint64          `json:"anchor_slot"`
	Decisions  []ChainDecision `json:"decisions"`
	BlockedAt  uint64          `json:"blocked_at,omitempty"`
	Conflict   bool            `json:"conflict,omitempty"`
}

type ChainSnapshot struct {
	CertificatesObserved         uint64  `json:"certificates_observed"`
	CertificatesAccepted         uint64  `json:"certificates_accepted"`
	CertificatesIgnoredUntrusted uint64  `json:"certificates_ignored_untrusted"`
	ReplayBlocksObserved         uint64  `json:"replay_blocks_observed"`
	CertifiedBlocks              uint64  `json:"certified_blocks"`
	CertifiedSkips               uint64  `json:"certified_skips"`
	IndirectSkips                uint64  `json:"indirect_skips"`
	DirectFinalizedBlocks        uint64  `json:"direct_finalized_blocks"`
	ConflictingSlots             uint64  `json:"conflicting_slots"`
	LatestCertificateSlot        uint64  `json:"latest_certificate_slot,omitempty"`
	LatestObservedBlock          BlockID `json:"latest_observed_block,omitempty"`
	LatestDirectFinalizedBlock   BlockID `json:"latest_direct_finalized_block,omitempty"`
}

type ChainCertificateUpdate struct {
	New      bool          `json:"new"`
	Trusted  bool          `json:"trusted"`
	Snapshot ChainSnapshot `json:"snapshot"`
}

type ChainReplayBlockUpdate struct {
	New      bool          `json:"new"`
	Snapshot ChainSnapshot `json:"snapshot"`
}

type ChainTracker struct {
	mu sync.RWMutex

	cfg ChainConfig

	certificates    map[CertificateKey]Certificate
	appliedCerts    map[CertificateKey]struct{} // certs already folded into chain state
	blocks          map[BlockID]*chainBlockState
	blockSlots      map[uint64]map[solana.Hash]BlockID
	skipCerts       map[uint64]Certificate
	finalizeCerts   map[uint64]Certificate
	directFinalized map[BlockID]CertificateType
	chainFinalized  map[BlockID]struct{} // finalized by ancestry of a finalized block
	// finalizedBySlot indexes the finalized block PER SLOT (direct or by
	// ancestry) so slot-keyed decision queries (CertifiedBlockAt, WantedBlocks)
	// can surface a finalized block even when it never received a certificate
	// of its own — blockSlots indexes certified blocks only, and a cert-less
	// ancestry-finalized parent would otherwise be invisible to the switch
	// sweep. First finalized block wins; a DIFFERENT finalized block at the
	// same slot is Byzantine and belongs to the conflict machinery.
	finalizedBySlot map[uint64]BlockID
	indirectSkips   map[uint64]chainIndirectSkip
	conflicts       map[uint64]chainConflict

	certificatesObserved         uint64
	certificatesAccepted         uint64
	certificatesIgnoredUntrusted uint64
	replayBlocksObserved         uint64
	latestCertificateSlot        uint64
	latestObservedBlock          BlockID
	latestDirectFinalizedBlock   BlockID

	// decisionVersion increments whenever the tracker becomes more decisive in a
	// way that could contradict an already-executed slot — not just on cert
	// acceptance but also on replay-derived parent links, finalized ancestry,
	// indirect skips, and conflicts. The execute-on-receipt switch sweep gates on
	// it, so it never misses a contradiction that arose without a new certificate.
	decisionVersion uint64
}

// bumpDecisionLocked marks that a decision-relevant change occurred.
func (t *ChainTracker) bumpDecisionLocked() { t.decisionVersion++ }

// DecisionVersion returns the monotonic decision-change counter.
func (t *ChainTracker) DecisionVersion() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.decisionVersion
}

type chainBlockState struct {
	block        BlockID
	observed     bool
	parentSlot   uint64
	parentHash   solana.Hash
	certificates map[CertificateType]Certificate
}

type chainIndirectSkip struct {
	slot            uint64
	viaFinalized    BlockID
	certificateType CertificateType
}

type chainConflict struct {
	slot       uint64
	reason     string
	candidates []ChainBlockCandidate
}

func NewChainTracker() *ChainTracker {
	return NewChainTrackerWithConfig(DefaultChainConfig())
}

func NewChainTrackerWithConfig(cfg ChainConfig) *ChainTracker {
	return &ChainTracker{
		cfg:             cfg,
		certificates:    make(map[CertificateKey]Certificate),
		appliedCerts:    make(map[CertificateKey]struct{}),
		blocks:          make(map[BlockID]*chainBlockState),
		blockSlots:      make(map[uint64]map[solana.Hash]BlockID),
		skipCerts:       make(map[uint64]Certificate),
		finalizeCerts:   make(map[uint64]Certificate),
		directFinalized: make(map[BlockID]CertificateType),
		chainFinalized:  make(map[BlockID]struct{}),
		indirectSkips:   make(map[uint64]chainIndirectSkip),
		finalizedBySlot: make(map[uint64]BlockID),
		conflicts:       make(map[uint64]chainConflict),
	}
}

func (t *ChainTracker) ObserveCertificate(cert Certificate) (ChainCertificateUpdate, error) {
	if err := validateChainCertificate(cert); err != nil {
		return ChainCertificateUpdate{Snapshot: t.Snapshot()}, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := cert.Key()
	if _, exists := t.certificates[key]; exists {
		// Already seen. If it was previously untrusted (e.g. validator set not yet
		// installed) and is now trustable, fold it in now — otherwise a cert that
		// arrived before its epoch's stakes would be lost, stalling finality.
		trusted := t.certificateTrustedLocked(cert)
		if _, applied := t.appliedCerts[key]; trusted && !applied {
			t.certificates[key] = cert
			t.appliedCerts[key] = struct{}{}
			t.certificatesAccepted++
			t.applyTrustedCertificateLocked(cert)
			return ChainCertificateUpdate{New: false, Trusted: true, Snapshot: t.snapshotLocked()}, nil
		}
		return ChainCertificateUpdate{New: false, Trusted: trusted, Snapshot: t.snapshotLocked()}, nil
	}

	t.certificates[key] = cert
	t.certificatesObserved++
	if cert.Slot > t.latestCertificateSlot {
		t.latestCertificateSlot = cert.Slot
	}

	trusted := t.certificateTrustedLocked(cert)
	if !trusted {
		t.certificatesIgnoredUntrusted++
		return ChainCertificateUpdate{New: true, Trusted: false, Snapshot: t.snapshotLocked()}, nil
	}

	t.appliedCerts[key] = struct{}{}
	t.certificatesAccepted++
	t.applyTrustedCertificateLocked(cert)
	return ChainCertificateUpdate{New: true, Trusted: true, Snapshot: t.snapshotLocked()}, nil
}

func (t *ChainTracker) ObserveReplayBlock(obs ReplayBlockObservation) ChainReplayBlockUpdate {
	t.mu.Lock()
	defer t.mu.Unlock()

	if obs.At.IsZero() {
		obs.At = time.Now()
	}
	t.replayBlocksObserved++
	if obs.Block.Slot > t.latestObservedBlock.Slot {
		t.latestObservedBlock = obs.Block
	}
	if obs.Block.IsZero() || !obs.Block.HasHash() {
		return ChainReplayBlockUpdate{New: false, Snapshot: t.snapshotLocked()}
	}

	state := t.ensureBlockStateLocked(obs.Block)
	wasObserved := state.observed
	state.observed = true
	prevParentSlot, prevParentHash := state.parentSlot, state.parentHash
	// Parent slot 0 means unknown — never clobber a known parent link with it, and
	// never replace a known parent hash with zero (indirect-skip and ancestry
	// derivation depend on the link surviving).
	if obs.ParentSlot != 0 {
		if obs.ParentSlot != state.parentSlot && obs.ParentHash.IsZero() {
			// New parent slot without a hash: drop the stale hash rather than pair
			// it with the wrong slot.
			state.parentHash = solana.Hash{}
		}
		state.parentSlot = obs.ParentSlot
		if !obs.ParentHash.IsZero() {
			state.parentHash = obs.ParentHash
		}
	}
	parentChanged := state.parentSlot != prevParentSlot || state.parentHash != prevParentHash

	derived := false
	if certType, finalized := t.directFinalized[obs.Block]; finalized {
		t.deriveIndirectSkipsLocked(obs.Block, certType)
		// The cert may have arrived before this observation supplied the parent
		// link — ancestry marking needs the link, so re-run it now.
		t.markChainFinalizedAncestorsLocked(obs.Block)
		derived = true
	} else if _, chainFin := t.chainFinalized[obs.Block]; chainFin {
		t.markChainFinalizedAncestorsLocked(obs.Block)
		derived = true
	}

	// A new parent link or a freshly-derived finalized-ancestry / indirect-skip
	// can contradict an executed slot without any new certificate — advance the
	// decision version so the switch sweep re-runs. A bare observation with no
	// link and no derivation changes nothing the sweep reads (it consults
	// certificates and derived skips), so it does not bump.
	if parentChanged || derived {
		t.bumpDecisionLocked()
	}

	return ChainReplayBlockUpdate{New: !wasObserved, Snapshot: t.snapshotLocked()}
}

func (t *ChainTracker) NextDecision(anchorSlot uint64) (ChainDecision, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nextDecisionLocked(anchorSlot)
}

func (t *ChainTracker) ResolvePath(anchorSlot uint64, maxDecisions int) ChainPath {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if maxDecisions <= 0 {
		maxDecisions = 1
	}
	path := ChainPath{AnchorSlot: anchorSlot}
	current := anchorSlot
	for len(path.Decisions) < maxDecisions {
		decision, ok := t.nextDecisionLocked(current)
		if !ok {
			path.BlockedAt = current + 1
			break
		}
		path.Decisions = append(path.Decisions, decision)
		if decision.Kind == ChainDecisionKindConflict {
			path.Conflict = true
			break
		}
		current = decision.Slot
	}
	return path
}

func (t *ChainTracker) Snapshot() ChainSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snapshotLocked()
}

func validateChainCertificate(cert Certificate) error {
	if err := cert.ValidateBasic(); err != nil {
		return err
	}
	if cert.Type.HasBlock() && cert.BlockHash == (solana.Hash{}) {
		return fmt.Errorf("%s certificate for slot %d has empty block hash", cert.Type, cert.Slot)
	}
	return nil
}

func (t *ChainTracker) certificateTrustedLocked(cert Certificate) bool {
	if t.cfg.RequireVerifiedCertificates && !cert.SignatureVerified {
		return false
	}
	if t.cfg.RequireStakeVerifiedCertificates && !cert.StakeVerified {
		return false
	}
	return true
}

func (t *ChainTracker) applyTrustedCertificateLocked(cert Certificate) {
	switch cert.Type {
	case CertificateNotarize, CertificateNotarizeFallback, CertificateFinalizeFast, CertificateGenesis:
		t.applyTrustedBlockCertificateLocked(cert)
	case CertificateFinalize:
		t.finalizeCerts[cert.Slot] = cert
		t.tryMarkSlowFinalizedLocked(cert.Slot)
		t.refreshConflictLocked(cert.Slot) // finalization creates exclusivity (Lemma 26)
	case CertificateSkip:
		t.skipCerts[cert.Slot] = cert
		t.refreshConflictLocked(cert.Slot)
	}
	// A trusted cert (and everything it just derived) can newly contradict an
	// executed slot — advance the decision version so the switch sweep re-runs.
	t.bumpDecisionLocked()
}

func (t *ChainTracker) applyTrustedBlockCertificateLocked(cert Certificate) {
	block, ok := cert.Block()
	if !ok || !block.HasHash() {
		return
	}

	state := t.ensureBlockStateLocked(block)
	state.certificates[cert.Type] = cert
	if t.blockSlots[block.Slot] == nil {
		t.blockSlots[block.Slot] = make(map[solana.Hash]BlockID)
	}
	t.blockSlots[block.Slot][block.Hash] = block

	switch cert.Type {
	case CertificateFinalizeFast, CertificateGenesis:
		t.markDirectFinalizedLocked(block, cert.Type)
	case CertificateNotarize:
		t.tryMarkSlowFinalizedLocked(block.Slot)
	}
	t.refreshConflictLocked(block.Slot)
}

func (t *ChainTracker) ensureBlockStateLocked(block BlockID) *chainBlockState {
	state := t.blocks[block]
	if state == nil {
		state = &chainBlockState{
			block:        block,
			certificates: make(map[CertificateType]Certificate),
		}
		t.blocks[block] = state
	}
	return state
}

func (t *ChainTracker) tryMarkSlowFinalizedLocked(slot uint64) {
	if _, ok := t.finalizeCerts[slot]; !ok {
		return
	}
	for _, block := range t.blockSlots[slot] {
		state := t.blocks[block]
		if state == nil {
			continue
		}
		if _, notarized := state.certificates[CertificateNotarize]; notarized {
			t.markDirectFinalizedLocked(block, CertificateFinalize)
		}
	}
}

func (t *ChainTracker) markDirectFinalizedLocked(block BlockID, certType CertificateType) {
	if block.IsZero() || !block.HasHash() {
		return
	}
	if existing, ok := t.directFinalized[block]; ok && existing == certType {
		return
	}
	t.directFinalized[block] = certType
	if _, taken := t.finalizedBySlot[block.Slot]; !taken {
		t.finalizedBySlot[block.Slot] = block
	}
	if block.Slot >= t.latestDirectFinalizedBlock.Slot {
		t.latestDirectFinalizedBlock = block
		// Bound memory on long runs: finalized slots well behind the watermark are
		// settled and no longer needed for decisions or indirect-skip derivation.
		if block.Slot > chainTrackerRetentionSlots {
			t.pruneBeforeSlotLocked(block.Slot - chainTrackerRetentionSlots)
		}
	}
	t.deriveIndirectSkipsLocked(block, certType)
	t.markChainFinalizedAncestorsLocked(block)
}

// markChainFinalizedAncestorsLocked walks parent links from a finalized block,
// marking ancestors finalized-by-ancestry so their slots resolve to block decisions
// even when they only carry fallback certs. The walk stops at unobserved parents or
// ambiguity (several certified blocks at the parent slot with no exact match).
func (t *ChainTracker) markChainFinalizedAncestorsLocked(block BlockID) {
	for {
		state := t.blocks[block]
		if state == nil || !state.observed || state.parentSlot == 0 || state.parentSlot >= block.Slot {
			return
		}
		parent := BlockID{Slot: state.parentSlot, Hash: state.parentHash}
		if state.parentHash.IsZero() {
			// No parent hash at all. Fall back to the parent slot's single
			// certified block ONLY if it carries a unique-strength cert
			// (notarize/fast-finalize/genesis — provably the slot's one block,
			// Lemmas 21(i)/24). A fallback-only cert could be an equivocation twin.
			slotBlocks := t.blockSlots[state.parentSlot]
			if len(slotBlocks) != 1 {
				return
			}
			for _, id := range slotBlocks {
				parent = id
			}
			st := t.blocks[parent]
			if st == nil {
				return
			}
			switch strongestBlockCertificateType(st.certificates) {
			case CertificateNotarize, CertificateFinalizeFast, CertificateGenesis:
			default:
				return
			}
		} else if _, known := t.blocks[parent]; !known {
			// The finalized child's header names its parent hash EXACTLY, but no
			// cert or replay observation tracks that block yet (e.g. replay
			// executed an equivocation twin, or the block was never fetched).
			// The hash binding is protocol-final — mint a stub so the finalized
			// identity is queryable (CertifiedBlockAt) and repairable
			// (WantedBlocks). The walk stops at the stub (not observed, no
			// parent link of its own) on the next iteration.
			t.ensureBlockStateLocked(parent)
		}
		if _, done := t.chainFinalized[parent]; done {
			return
		}
		t.chainFinalized[parent] = struct{}{}
		if _, taken := t.finalizedBySlot[parent.Slot]; !taken {
			t.finalizedBySlot[parent.Slot] = parent
		}
		// Ancestry finalization creates the same exclusivity as direct finalization.
		t.refreshConflictLocked(parent.Slot)
		block = parent
	}
}

// maxCertifiedBlocksPerSlot bounds notar-fallback-or-stronger certified blocks per
// slot. It follows from the whitepaper's per-validator vote budget (Def. 12: one
// notarize plus at most three notar-fallback votes = four block entries per voter;
// with 60% required per cert, at most 4/0.6 ≈ 6.7 → 7 distinct blocks can ever be
// certified). More is protocol-impossible — cryptographic evidence of an attack.
const maxCertifiedBlocksPerSlot = 7

// chainTrackerRetentionSlots is how many slots of state the tracker keeps behind the
// finalized watermark. Live decisions are only made near the tip (the block source
// gates on isNearTip, ~32-64 slots), so a window this far behind finality can never
// remove state a live decision or indirect-skip derivation still needs.
const chainTrackerRetentionSlots = 512

// FinalizedBlockAt returns the finalized block for slot when it is unambiguous.
// ok is false when nothing finalized is known (including pruned history), the slot
// is flagged conflicted, or more than one finalized block exists (Byzantine
// evidence) — a caller must never promote through an ambiguous slot.
func (t *ChainTracker) FinalizedBlockAt(slot uint64) (BlockID, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, conflicted := t.conflicts[slot]; conflicted {
		return BlockID{}, false
	}
	var found BlockID
	matches := 0
	for _, id := range t.blockSlots[slot] {
		if t.finalizedLocked(id) {
			found = id
			matches++
		}
	}
	if matches != 1 {
		return BlockID{}, false
	}
	return found, true
}

// FinalityConflictAt reports whether the slot carries a recorded safety violation.
func (t *ChainTracker) FinalityConflictAt(slot uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.conflicts[slot]
	return ok
}

// PruneBeforeSlot drops all tracker state for slots strictly below slot, bounding
// memory on a long-running node. Pruning runs automatically behind finality; this
// exported form lets a caller prune explicitly (e.g. behind the rooted watermark).
// CertifiedBlockAt returns the slot's DECISIVELY certified block: one backed
// by a unique-strength certificate (notarize / finalize-fast / genesis — at
// most one per slot by protocol, Lemma 21(i)/24) or finalized directly or by
// ancestry. Fallback-only candidates are ambiguous (up to 7 can legally
// coexist) and never returned. This is the execute-on-receipt switch signal:
// an executed block contradicting the decisive block must be unwound.
func (t *ChainTracker) CertifiedBlockAt(slot uint64) (BlockID, CertificateType, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var winner BlockID
	var winnerType CertificateType
	found := false
	// A finalized block (direct or by ancestry) is decisive even when it never
	// received a certificate of its own — the cert-less ancestry-finalized
	// parent case, which the cert-only blockSlots scan below cannot see.
	if fin, ok := t.finalizedBySlot[slot]; ok {
		winner, found = fin, true
		if state := t.blocks[fin]; state != nil {
			winnerType = strongestBlockCertificateType(state.certificates)
		}
	}
	// Iterate the slot's blocks directly (no candidate-slice allocation): the
	// switch sweep calls this for every executed-unfolded slot whenever the
	// tracker's decision version advances — which on a healthy cluster is
	// nearly every block.
	for _, block := range t.blockSlots[slot] {
		state := t.blocks[block]
		if state == nil {
			continue
		}
		certType := strongestBlockCertificateType(state.certificates)
		decisive := false
		switch certType {
		case CertificateFinalizeFast, CertificateNotarize, CertificateGenesis:
			decisive = true
		}
		if !decisive {
			if _, fin := t.directFinalized[block]; fin {
				decisive = true
			} else if _, fin := t.chainFinalized[block]; fin {
				decisive = true
			}
		}
		if !decisive {
			continue
		}
		if found && winner != block {
			// Two decisive blocks in one slot is Byzantine evidence; the
			// conflict machinery owns it — report no decisive block here.
			return BlockID{}, "", false
		}
		winner, winnerType, found = block, certType, true
	}
	return winner, winnerType, found
}

// SkipCertifiedAt reports whether the slot is certified skipped, explicitly
// (skip cert) or indirectly (omitted between finalized ancestors).
func (t *ChainTracker) SkipCertifiedAt(slot uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, ok := t.skipCerts[slot]; ok {
		return true
	}
	_, ok := t.indirectSkips[slot]
	return ok
}

// WantedBlock names a certified block whose data replay has not observed yet —
// the target of cert-driven repair.
type WantedBlock struct {
	Block     BlockID
	Strongest CertificateType
	Finalized bool
}

// wantedPriority ranks a slot's candidates for repair: a finalized block wins,
// then a unique-strength certificate (notarize / fast-finalize / genesis), then
// a fallback. -1 means it is not a repair target. Picking the highest-priority
// candidate per slot (rather than the lowest hash) keeps the repair loop — which
// nudges at most once per slot — aimed at the DECISIVE block, not a fallback
// sibling that merely happens to sort first.
func wantedPriority(ct CertificateType, finalized bool) int {
	if finalized {
		return 3
	}
	switch ct {
	case CertificateFinalizeFast, CertificateNotarize, CertificateGenesis:
		return 2
	case CertificateNotarizeFallback:
		return 1
	default:
		return -1
	}
}

// WantedBlocks returns ONE certified-but-unobserved repair target per slot
// strictly above afterSlot, ascending by slot, capped at max. Within a slot the
// most decisive candidate is chosen (finalized > unique-strength > fallback),
// tie-broken by lowest hash for determinism — so a fallback is targeted only
// when no decisive candidate exists. Skip-certified slots are excluded unless
// the block is finalized (finality outranks a skip; the illegal coexistence is
// the conflict machinery's to flag). The scan is bounded by the tracker's
// retention window and the <= 7 certified candidates per slot protocol bound.
func (t *ChainTracker) WantedBlocks(afterSlot uint64, max int) []WantedBlock {
	if max <= 0 {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	slots := make([]uint64, 0, len(t.blockSlots))
	seenSlot := make(map[uint64]struct{}, len(t.blockSlots))
	for slot := range t.blockSlots {
		if slot > afterSlot {
			slots = append(slots, slot)
			seenSlot[slot] = struct{}{}
		}
	}
	// A cert-less ancestry-finalized block's slot may have NO certified blocks
	// at all — it must still be repairable (it is the decisive block).
	for slot := range t.finalizedBySlot {
		if slot > afterSlot {
			if _, dup := seenSlot[slot]; !dup {
				slots = append(slots, slot)
			}
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	out := make([]WantedBlock, 0, min(max, len(slots)))
	for _, slot := range slots {
		if len(out) >= max {
			break
		}
		_, skipped := t.skipCerts[slot]
		if !skipped {
			_, skipped = t.indirectSkips[slot]
		}
		// Pick the single most decisive unobserved candidate for the slot.
		var best *WantedBlock
		bestPri := -1
		// The finalized block first (may be cert-less — absent from the
		// certified-candidates scan below).
		if fin, ok := t.finalizedBySlot[slot]; ok {
			if state := t.blocks[fin]; state != nil && !state.observed {
				w := WantedBlock{Block: fin, Strongest: strongestBlockCertificateType(state.certificates), Finalized: true}
				best, bestPri = &w, wantedPriority(w.Strongest, true)
			}
		}
		for _, cand := range t.blockCandidatesLocked(slot) {
			if cand.Observed {
				continue
			}
			finalized := t.finalizedLocked(cand.Block)
			pri := wantedPriority(cand.CertificateType, finalized)
			if pri < 0 {
				continue // tracked but uncertified (e.g. replay-observed sibling)
			}
			if skipped && !finalized {
				continue // skip-certified slot: only a finalized block overrides
			}
			if best == nil || pri > bestPri ||
				(pri == bestPri && bytes.Compare(cand.Block.Hash[:], best.Block.Hash[:]) < 0) {
				w := WantedBlock{Block: cand.Block, Strongest: cand.CertificateType, Finalized: finalized}
				best, bestPri = &w, pri
			}
		}
		if best != nil {
			out = append(out, *best)
		}
	}
	return out
}

func (t *ChainTracker) PruneBeforeSlot(slot uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneBeforeSlotLocked(slot)
}

func (t *ChainTracker) pruneBeforeSlotLocked(slot uint64) {
	if slot == 0 {
		return
	}
	for k := range t.certificates {
		if k.Slot < slot {
			delete(t.certificates, k)
			delete(t.appliedCerts, k)
		}
	}
	for k := range t.appliedCerts {
		if k.Slot < slot {
			delete(t.appliedCerts, k)
		}
	}
	for id := range t.blocks {
		if id.Slot < slot {
			delete(t.blocks, id)
		}
	}
	for id := range t.directFinalized {
		if id.Slot < slot {
			delete(t.directFinalized, id)
		}
	}
	for id := range t.chainFinalized {
		if id.Slot < slot {
			delete(t.chainFinalized, id)
		}
	}
	for s := range t.finalizedBySlot {
		if s < slot {
			delete(t.finalizedBySlot, s)
		}
	}
	for s := range t.blockSlots {
		if s < slot {
			delete(t.blockSlots, s)
		}
	}
	for s := range t.skipCerts {
		if s < slot {
			delete(t.skipCerts, s)
		}
	}
	for s := range t.finalizeCerts {
		if s < slot {
			delete(t.finalizeCerts, s)
		}
	}
	for s := range t.indirectSkips {
		if s < slot {
			delete(t.indirectSkips, s)
		}
	}
	// conflicts are deliberately NOT pruned: they are rare, tiny, permanent Byzantine
	// evidence, and the promotion gate must still fail closed when the executed tip
	// reaches a conflicted slot long after the cert watermark passed it.
}

func (t *ChainTracker) deriveIndirectSkipsLocked(block BlockID, certType CertificateType) {
	state := t.blocks[block]
	if state == nil || !state.observed || state.parentSlot == 0 || state.parentSlot >= block.Slot {
		return
	}
	for slot := state.parentSlot + 1; slot < block.Slot; slot++ {
		t.indirectSkips[slot] = chainIndirectSkip{
			slot:            slot,
			viaFinalized:    block,
			certificateType: certType,
		}
		t.refreshConflictLocked(slot)
	}
}

func (t *ChainTracker) nextDecisionLocked(anchorSlot uint64) (ChainDecision, bool) {
	slot := anchorSlot + 1
	if conflict, ok := t.conflicts[slot]; ok {
		return ChainDecision{
			Slot:       slot,
			Kind:       ChainDecisionKindConflict,
			Reason:     conflict.reason,
			Candidates: conflict.candidates,
		}, true
	}
	if cert, ok := t.skipCerts[slot]; ok {
		return ChainDecision{
			Slot:            slot,
			Kind:            ChainDecisionKindSkip,
			CertificateType: cert.Type,
			Reason:          "skip certificate",
		}, true
	}
	if indirect, ok := t.indirectSkips[slot]; ok {
		return ChainDecision{
			Slot:            slot,
			Kind:            ChainDecisionKindSkip,
			CertificateType: indirect.certificateType,
			Indirect:        true,
			ViaFinalized:    indirect.viaFinalized,
			Reason:          "omitted by finalized ancestor chain",
		}, true
	}
	// A block decision needs decisive strength: a notarize/fast-finalize/genesis cert
	// (unique per slot: Lemmas 21(i)/24) or membership in a finalized chain. Fallback
	// certs alone are ambiguous (up to 7 blocks per slot can carry one) — wait.
	candidates := t.blockCandidatesLocked(slot)
	var decisive []ChainBlockCandidate
	for _, c := range candidates {
		switch c.CertificateType {
		case CertificateFinalizeFast, CertificateNotarize, CertificateGenesis:
			decisive = append(decisive, c)
			continue
		}
		if t.finalizedLocked(c.Block) {
			decisive = append(decisive, c)
		}
	}
	switch len(decisive) {
	case 0:
		return ChainDecision{}, false
	case 1:
		candidate := decisive[0]
		return ChainDecision{
			Slot:            slot,
			Kind:            ChainDecisionKindBlock,
			Block:           candidate.Block,
			Observed:        candidate.Observed,
			ParentSlot:      candidate.ParentSlot,
			ParentHash:      candidate.ParentHash,
			CertificateType: candidate.CertificateType,
			Reason:          "block certificate",
		}, true
	default: // defensive: refreshConflictLocked should already have flagged this
		return ChainDecision{
			Slot:       slot,
			Kind:       ChainDecisionKindConflict,
			Reason:     "multiple notarized blocks for slot",
			Candidates: candidates,
		}, true
	}
}

// finalizedLocked reports whether the block is finalized directly (fast/slow/genesis
// certificate) or by ancestry of a finalized block.
func (t *ChainTracker) finalizedLocked(block BlockID) bool {
	if _, ok := t.directFinalized[block]; ok {
		return true
	}
	_, ok := t.chainFinalized[block]
	return ok
}

func (t *ChainTracker) blockCandidatesLocked(slot uint64) []ChainBlockCandidate {
	slotBlocks := t.blockSlots[slot]
	if len(slotBlocks) == 0 {
		return nil
	}
	candidates := make([]ChainBlockCandidate, 0, len(slotBlocks))
	for _, block := range slotBlocks {
		state := t.blocks[block]
		if state == nil {
			continue
		}
		candidates = append(candidates, ChainBlockCandidate{
			Block:           block,
			Observed:        state.observed,
			ParentSlot:      state.parentSlot,
			ParentHash:      state.parentHash,
			CertificateType: strongestBlockCertificateType(state.certificates),
		})
	}
	return candidates
}

func strongestBlockCertificateType(certs map[CertificateType]Certificate) CertificateType {
	if _, ok := certs[CertificateFinalizeFast]; ok {
		return CertificateFinalizeFast
	}
	if _, ok := certs[CertificateNotarize]; ok {
		return CertificateNotarize
	}
	if _, ok := certs[CertificateNotarizeFallback]; ok {
		return CertificateNotarizeFallback
	}
	if _, ok := certs[CertificateGenesis]; ok {
		return CertificateGenesis
	}
	return ""
}

// refreshConflictLocked flags only true Alpenglow safety violations (whitepaper
// Lemmas 21/24/26). Notarize/notar-fallback certs legally coexist with skips and
// with each other pre-finality — only finalized blocks carry exclusivity.
func (t *ChainTracker) refreshConflictLocked(slot uint64) {
	candidates := t.blockCandidatesLocked(slot)
	_, hasExplicitSkip := t.skipCerts[slot]
	_, hasIndirectSkip := t.indirectSkips[slot]
	hasSkip := hasExplicitSkip || hasIndirectSkip

	// Unique-strength certs (at most one per slot: Lemmas 21(i)/24) and finalized
	// blocks (direct or by ancestry — both carry exclusivity).
	var unique, finalized int
	for _, c := range candidates {
		switch c.CertificateType {
		case CertificateFinalizeFast, CertificateNotarize, CertificateGenesis:
			unique++
		}
		if t.finalizedLocked(c.Block) {
			finalized++
		}
	}

	switch {
	case len(candidates) > maxCertifiedBlocksPerSlot:
		// See maxCertifiedBlocksPerSlot: beyond the vote-budget bound is
		// protocol-impossible — cryptographic evidence of an attack.
		t.conflicts[slot] = chainConflict{
			slot:       slot,
			reason:     "certified blocks exceed the per-slot protocol bound",
			candidates: candidates,
		}
	case unique > 1: // two notarized (or fast-finalized) blocks in one slot
		t.conflicts[slot] = chainConflict{
			slot:       slot,
			reason:     "multiple notarized blocks for slot",
			candidates: candidates,
		}
	case finalized > 0 && hasSkip: // finalized block contradicted by a skip (Lemmas 21(iii)/26(iii))
		t.conflicts[slot] = chainConflict{
			slot:       slot,
			reason:     "finalized block contradicted by skip",
			candidates: candidates,
		}
	case finalized > 0 && len(candidates) > 1: // finalized block plus another certified block (Lemmas 21(i,ii)/26(i,ii))
		t.conflicts[slot] = chainConflict{
			slot:       slot,
			reason:     "finalized block plus competing certified block",
			candidates: candidates,
		}
	}
	// No delete arm: recorded violations are write-once Byzantine evidence. The
	// conditions are monotone while a slot's state is retained, and after pruning a
	// single re-observed cert would otherwise rebuild the slot as conflict-free and
	// erase the flag right before the promotion gate consults it.
}

func (t *ChainTracker) snapshotLocked() ChainSnapshot {
	return ChainSnapshot{
		CertificatesObserved:         t.certificatesObserved,
		CertificatesAccepted:         t.certificatesAccepted,
		CertificatesIgnoredUntrusted: t.certificatesIgnoredUntrusted,
		ReplayBlocksObserved:         t.replayBlocksObserved,
		CertifiedBlocks:              uint64(len(t.blocks)),
		CertifiedSkips:               uint64(len(t.skipCerts)),
		IndirectSkips:                uint64(len(t.indirectSkips)),
		DirectFinalizedBlocks:        uint64(len(t.directFinalized)),
		ConflictingSlots:             uint64(len(t.conflicts)),
		LatestCertificateSlot:        t.latestCertificateSlot,
		LatestObservedBlock:          t.latestObservedBlock,
		LatestDirectFinalizedBlock:   t.latestDirectFinalizedBlock,
	}
}
