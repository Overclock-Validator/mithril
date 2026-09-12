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
	New            bool          `json:"new"`
	Conflict       bool          `json:"conflict,omitempty"`
	ConflictSlot   uint64        `json:"conflict_slot,omitempty"`
	ConflictReason string        `json:"conflict_reason,omitempty"`
	Snapshot       ChainSnapshot `json:"snapshot"`
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
	// chainFinalized records the certificate type of the finalized descendant
	// that rooted each ancestor. Retaining it lets later parent observations
	// derive every skipped gap in the finalized chain.
	chainFinalized map[BlockID]CertificateType
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
	// invalidBlocks contains exact block identities rejected by deterministic
	// execution-independent validation (for example duplicate transaction
	// messages). It is deliberately separate from certificate conflicts: a
	// fallback-only certificate for an invalid block is not by itself a safety
	// violation, but the identity must never be repaired, selected, or extended.
	invalidBlocks map[BlockID]string
	// prunedBeforeSlot is the durable AccountsDB boundary. Consensus input
	// below it is historical and must not regrow decision state after pruning.
	prunedBeforeSlot uint64

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
		chainFinalized:  make(map[BlockID]CertificateType),
		indirectSkips:   make(map[uint64]chainIndirectSkip),
		finalizedBySlot: make(map[uint64]BlockID),
		conflicts:       make(map[uint64]chainConflict),
		invalidBlocks:   make(map[BlockID]string),
	}
}

// ObserveObjectivelyInvalidBlock hard-rejects an exact block identity and every
// already-observed descendant whose exact parent link reaches it. Validation
// failure alone is not a consensus safety fault: a fallback-only certificate is
// merely a repair lead. Any decisive certificate or finalized ancestry involving
// the invalid identity is contradictory evidence and permanently conflicts.
func (t *ChainTracker) ObserveObjectivelyInvalidBlock(block BlockID, reason string) error {
	if block.IsZero() || !block.HasHash() {
		return fmt.Errorf("objectively invalid block has no exact identity")
	}
	if reason == "" {
		reason = "deterministic block validation failed"
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if block.Slot < t.prunedBeforeSlot {
		return nil
	}
	versionBefore := t.decisionVersion
	firstUnsafe, changed := t.markObjectivelyInvalidBlockLocked(block, reason)
	if changed && t.decisionVersion == versionBefore {
		t.bumpDecisionLocked()
	}
	if !firstUnsafe.IsZero() {
		conflict := t.conflicts[firstUnsafe.Slot]
		return fmt.Errorf("objectively invalid chain conflicts at slot %d: %s", firstUnsafe.Slot, conflict.reason)
	}
	return nil
}

// markObjectivelyInvalidBlockLocked performs the ancestry cascade while t.mu is
// held. It returns the earliest invalid identity carrying decisive safety
// evidence and whether this call added any tombstone.
func (t *ChainTracker) markObjectivelyInvalidBlockLocked(block BlockID, reason string) (BlockID, bool) {
	changed := false
	firstUnsafe := BlockID{}
	queue := []BlockID{block}
	seen := make(map[BlockID]struct{})
	for len(queue) > 0 {
		invalid := queue[0]
		queue = queue[1:]
		if _, done := seen[invalid]; done {
			continue
		}
		seen[invalid] = struct{}{}
		invalidReason := reason
		if invalid != block {
			invalidReason = fmt.Sprintf("descends from objectively invalid block %s: %s", block, reason)
		}
		if _, exists := t.invalidBlocks[invalid]; !exists {
			t.invalidBlocks[invalid] = invalidReason
			changed = true
		}
		if evidence := t.invalidBlockSafetyEvidenceLocked(invalid); evidence != "" {
			t.recordConflictLocked(invalid.Slot, fmt.Sprintf("objectively invalid block %s has %s", invalid, evidence))
			if firstUnsafe.IsZero() || invalid.Slot < firstUnsafe.Slot {
				firstUnsafe = invalid
			}
		}
		// A bounded scan is sufficient: ChainTracker already retains only its
		// consensus window, and it handles both parent-first and child-first arrival.
		for child, state := range t.blocks {
			if state == nil || !state.observed || state.parentSlot != invalid.Slot || state.parentHash != invalid.Hash {
				continue
			}
			queue = append(queue, child)
		}
	}
	return firstUnsafe, changed
}

// IsObjectivelyInvalidBlock reports whether block, or its exact observed
// ancestry, has been rejected by deterministic validation.
func (t *ChainTracker) IsObjectivelyInvalidBlock(block BlockID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.isObjectivelyInvalidBlockLocked(block)
}

// ObjectivelyInvalidBlocks returns the retained invalid identities, including
// descendants discovered through exact observed parent links. Callers use this
// snapshot to mirror the tracker's tombstones into active non-consensus caches.
func (t *ChainTracker) ObjectivelyInvalidBlocks() []BlockID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	blocks := make([]BlockID, 0, len(t.invalidBlocks))
	for block := range t.invalidBlocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blockLess(blocks[i], blocks[j]) })
	return blocks
}

func (t *ChainTracker) isObjectivelyInvalidBlockLocked(block BlockID) bool {
	_, invalid := t.invalidBlocks[block]
	return invalid
}

func (t *ChainTracker) invalidBlockSafetyEvidenceLocked(block BlockID) string {
	if _, ok := t.directFinalized[block]; ok {
		return "direct finality"
	}
	if _, ok := t.chainFinalized[block]; ok {
		return "finalized ancestry"
	}
	if finalized, ok := t.finalizedBySlot[block.Slot]; ok && finalized == block {
		return "finalized slot selection"
	}
	state := t.blocks[block]
	if state == nil {
		return ""
	}
	for _, certType := range []CertificateType{CertificateGenesis, CertificateFinalizeFast, CertificateNotarize} {
		if _, ok := state.certificates[certType]; ok {
			return fmt.Sprintf("decisive %s certificate", certType)
		}
	}
	return ""
}

func (t *ChainTracker) ObserveCertificate(cert Certificate) (ChainCertificateUpdate, error) {
	if err := validateChainCertificate(cert); err != nil {
		return ChainCertificateUpdate{Snapshot: t.Snapshot()}, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if cert.Slot < t.prunedBeforeSlot {
		return ChainCertificateUpdate{Snapshot: t.snapshotLocked()}, nil
	}

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

// ObserveFinalized applies the sole finalization decision emitted by
// ConsensusPool. Certificates populate the chain graph, but ChainTracker does
// not independently pair them into a second finality authority.
func (t *ChainTracker) ObserveFinalized(block BlockID, certType CertificateType) error {
	if block.IsZero() || !block.HasHash() {
		return fmt.Errorf("finalization decision has no block identity")
	}
	if certType != CertificateFinalize && certType != CertificateFinalizeFast && certType != CertificateGenesis {
		return fmt.Errorf("certificate %s cannot finalize block %s", certType, block)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if block.Slot < t.prunedBeforeSlot {
		return nil
	}
	state := t.blocks[block]
	switch certType {
	case CertificateFinalize:
		if _, ok := t.finalizeCerts[block.Slot]; !ok {
			return fmt.Errorf("slow finalization for block %s has no accepted finalize certificate", block)
		}
		if state == nil {
			return fmt.Errorf("slow finalization for block %s has no accepted notarize certificate", block)
		}
		if _, ok := state.certificates[CertificateNotarize]; !ok {
			return fmt.Errorf("slow finalization for block %s has no accepted notarize certificate", block)
		}
	case CertificateFinalizeFast, CertificateGenesis:
		if state == nil {
			return fmt.Errorf("finalization for block %s has no accepted %s certificate", block, certType)
		}
		if _, ok := state.certificates[certType]; !ok {
			return fmt.Errorf("finalization for block %s has no accepted %s certificate", block, certType)
		}
	}

	conflictsBefore := len(t.conflicts)
	_, wasDirectFinalized := t.directFinalized[block]
	decisionVersionBefore := t.decisionVersion
	t.markDirectFinalizedLocked(block, certType)
	if conflict, ok := t.conflicts[block.Slot]; ok {
		return fmt.Errorf("finalization conflict at slot %d: %s", block.Slot, conflict.reason)
	}
	if len(t.conflicts) > conflictsBefore {
		return fmt.Errorf("finalization of block %s exposed conflicting finalized ancestry", block)
	}
	// Certificate acceptance and pool finalization are separate tracker
	// transitions. The certificate already re-arms decision consumers, but the
	// finalization step can additionally select exact ancestry and derive omitted
	// slots. Re-arm once for a newly installed direct-finality decision so a
	// concurrent consumer that observed the certificate version cannot miss the
	// ancestry change. Idempotent ObserveFinalized calls do not bump again.
	if _, nowDirectFinalized := t.directFinalized[block]; !wasDirectFinalized && nowDirectFinalized && t.decisionVersion == decisionVersionBefore {
		t.bumpDecisionLocked()
	}
	return nil
}

func (t *ChainTracker) ObserveReplayBlock(obs ReplayBlockObservation) ChainReplayBlockUpdate {
	t.mu.Lock()
	defer t.mu.Unlock()
	if obs.Block.Slot < t.prunedBeforeSlot {
		return ChainReplayBlockUpdate{Snapshot: t.snapshotLocked()}
	}

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
	conflictsBefore := len(t.conflicts)
	state.observed = true
	prevParentSlot, prevParentHash := state.parentSlot, state.parentHash
	if obs.ParentSlot != 0 {
		switch {
		case obs.ParentSlot >= obs.Block.Slot:
			t.recordConflictLocked(obs.Block.Slot, "replayed block has a non-ancestor parent slot")
			return t.replayBlockUpdateLocked(wasObserved, conflictsBefore, obs.Block.Slot)
		case state.parentSlot != 0 && state.parentSlot != obs.ParentSlot:
			t.recordConflictLocked(obs.Block.Slot, "replayed block identity has conflicting parent slots")
			return t.replayBlockUpdateLocked(wasObserved, conflictsBefore, obs.Block.Slot)
		case !state.parentHash.IsZero() && !obs.ParentHash.IsZero() && state.parentHash != obs.ParentHash:
			t.recordConflictLocked(obs.Block.Slot, "replayed block identity has conflicting parent block IDs")
			return t.replayBlockUpdateLocked(wasObserved, conflictsBefore, obs.Block.Slot)
		}
		state.parentSlot = obs.ParentSlot
		if !obs.ParentHash.IsZero() {
			state.parentHash = obs.ParentHash
		}
	}
	parentChanged := state.parentSlot != prevParentSlot || state.parentHash != prevParentHash

	invalidReason, invalid := t.invalidBlocks[obs.Block]
	if !invalid && state.parentSlot != 0 && !state.parentHash.IsZero() {
		parent := BlockID{Slot: state.parentSlot, Hash: state.parentHash}
		if parentReason, parentInvalid := t.invalidBlocks[parent]; parentInvalid {
			invalid = true
			invalidReason = fmt.Sprintf("exact parent %s is objectively invalid: %s", parent, parentReason)
		}
	}
	if invalid {
		versionBefore := t.decisionVersion
		t.markObjectivelyInvalidBlockLocked(obs.Block, invalidReason)
		if parentChanged && t.decisionVersion == versionBefore {
			t.bumpDecisionLocked()
		}
		return t.replayBlockUpdateLocked(wasObserved, conflictsBefore, obs.Block.Slot)
	}

	derived := false
	if certType, finalized := t.directFinalized[obs.Block]; finalized {
		// The cert may have arrived before this observation supplied the parent
		// link — ancestry and skipped-gap derivation need the link, so re-run now.
		t.markChainFinalizedAncestorsLocked(obs.Block, certType)
		derived = true
	} else if certType, chainFin := t.chainFinalized[obs.Block]; chainFin {
		t.markChainFinalizedAncestorsLocked(obs.Block, certType)
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

	return t.replayBlockUpdateLocked(wasObserved, conflictsBefore, obs.Block.Slot)
}

func (t *ChainTracker) replayBlockUpdateLocked(wasObserved bool, conflictsBefore int, preferredSlot uint64) ChainReplayBlockUpdate {
	update := ChainReplayBlockUpdate{New: !wasObserved, Snapshot: t.snapshotLocked()}
	if conflict, ok := t.conflicts[preferredSlot]; ok {
		update.Conflict = true
		update.ConflictSlot = preferredSlot
		update.ConflictReason = conflict.reason
		return update
	}
	if len(t.conflicts) <= conflictsBefore {
		return update
	}
	for slot, conflict := range t.conflicts {
		if !update.Conflict || slot < update.ConflictSlot {
			update.Conflict = true
			update.ConflictSlot = slot
			update.ConflictReason = conflict.reason
		}
	}
	return update
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

	if _, invalid := t.invalidBlocks[block]; invalid && cert.Type != CertificateNotarizeFallback {
		t.recordConflictLocked(block.Slot, fmt.Sprintf("objectively invalid block %s has decisive %s certificate", block, cert.Type))
		return
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

func (t *ChainTracker) markDirectFinalizedLocked(block BlockID, certType CertificateType) {
	if block.IsZero() || !block.HasHash() {
		return
	}
	if _, invalid := t.invalidBlocks[block]; invalid {
		t.recordConflictLocked(block.Slot, fmt.Sprintf("objectively invalid block %s selected by %s finality", block, certType))
		return
	}
	if existing, ok := t.directFinalized[block]; ok && existing == certType {
		return
	}
	if existing, ok := t.finalizedBySlot[block.Slot]; ok && existing != block {
		t.recordConflictLocked(block.Slot, "multiple finalized block IDs for slot")
		return
	}
	for existing := range t.directFinalized {
		if existing.Slot == block.Slot && existing != block {
			t.recordConflictLocked(block.Slot, "multiple finalized block IDs for slot")
			return
		}
	}
	t.directFinalized[block] = certType
	t.finalizedBySlot[block.Slot] = block
	t.refreshConflictLocked(block.Slot)
	if _, conflicted := t.conflicts[block.Slot]; conflicted {
		return
	}
	if block.Slot >= t.latestDirectFinalizedBlock.Slot {
		t.latestDirectFinalizedBlock = block
	}
	t.markChainFinalizedAncestorsLocked(block, certType)
}

// markChainFinalizedAncestorsLocked walks parent links from a finalized block,
// marking ancestors finalized-by-ancestry so their slots resolve to block decisions
// even when they only carry fallback certs. The walk stops at unobserved parents or
// ambiguity (several certified blocks at the parent slot with no exact match).
func (t *ChainTracker) markChainFinalizedAncestorsLocked(block BlockID, certType CertificateType) {
	for {
		if t.prunedBeforeSlot != 0 && block.Slot <= t.prunedBeforeSlot {
			return
		}
		if _, invalid := t.invalidBlocks[block]; invalid {
			t.recordConflictLocked(block.Slot, fmt.Sprintf("objectively invalid block %s selected by finalized ancestry", block))
			return
		}
		state := t.blocks[block]
		if state == nil || !state.observed || state.parentSlot == 0 || state.parentSlot >= block.Slot {
			return
		}
		// Every omitted slot on every parent edge of a finalized chain is
		// resolved as skipped, not only the gap directly below the tip.
		t.deriveIndirectSkipsLocked(block, certType)
		if t.prunedBeforeSlot != 0 && state.parentSlot < t.prunedBeforeSlot {
			t.recordConflictLocked(t.prunedBeforeSlot, "finalized ancestry crosses the durable root")
			return
		}
		parent := BlockID{Slot: state.parentSlot, Hash: state.parentHash}
		if state.parentHash.IsZero() {
			// No parent hash at all. Fall back to the parent slot's single
			// certified block ONLY if it carries a unique-strength cert
			// (notarize/fast-finalize/genesis — provably the slot's one block,
			// Lemmas 21(i)/24). A fallback-only cert could be an equivocation twin.
			slotBlocks := t.blockSlots[state.parentSlot]
			var validParents int
			for _, id := range slotBlocks {
				if _, invalid := t.invalidBlocks[id]; invalid {
					continue
				}
				parent = id
				validParents++
			}
			if validParents != 1 {
				return
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
		} else if _, invalid := t.invalidBlocks[parent]; invalid {
			t.recordConflictLocked(parent.Slot, fmt.Sprintf("finalized block %s names objectively invalid parent %s", block, parent))
			return
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
		if existing, ok := t.finalizedBySlot[parent.Slot]; ok && existing != parent {
			t.recordConflictLocked(parent.Slot, "multiple finalized block IDs for slot")
			return
		}
		t.chainFinalized[parent] = certType
		t.finalizedBySlot[parent.Slot] = parent
		// The exact parent link selects this ancestor, but it does not turn the
		// ancestor into a directly-finalized slot. Agave can retain a block as
		// finalized ancestry even when that slot also has a skip certificate (or
		// other pre-finality certificates); only a direct finalization certificate
		// carries the per-slot exclusivity checked by refreshConflictLocked.
		block = parent
	}
}

// maxCertifiedBlocksPerSlot bounds notar-fallback-or-stronger certified blocks per
// slot. It follows from the whitepaper's per-validator vote budget (Def. 12: one
// notarize plus at most three notar-fallback votes = four block entries per voter;
// with 60% required per cert, at most 4/0.6 ≈ 6.7 → 7 distinct blocks can ever be
// certified). More is protocol-impossible — cryptographic evidence of an attack.
const maxCertifiedBlocksPerSlot = 7

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
	// The per-slot finalized index sees what the certified-blocks scan below
	// cannot: an ancestry-finalized parent with no certificate of its own
	// (its stub, minted from the finalized child's exact parent-hash
	// binding, never enters blockSlots). Same reasoning as CertifiedBlockAt.
	// The scan still runs as the Byzantine defense — a SECOND finalized
	// block at the slot must yield ok=false, never a silent pick.
	var found BlockID
	matches := 0
	if fin, ok := t.finalizedBySlot[slot]; ok {
		if _, invalid := t.invalidBlocks[fin]; invalid {
			return BlockID{}, false
		}
		found = fin
		matches = 1
	}
	for _, id := range t.blockSlots[slot] {
		if id != found && t.finalizedLocked(id) {
			found = id
			matches++
		}
	}
	if matches != 1 {
		return BlockID{}, false
	}
	return found, true
}

// FinalizedSkipAt returns the finalized descendant whose exact parent edge
// omits slot. Unlike SkipCertifiedAt, this deliberately excludes a standalone
// skip certificate: only finalized ancestry proves that an executed block at
// the omitted slot is off the selected chain. A finalized block selected at
// slot itself remains authoritative over any same-slot skip evidence.
func (t *ChainTracker) FinalizedSkipAt(slot uint64) (BlockID, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, conflicted := t.conflicts[slot]; conflicted {
		return BlockID{}, false
	}
	if selected, ok := t.finalizedBySlot[slot]; ok && !t.isObjectivelyInvalidBlockLocked(selected) {
		return BlockID{}, false
	}
	indirect, ok := t.indirectSkips[slot]
	if !ok || indirect.viaFinalized.IsZero() || t.isObjectivelyInvalidBlockLocked(indirect.viaFinalized) {
		return BlockID{}, false
	}
	return indirect.viaFinalized, true
}

// FinalityConflictAt reports whether the slot carries a recorded safety violation.
func (t *ChainTracker) FinalityConflictAt(slot uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.conflicts[slot]
	return ok
}

func (t *ChainTracker) FinalityConflictReasonAt(slot uint64) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	conflict, ok := t.conflicts[slot]
	return conflict.reason, ok
}

// CertifiedBlockAt returns the slot's DECISIVELY certified block: one backed
// by a unique-strength certificate (notarize / finalize-fast / genesis — at
// most one per slot by protocol, Lemma 21(i)/24) or finalized directly or by
// ancestry. Fallback-only candidates are ambiguous (up to 7 can legally
// coexist) and never returned. This is the execute-on-receipt switch signal:
// an executed block contradicting the decisive block must be unwound.
func (t *ChainTracker) CertifiedBlockAt(slot uint64) (BlockID, CertificateType, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, conflicted := t.conflicts[slot]; conflicted {
		return BlockID{}, "", false
	}
	// A finalized block (direct or by ancestry) is decisive even when it never
	// received a certificate of its own — the cert-less ancestry-finalized
	// parent case, which the cert-only blockSlots scan below cannot see. Exact
	// finalized ancestry also outranks other non-finalizing certificates at this
	// slot; Agave roots the finalized bank and its exact BankForks ancestors. A
	// different directly-finalized block would already have recorded a conflict.
	if fin, ok := t.finalizedBySlot[slot]; ok {
		if _, invalid := t.invalidBlocks[fin]; invalid {
			return BlockID{}, "", false
		}
		var certType CertificateType
		if state := t.blocks[fin]; state != nil {
			certType = strongestBlockCertificateType(state.certificates)
		}
		return fin, certType, true
	}
	var winner BlockID
	var winnerType CertificateType
	found := false
	// Iterate the slot's blocks directly (no candidate-slice allocation): the
	// switch sweep calls this for every executed-unfolded slot whenever the
	// tracker's decision version advances — which on a healthy cluster is
	// nearly every block.
	for _, block := range t.blockSlots[slot] {
		if _, invalid := t.invalidBlocks[block]; invalid {
			continue
		}
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

// SkipCertifiedAt reports whether replay should treat the slot as skipped,
// explicitly (skip cert) or indirectly (omitted between finalized ancestors).
// A block selected as exact finalized ancestry overrides a same-slot skip: the
// skip made omission legal, but did not invalidate the block as a future parent.
func (t *ChainTracker) SkipCertifiedAt(slot uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if _, conflicted := t.conflicts[slot]; conflicted {
		return false
	}
	if selected, ok := t.finalizedBySlot[slot]; ok && !t.isObjectivelyInvalidBlockLocked(selected) {
		return false
	}
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
// the block is selected by finality; ancestry-selected blocks can legally
// coexist with a skip because the skip does not invalidate them as parents.
// The scan is bounded by the tracker's
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
		if _, conflicted := t.conflicts[slot]; conflicted {
			continue
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
			if state := t.blocks[fin]; state != nil && !state.observed && !t.isObjectivelyInvalidBlockLocked(fin) {
				w := WantedBlock{Block: fin, Strongest: strongestBlockCertificateType(state.certificates), Finalized: true}
				best, bestPri = &w, wantedPriority(w.Strongest, true)
			}
		}
		for _, cand := range t.selectableBlockCandidatesLocked(slot) {
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

// PruneBeforeSlot drops tracker state strictly below slot. The durable
// AccountsDB promotion path is the normal caller: consensus finality alone must
// not discard speculative ancestry that has not actually been folded. Startup
// also uses it to restore the already-durable lower bound.
func (t *ChainTracker) PruneBeforeSlot(slot uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if slot <= t.prunedBeforeSlot {
		return
	}
	t.prunedBeforeSlot = slot
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
	for id := range t.invalidBlocks {
		if id.Slot < slot {
			delete(t.invalidBlocks, id)
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
	if t.isObjectivelyInvalidBlockLocked(block) {
		return
	}
	for slot := state.parentSlot + 1; slot < block.Slot; slot++ {
		if slot < t.prunedBeforeSlot {
			continue
		}
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
	// Direct finality or exact finalized ancestry decides the block even if an
	// explicit skip also exists at an ancestor slot. Agave roots the finalized
	// bank's exact BankForks ancestry; its parent-ready tracker permits that
	// ancestry block and a skip certificate to coexist.
	if block, ok := t.finalizedBySlot[slot]; ok {
		if reason, invalid := t.invalidBlocks[block]; invalid {
			return ChainDecision{
				Slot:   slot,
				Kind:   ChainDecisionKindConflict,
				Reason: fmt.Sprintf("finality selected objectively invalid block %s: %s", block, reason),
			}, true
		}
		decision := ChainDecision{
			Slot:   slot,
			Kind:   ChainDecisionKindBlock,
			Block:  block,
			Reason: "selected by finality",
		}
		if state := t.blocks[block]; state != nil {
			decision.Observed = state.observed
			decision.ParentSlot = state.parentSlot
			decision.ParentHash = state.parentHash
			decision.CertificateType = strongestBlockCertificateType(state.certificates)
		}
		return decision, true
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
	candidates := t.selectableBlockCandidatesLocked(slot)
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
	if t.isObjectivelyInvalidBlockLocked(block) {
		return false
	}
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

// selectableBlockCandidatesLocked is the execution/repair view of certified
// candidates. blockCandidatesLocked deliberately retains objectively invalid
// identities because their verified certificates remain safety evidence and
// count toward the protocol's per-slot certificate bound.
func (t *ChainTracker) selectableBlockCandidatesLocked(slot uint64) []ChainBlockCandidate {
	candidates := t.blockCandidatesLocked(slot)
	selectable := candidates[:0]
	for _, candidate := range candidates {
		if t.isObjectivelyInvalidBlockLocked(candidate.Block) {
			continue
		}
		selectable = append(selectable, candidate)
	}
	return selectable
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
// with each other pre-finality. Only DIRECT finalization carries certificate
// exclusivity at its slot; a block selected indirectly as finalized ancestry can
// legally coexist with a same-slot skip and other pre-finality certificates.
func (t *ChainTracker) refreshConflictLocked(slot uint64) {
	candidates := t.blockCandidatesLocked(slot)
	_, hasExplicitSkip := t.skipCerts[slot]
	_, hasIndirectSkip := t.indirectSkips[slot]
	hasSkip := hasExplicitSkip || hasIndirectSkip

	// Unique-strength certs are bounded to one per slot (Lemmas 21(i)/24).
	// Directly-finalized blocks carry the exclusivity used below; exact ancestors
	// selected by a later finalized block do not.
	var unique, directFinalizedCount int
	for _, c := range candidates {
		switch c.CertificateType {
		case CertificateFinalizeFast, CertificateNotarize, CertificateGenesis:
			unique++
		}
		if _, directlyFinalized := t.directFinalized[c.Block]; directlyFinalized {
			directFinalizedCount++
		}
	}

	switch {
	case len(candidates) > maxCertifiedBlocksPerSlot:
		// See maxCertifiedBlocksPerSlot: beyond the vote-budget bound is
		// protocol-impossible — cryptographic evidence of an attack.
		t.recordConflictLocked(slot, "certified blocks exceed the per-slot protocol bound")
	case unique > 1: // two notarized (or fast-finalized) blocks in one slot
		t.recordConflictLocked(slot, "multiple notarized blocks for slot")
	case directFinalizedCount > 0 && hasSkip: // directly finalized block contradicted by a skip (Lemmas 21(iii)/26(iii))
		t.recordConflictLocked(slot, "finalized block contradicted by skip")
	case directFinalizedCount > 0 && len(candidates) > 1: // directly finalized block plus another certified block (Lemmas 21(i,ii)/26(i,ii))
		t.recordConflictLocked(slot, "finalized block plus competing certified block")
	}
	// No delete arm: recorded violations are write-once Byzantine evidence. The
	// conditions are monotone while a slot's state is retained, and after pruning a
	// single re-observed cert would otherwise rebuild the slot as conflict-free and
	// erase the flag right before the promotion gate consults it.
}

func (t *ChainTracker) recordConflictLocked(slot uint64, reason string) {
	if _, exists := t.conflicts[slot]; exists {
		return
	}
	t.conflicts[slot] = chainConflict{
		slot:       slot,
		reason:     reason,
		candidates: t.blockCandidatesLocked(slot),
	}
	t.bumpDecisionLocked()
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
