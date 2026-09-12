package alpenglow

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/gagliardetto/solana-go"
)

// Agave v4.3's bls-sigverify accepts Votor votes and certificates at most
// 30,000 slots ahead of root. Peer admission uses the same horizon to open the
// upcoming epoch's validator set before the boundary.
const AgaveVoteVerificationWindow = uint64(30_000)

type ConsensusEventKind string

const (
	ConsensusEventBlockNotarized ConsensusEventKind = "block-notarized"
	ConsensusEventParentReady    ConsensusEventKind = "parent-ready"
	ConsensusEventFinalized      ConsensusEventKind = "finalized"
	ConsensusEventSafeToNotar    ConsensusEventKind = "safe-to-notar"
	ConsensusEventSafeToSkip     ConsensusEventKind = "safe-to-skip"
	ConsensusEventConflict       ConsensusEventKind = "conflict"
)

type ConsensusEvent struct {
	Kind            ConsensusEventKind
	Slot            uint64
	Block           BlockID
	Fast            bool
	CertificateType CertificateType
	Reason          string
}

type ConsensusPoolConfig struct {
	RootBlock            BlockID
	MaxSlotsAhead        uint64
	MaxTrackedSlots      int
	MaxVerifiedVotes     int
	MaxEquivocations     int
	FinalizedRetainSlots uint64
}

func DefaultConsensusPoolConfig() ConsensusPoolConfig {
	return ConsensusPoolConfig{
		MaxSlotsAhead:        AgaveVoteVerificationWindow,
		MaxTrackedSlots:      int(AgaveVoteVerificationWindow),
		MaxVerifiedVotes:     262_144,
		MaxEquivocations:     4_096,
		FinalizedRetainSlots: 16,
	}
}

type VerifiedVote struct {
	Message VoteMessage
	Result  VoteVerifyResult
	Local   bool
}

// VoteAdmissionDisposition is the pool's atomic verdict for a verified vote.
// Callers must use this value instead of probing pool state after
// AddVerifiedVote returns: finality pruning can otherwise race that second
// lookup and make a successfully classified local vote appear to have been
// rejected for an unrelated reason.
type VoteAdmissionDisposition string

const (
	VoteAdmissionAccepted            VoteAdmissionDisposition = "accepted"
	VoteAdmissionExactDuplicate      VoteAdmissionDisposition = "exact-duplicate"
	VoteAdmissionRooted              VoteAdmissionDisposition = "rooted"
	VoteAdmissionTooFarAhead         VoteAdmissionDisposition = "too-far-ahead"
	VoteAdmissionTrackedSlotCapacity VoteAdmissionDisposition = "tracked-slot-capacity"
	VoteAdmissionVoteCapacity        VoteAdmissionDisposition = "vote-capacity"
	VoteAdmissionConflict            VoteAdmissionDisposition = "conflict"
)

type ConsensusUpdate struct {
	Certificates []Certificate
	Events       []ConsensusEvent
	VoteAccepted bool
	// VoteAdmission is set by AddVerifiedVote for every non-error outcome.
	// VoteAccepted remains true only when this call inserted a new vote; an
	// exact duplicate is classified explicitly but keeps VoteAccepted false.
	VoteAdmission VoteAdmissionDisposition
}

type VoteEvidence struct {
	Slot            uint64
	Rank            uint16
	VoteType        VoteType
	Conflicting     VoteType
	FirstBlockHash  solana.Hash
	SecondBlockHash solana.Hash
}

type ConsensusPoolSnapshot struct {
	RootSlot         uint64
	LiveSlot         uint64
	TrackedSlots     int
	VerifiedVotes    uint64
	RejectedVotes    uint64
	DuplicateVotes   uint64
	ConflictingVotes uint64
	Certificates     int
	FinalizedBlocks  int
	Equivocations    int
	ConflictingSlots int
}

type consensusTallyKey struct {
	voteType VoteType
	block    solana.Hash
}

type verifiedVoteRecord struct {
	message VoteMessage
	stake   uint64
}

type consensusTally struct {
	votes map[uint16]verifiedVoteRecord
	stake uint64
}

type slotConsensusState struct {
	epoch          uint64
	totalStake     uint64
	tallies        map[consensusTallyKey]*consensusTally
	byRank         map[uint16]map[VoteType][]solana.Hash
	localFirstVote *Vote
	localFirstRank uint16
	safeNotarSent  map[solana.Hash]struct{}
	safeSkipSent   bool
}

type localFirstVoteProvenance struct {
	Vote Vote
	Rank uint16
}

// ConsensusPool is the verified-message Alpenglow bean counter. It mirrors
// Agave's certificate thresholds, conflicting vote rules, ParentReady state,
// and slow/fast finalization event ordering. Raw network messages must never
// be passed here.
type ConsensusPool struct {
	mu sync.Mutex

	cfg       ConsensusPoolConfig
	root      BlockID
	liveSlot  uint64
	slots     map[uint64]*slotConsensusState
	completed map[CertificateKey]Certificate
	// finalized records the strongest certificate observed for each block.
	// A bool loses the slow->fast upgrade Agave emits to downstream consumers.
	finalized     map[BlockID]CertificateType
	parentReady   *ParentReadyTracker
	invalidBlocks map[BlockID]struct{}
	// localFirstVotes preserves the validator's durable round-one choice across
	// restart without pretending that an execution-unproven block vote was
	// admitted to the verified tally. A later vote for the slot applies this
	// provenance before evaluating SafeToNotar/SafeToSkip.
	localFirstVotes map[uint64]localFirstVoteProvenance
	evidence        []VoteEvidence
	conflicts       map[uint64]string
	// Intrawindow SafeToNotar events require parent certification and block
	// availability checks in the consensus service. Keep them pending here,
	// matching Agave, instead of issuing an unsafe immediate event.
	pendingSafeToNotar []BlockID

	verifiedVotes    int
	verifiedTotal    uint64
	rejectedVotes    uint64
	duplicateVotes   uint64
	conflictingVotes uint64
}

func NewConsensusPool(cfg ConsensusPoolConfig) *ConsensusPool {
	defaults := DefaultConsensusPoolConfig()
	if cfg.MaxSlotsAhead == 0 {
		cfg.MaxSlotsAhead = defaults.MaxSlotsAhead
	}
	if cfg.MaxTrackedSlots <= 0 {
		cfg.MaxTrackedSlots = defaults.MaxTrackedSlots
	}
	if cfg.MaxVerifiedVotes <= 0 {
		cfg.MaxVerifiedVotes = defaults.MaxVerifiedVotes
	}
	if cfg.MaxEquivocations <= 0 {
		cfg.MaxEquivocations = defaults.MaxEquivocations
	}
	if cfg.FinalizedRetainSlots == 0 {
		cfg.FinalizedRetainSlots = defaults.FinalizedRetainSlots
	}
	return &ConsensusPool{
		cfg:             cfg,
		root:            cfg.RootBlock,
		slots:           make(map[uint64]*slotConsensusState),
		completed:       make(map[CertificateKey]Certificate),
		finalized:       make(map[BlockID]CertificateType),
		parentReady:     NewParentReadyTracker(cfg.RootBlock),
		invalidBlocks:   make(map[BlockID]struct{}),
		localFirstVotes: make(map[uint64]localFirstVoteProvenance),
		conflicts:       make(map[uint64]string),
	}
}

func (p *ConsensusPool) NoteLiveSlot(slot uint64) {
	p.mu.Lock()
	if slot > p.liveSlot {
		p.liveSlot = slot
	}
	p.mu.Unlock()
}

func (p *ConsensusPool) SetRoot(root BlockID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setRootLocked(root)
}

func (p *ConsensusPool) setRootLocked(root BlockID) {
	if root.Slot < p.root.Slot {
		return
	}
	if root.Slot == p.root.Slot {
		if root.HasHash() {
			p.root = root
		}
		return
	}
	p.root = root
	p.parentReady.SetRoot(root.Slot)
	for slot, state := range p.slots {
		if slot <= root.Slot {
			p.verifiedVotes -= countSlotVotes(state)
			delete(p.slots, slot)
		}
	}
	for slot := range p.localFirstVotes {
		if slot <= root.Slot {
			delete(p.localFirstVotes, slot)
		}
	}
	if p.verifiedVotes < 0 {
		p.verifiedVotes = 0
	}
	for key := range p.completed {
		if key.Slot < root.Slot {
			delete(p.completed, key)
		}
	}
	for block := range p.finalized {
		if block.Slot < root.Slot {
			delete(p.finalized, block)
		}
	}
	for slot := range p.conflicts {
		if slot < root.Slot {
			delete(p.conflicts, slot)
		}
	}
	for block := range p.invalidBlocks {
		if block.Slot < root.Slot {
			delete(p.invalidBlocks, block)
		}
	}
	p.pendingSafeToNotar = filterBlocksAbove(p.pendingSafeToNotar, root.Slot)
}

// PruneBehindFinality bounds vote/certificate accounting independently of the
// slower durable AccountsDB watermark. A short history remains so slot+8 footer
// reward construction can flush late, lazily verified votes through this same
// accepted-vote path before they age out.
func (p *ConsensusPool) PruneBehindFinality(finalizedSlot uint64) {
	if finalizedSlot <= p.cfg.FinalizedRetainSlots {
		return
	}
	p.mu.Lock()
	p.setRootLocked(BlockID{Slot: finalizedSlot - p.cfg.FinalizedRetainSlots})
	p.mu.Unlock()
}

func (p *ConsensusPool) RestoreParentReady(slot uint64, parent BlockID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if parent.Slot >= slot {
		return false
	}
	if slot <= p.parentReady.highestWithParentReady {
		return false
	}
	if _, invalid := p.invalidBlocks[parent]; invalid {
		return false
	}
	// A persisted ParentReady edge is a liveness hint, never authority to move
	// the verified-vote admission root backward or replace its exact identity.
	if parent.Slot < p.root.Slot || parent.Slot == p.root.Slot && p.root.HasHash() && p.root.Hash != parent.Hash {
		return false
	}
	if !p.parentReady.Restore(slot, parent) {
		return false
	}
	// Use the normal root transition so vote capacity, first-vote provenance,
	// certificates, conflicts, and invalid tombstones are pruned together.
	p.setRootLocked(parent)
	return true
}

// InvalidateBlock tombstones an objectively invalid identity in every active
// parent/safe-action surface while retaining verified certificates and votes as
// safety evidence. Returned events re-publish a surviving preferred parent.
func (p *ConsensusPool) InvalidateBlock(block BlockID) []ConsensusEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.invalidBlocks == nil {
		p.invalidBlocks = make(map[BlockID]struct{})
	}
	if _, exists := p.invalidBlocks[block]; exists {
		return nil
	}
	p.invalidBlocks[block] = struct{}{}
	p.pendingSafeToNotar, _ = removeBlock(p.pendingSafeToNotar, block)
	return p.parentReady.InvalidateBlock(block)
}

func (p *ConsensusPool) AddVerifiedVote(v VerifiedVote) (ConsensusUpdate, error) {
	if err := v.Message.ValidateBasic(); err != nil {
		return ConsensusUpdate{}, err
	}
	if len(v.Message.Signature) != BLSSignatureSize {
		return ConsensusUpdate{}, fmt.Errorf("verified vote has signature length %d, want %d", len(v.Message.Signature), BLSSignatureSize)
	}
	if v.Result.Rank != v.Message.Rank || v.Result.Stake == 0 || v.Result.TotalStake == 0 {
		return ConsensusUpdate{}, fmt.Errorf("verified vote metadata does not match vote rank/stake")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	slot := v.Message.Vote.Slot
	if slot <= p.root.Slot {
		p.rejectedVotes++
		return ConsensusUpdate{VoteAdmission: VoteAdmissionRooted}, nil
	}
	anchor := p.liveSlot
	if p.root.Slot > anchor {
		anchor = p.root.Slot
	}
	if anchor != 0 && slot > anchor+p.cfg.MaxSlotsAhead {
		p.rejectedVotes++
		return ConsensusUpdate{VoteAdmission: VoteAdmissionTooFarAhead}, nil
	}
	if first, ok := p.localFirstVotes[slot]; ok {
		if v.Message.Rank == first.Rank && votesConflict(first.Vote, v.Message.Vote) {
			p.rejectedVotes++
			return ConsensusUpdate{}, fmt.Errorf("slot %d verified vote conflicts with durable local first vote at rank %d", slot, first.Rank)
		}
		if v.Local && voteEstablishesLocalFirst(v.Message.Vote.Type) && (first.Vote != v.Message.Vote || first.Rank != v.Message.Rank) {
			p.rejectedVotes++
			return ConsensusUpdate{}, fmt.Errorf("slot %d local first-vote provenance conflicts: have %s rank %d, got %s rank %d", slot, first.Vote.Type, first.Rank, v.Message.Vote.Type, v.Message.Rank)
		}
	}

	state := p.slots[slot]
	if state == nil {
		if len(p.slots) >= p.cfg.MaxTrackedSlots {
			p.rejectedVotes++
			return ConsensusUpdate{VoteAdmission: VoteAdmissionTrackedSlotCapacity}, nil
		}
		if p.verifiedVotes >= p.cfg.MaxVerifiedVotes {
			p.rejectedVotes++
			return ConsensusUpdate{VoteAdmission: VoteAdmissionVoteCapacity}, nil
		}
		state = &slotConsensusState{
			epoch:         v.Result.Epoch,
			totalStake:    v.Result.TotalStake,
			tallies:       make(map[consensusTallyKey]*consensusTally),
			byRank:        make(map[uint16]map[VoteType][]solana.Hash),
			safeNotarSent: make(map[solana.Hash]struct{}),
		}
		if first, ok := p.localFirstVotes[slot]; ok {
			vote := first.Vote
			state.localFirstVote = &vote
			state.localFirstRank = first.Rank
		}
		p.slots[slot] = state
	}
	if state.epoch != v.Result.Epoch || state.totalStake != v.Result.TotalStake {
		p.rejectedVotes++
		return ConsensusUpdate{}, fmt.Errorf("slot %d validator-set mismatch: epoch/total %d/%d, got %d/%d", slot, state.epoch, state.totalStake, v.Result.Epoch, v.Result.TotalStake)
	}
	if v.Local && voteEstablishesLocalFirst(v.Message.Vote.Type) && state.localFirstVote != nil && (*state.localFirstVote != v.Message.Vote || state.localFirstRank != v.Message.Rank) {
		p.rejectedVotes++
		return ConsensusUpdate{}, fmt.Errorf("slot %d local first-vote provenance conflicts: have %s rank %d, got %s rank %d", slot, state.localFirstVote.Type, state.localFirstRank, v.Message.Vote.Type, v.Message.Rank)
	}
	if hasExactVoteLocked(state, v.Message) {
		p.duplicateVotes++
		return p.exactDuplicateUpdateLocked(slot, state, v), nil
	}
	if evidence, conflict := conflictingVoteEvidence(state.byRank[v.Message.Rank], v.Message); conflict {
		p.recordEvidenceLocked(evidence)
		p.rejectedVotes++
		return ConsensusUpdate{VoteAdmission: VoteAdmissionConflict}, nil
	}
	if p.verifiedVotes >= p.cfg.MaxVerifiedVotes {
		p.rejectedVotes++
		return ConsensusUpdate{VoteAdmission: VoteAdmissionVoteCapacity}, nil
	}

	accepted, duplicate, err := p.acceptVoteLocked(state, v)
	if err != nil {
		p.rejectedVotes++
		return ConsensusUpdate{}, err
	}
	if duplicate {
		p.duplicateVotes++
		return p.exactDuplicateUpdateLocked(slot, state, v), nil
	}
	if !accepted {
		p.rejectedVotes++
		return ConsensusUpdate{VoteAdmission: VoteAdmissionConflict}, nil
	}

	p.verifiedVotes++
	p.verifiedTotal++
	if v.Local && voteEstablishesLocalFirst(v.Message.Vote.Type) && state.localFirstVote == nil {
		vote := v.Message.Vote
		state.localFirstVote = &vote
		state.localFirstRank = v.Message.Rank
		p.localFirstVotes[slot] = localFirstVoteProvenance{Vote: vote, Rank: v.Message.Rank}
	}

	update := ConsensusUpdate{VoteAccepted: true, VoteAdmission: VoteAdmissionAccepted}
	update.Events = append(update.Events, p.safeEventsLocked(slot, state)...)
	certs, events, err := p.assembleCertificatesLocked(slot, state)
	if err != nil {
		return ConsensusUpdate{}, err
	}
	update.Certificates = append(update.Certificates, certs...)
	update.Events = append(update.Events, events...)
	return update, nil
}

func (p *ConsensusPool) exactDuplicateUpdateLocked(slot uint64, state *slotConsensusState, v VerifiedVote) ConsensusUpdate {
	update := ConsensusUpdate{VoteAdmission: VoteAdmissionExactDuplicate}
	if v.Local && voteEstablishesLocalFirst(v.Message.Vote.Type) && state.localFirstVote == nil {
		vote := v.Message.Vote
		state.localFirstVote = &vote
		state.localFirstRank = v.Message.Rank
		p.localFirstVotes[slot] = localFirstVoteProvenance{Vote: vote, Rank: v.Message.Rank}
		// The tally already existed as a network vote. Installing its local
		// provenance can make previously accumulated stake actionable without
		// incrementing stake, capacity, or VoteAccepted.
		update.Events = append(update.Events, p.safeEventsLocked(slot, state)...)
	}
	return update
}

// RestoreLocalFirstVote records a signed, durable round-one choice without
// adding it to the verified vote tally. This is deliberately provenance-only:
// restart restoration must retain the original Notarize/Skip decision even
// when its block has not replayed successfully in this process, but must not
// broadcast it or use it to assemble a certificate.
func (p *ConsensusPool) RestoreLocalFirstVote(vote Vote, rank uint16) (ConsensusUpdate, error) {
	if err := vote.ValidateBasic(); err != nil {
		return ConsensusUpdate{}, err
	}
	if !voteEstablishesLocalFirst(vote.Type) {
		return ConsensusUpdate{}, fmt.Errorf("%s vote cannot establish local first-vote provenance", vote.Type)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if vote.Slot <= p.root.Slot {
		return ConsensusUpdate{}, nil
	}
	if existing, ok := p.localFirstVotes[vote.Slot]; ok {
		if existing.Vote != vote || existing.Rank != rank {
			return ConsensusUpdate{}, fmt.Errorf("slot %d local first-vote provenance conflicts: have %s rank %d, got %s rank %d", vote.Slot, existing.Vote.Type, existing.Rank, vote.Type, rank)
		}
		return ConsensusUpdate{}, nil
	}
	state := p.slots[vote.Slot]
	if state != nil {
		if conflict, _, ok := conflictingRankVote(state.byRank[rank], vote); ok {
			return ConsensusUpdate{}, fmt.Errorf("slot %d durable local first vote conflicts with verified %s vote at rank %d", vote.Slot, conflict, rank)
		}
	}
	p.localFirstVotes[vote.Slot] = localFirstVoteProvenance{Vote: vote, Rank: rank}
	if state == nil {
		return ConsensusUpdate{}, nil
	}
	if state.localFirstVote != nil {
		if *state.localFirstVote != vote || state.localFirstRank != rank {
			delete(p.localFirstVotes, vote.Slot)
			return ConsensusUpdate{}, fmt.Errorf("slot %d local first-vote provenance conflicts: have %s rank %d, got %s rank %d", vote.Slot, state.localFirstVote.Type, state.localFirstRank, vote.Type, rank)
		}
		return ConsensusUpdate{}, nil
	}
	local := vote
	state.localFirstVote = &local
	state.localFirstRank = rank
	return ConsensusUpdate{Events: p.safeEventsLocked(vote.Slot, state)}, nil
}

func conflictingRankVote(rankVotes map[VoteType][]solana.Hash, vote Vote) (VoteType, solana.Hash, bool) {
	for _, hash := range rankVotes[vote.Type] {
		if hash != vote.BlockHash {
			return vote.Type, hash, true
		}
	}
	return conflictingVerifiedVote(rankVotes, vote)
}

func votesConflict(first, candidate Vote) bool {
	rankVotes := map[VoteType][]solana.Hash{first.Type: {first.BlockHash}}
	_, _, conflict := conflictingRankVote(rankVotes, candidate)
	return conflict
}

func voteEstablishesLocalFirst(voteType VoteType) bool {
	return voteType == VoteTypeNotarize || voteType == VoteTypeSkip
}

func hasExactVoteLocked(state *slotConsensusState, message VoteMessage) bool {
	for _, hash := range state.byRank[message.Rank][message.Vote.Type] {
		if hash == message.Vote.BlockHash {
			return true
		}
	}
	return false
}

// HasVerifiedVote reports whether the exact logical vote is currently retained
// in the verified pool. It is for diagnostics and tests only; action callers
// must use AddVerifiedVote's atomic VoteAdmission verdict because finality can
// prune this state immediately after admission.
func (p *ConsensusPool) HasVerifiedVote(message VoteMessage) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.slots[message.Vote.Slot]
	if state == nil {
		return false
	}
	for _, hash := range state.byRank[message.Rank][message.Vote.Type] {
		if hash == message.Vote.BlockHash {
			return true
		}
	}
	return false
}

func (p *ConsensusPool) AddVerifiedCertificate(cert Certificate) (ConsensusUpdate, error) {
	if !cert.SignatureVerified || !cert.StakeVerified {
		return ConsensusUpdate{}, fmt.Errorf("consensus pool requires signature and stake verified certificate")
	}
	if err := validateChainCertificate(cert); err != nil {
		return ConsensusUpdate{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cert.Slot < p.root.Slot {
		return ConsensusUpdate{}, nil
	}
	events, inserted, err := p.insertCertificateLocked(cert)
	if err != nil {
		return ConsensusUpdate{}, err
	}
	update := ConsensusUpdate{Events: events}
	if inserted {
		// Certificates is the pool's accepted-output stream. Downstream chain
		// rooting must consume this list rather than transport input directly.
		update.Certificates = []Certificate{cert}
	}
	return update, nil
}

func (p *ConsensusPool) BlockProductionParent(slot uint64) BlockProductionParent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.parentReady.BlockProductionParent(slot)
}

// HighestParentReady returns the highest slot and Agave-compatible preferred
// parent currently known to the pool.
func (p *ConsensusPool) HighestParentReady() (uint64, BlockID, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	slot := p.parentReady.highestWithParentReady
	parents := p.parentReady.Parents(slot)
	if len(parents) == 0 {
		return 0, BlockID{}, false
	}
	return slot, parents[0], true
}

// CertificatesAtOrAfter snapshots accepted certificates for standstill
// refresh. Returned signatures/bitmaps are deep-copied so broadcast never
// aliases pool state.
func (p *ConsensusPool) CertificatesAtOrAfter(slot uint64) []Certificate {
	p.mu.Lock()
	defer p.mu.Unlock()
	certs := make([]Certificate, 0, len(p.completed))
	for _, cert := range p.completed {
		if cert.Slot < slot {
			continue
		}
		cert.Signature = append([]byte(nil), cert.Signature...)
		cert.Bitmap = append([]byte(nil), cert.Bitmap...)
		certs = append(certs, cert)
	}
	sort.Slice(certs, func(i, j int) bool {
		if certs[i].Slot != certs[j].Slot {
			return certs[i].Slot < certs[j].Slot
		}
		return certificateAssemblyPriority(certs[i].Type) < certificateAssemblyPriority(certs[j].Type)
	})
	return certs
}

func (p *ConsensusPool) HasNotarFallbackOrStronger(block BlockID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.parentReady.HasNotarFallbackOrStronger(block)
}

// TakePendingSafeToNotar transfers intrawindow candidates to the consensus
// service, which must verify parent certification and local block availability
// before emitting SafeToNotar.
func (p *ConsensusPool) TakePendingSafeToNotar() []BlockID {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := make([]BlockID, 0, len(p.pendingSafeToNotar))
	for _, block := range p.pendingSafeToNotar {
		if _, invalid := p.invalidBlocks[block]; !invalid && block.Slot > p.root.Slot {
			pending = append(pending, block)
		}
	}
	p.pendingSafeToNotar = nil
	return pending
}

// RequeuePendingSafeToNotar returns a candidate that the consensus service
// could not yet resolve (for example, because its block or parent is missing).
// This mirrors Agave's pool/service handoff without letting the pool guess
// block ancestry from arrival order.
func (p *ConsensusPool) RequeuePendingSafeToNotar(block BlockID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, invalid := p.invalidBlocks[block]; invalid {
		return
	}
	if block.Slot <= p.root.Slot || containsBlock(p.pendingSafeToNotar, block) {
		return
	}
	p.pendingSafeToNotar = append(p.pendingSafeToNotar, block)
}

func (p *ConsensusPool) Snapshot() ConsensusPoolSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ConsensusPoolSnapshot{
		RootSlot:         p.root.Slot,
		LiveSlot:         p.liveSlot,
		TrackedSlots:     len(p.slots),
		VerifiedVotes:    p.verifiedTotal,
		RejectedVotes:    p.rejectedVotes,
		DuplicateVotes:   p.duplicateVotes,
		ConflictingVotes: p.conflictingVotes,
		Certificates:     len(p.completed),
		FinalizedBlocks:  len(p.finalized),
		Equivocations:    len(p.evidence),
		ConflictingSlots: len(p.conflicts),
	}
}

func (p *ConsensusPool) Evidence() []VoteEvidence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]VoteEvidence(nil), p.evidence...)
}

func (p *ConsensusPool) acceptVoteLocked(state *slotConsensusState, vote VerifiedVote) (accepted, duplicate bool, err error) {
	msg := vote.Message
	rankVotes := state.byRank[msg.Rank]
	if rankVotes == nil {
		rankVotes = make(map[VoteType][]solana.Hash)
		state.byRank[msg.Rank] = rankVotes
	}
	hash := msg.Vote.BlockHash
	for _, existing := range rankVotes[msg.Vote.Type] {
		if existing == hash {
			return false, true, nil
		}
	}

	if evidence, conflict := conflictingVoteEvidence(rankVotes, msg); conflict {
		p.recordEvidenceLocked(evidence)
		return false, false, nil
	}

	key := consensusTallyKey{voteType: msg.Vote.Type, block: hash}
	tally := state.tallies[key]
	if tally == nil {
		tally = &consensusTally{votes: make(map[uint16]verifiedVoteRecord)}
		state.tallies[key] = tally
	}
	tally.votes[msg.Rank] = verifiedVoteRecord{message: msg, stake: vote.Result.Stake}
	tally.stake += vote.Result.Stake
	rankVotes[msg.Vote.Type] = append(rankVotes[msg.Vote.Type], hash)
	return true, false, nil
}

func conflictingVoteEvidence(rankVotes map[VoteType][]solana.Hash, msg VoteMessage) (VoteEvidence, bool) {
	hashes := rankVotes[msg.Vote.Type]
	budget := 1
	if msg.Vote.Type == VoteTypeNotarizeFallback {
		budget = 3
	}
	if len(hashes) >= budget {
		return VoteEvidence{
			Slot: msg.Vote.Slot, Rank: msg.Rank, VoteType: msg.Vote.Type, Conflicting: msg.Vote.Type,
			FirstBlockHash: hashes[0], SecondBlockHash: msg.Vote.BlockHash,
		}, true
	}
	if conflict, first, ok := conflictingVerifiedVote(rankVotes, msg.Vote); ok {
		return VoteEvidence{
			Slot: msg.Vote.Slot, Rank: msg.Rank, VoteType: msg.Vote.Type, Conflicting: conflict,
			FirstBlockHash: first, SecondBlockHash: msg.Vote.BlockHash,
		}, true
	}
	return VoteEvidence{}, false
}

func conflictingVerifiedVote(rankVotes map[VoteType][]solana.Hash, vote Vote) (VoteType, solana.Hash, bool) {
	for _, conflict := range conflictingVoteTypes(vote.Type) {
		hashes := rankVotes[conflict]
		if len(hashes) == 0 {
			continue
		}
		if voteHasBlock(vote.Type) && duplicateBlockVoteType(conflict) {
			for _, hash := range hashes {
				if hash == vote.BlockHash {
					return conflict, hash, true
				}
			}
			continue
		}
		return conflict, hashes[0], true
	}
	return "", solana.Hash{}, false
}

func conflictingVoteTypes(voteType VoteType) []VoteType {
	switch voteType {
	case VoteTypeFinalize:
		return []VoteType{VoteTypeNotarizeFallback, VoteTypeSkip, VoteTypeSkipFallback, VoteTypeGenesis}
	case VoteTypeNotarize:
		return []VoteType{VoteTypeSkip, VoteTypeNotarizeFallback, VoteTypeGenesis}
	case VoteTypeNotarizeFallback:
		return []VoteType{VoteTypeFinalize, VoteTypeNotarize, VoteTypeGenesis}
	case VoteTypeSkip:
		return []VoteType{VoteTypeFinalize, VoteTypeNotarize, VoteTypeSkipFallback, VoteTypeGenesis}
	case VoteTypeSkipFallback:
		return []VoteType{VoteTypeSkip, VoteTypeFinalize, VoteTypeGenesis}
	case VoteTypeGenesis:
		return []VoteType{VoteTypeFinalize, VoteTypeNotarize, VoteTypeNotarizeFallback, VoteTypeSkip, VoteTypeSkipFallback}
	default:
		return nil
	}
}

func duplicateBlockVoteType(voteType VoteType) bool {
	return voteType == VoteTypeNotarize || voteType == VoteTypeNotarizeFallback
}

func voteHasBlock(voteType VoteType) bool {
	return voteType == VoteTypeNotarize || voteType == VoteTypeNotarizeFallback || voteType == VoteTypeGenesis
}

func (p *ConsensusPool) recordEvidenceLocked(evidence VoteEvidence) {
	p.conflictingVotes++
	if len(p.evidence) >= p.cfg.MaxEquivocations {
		copy(p.evidence, p.evidence[1:])
		p.evidence = p.evidence[:len(p.evidence)-1]
	}
	p.evidence = append(p.evidence, evidence)
}

func (p *ConsensusPool) safeEventsLocked(slot uint64, state *slotConsensusState) []ConsensusEvent {
	if state.localFirstVote == nil {
		return nil
	}
	var events []ConsensusEvent
	skipStake := tallyStake(state, VoteTypeSkip, solana.Hash{})
	var notarTotal, topNotar uint64
	for key, tally := range state.tallies {
		if key.voteType != VoteTypeNotarize {
			continue
		}
		notarTotal += tally.stake
		if tally.stake > topNotar {
			topNotar = tally.stake
		}
		block := BlockID{Slot: slot, Hash: key.block}
		if _, invalid := p.invalidBlocks[block]; invalid {
			continue
		}
		if _, sent := state.safeNotarSent[key.block]; sent {
			continue
		}
		if state.localFirstVote.Type == VoteTypeNotarize && state.localFirstVote.BlockHash == key.block {
			continue
		}
		if fractionMeets(40, tally.stake, state.totalStake) ||
			(fractionMeets(20, tally.stake, state.totalStake) && fractionMeets(60, tally.stake+skipStake, state.totalStake)) {
			state.safeNotarSent[key.block] = struct{}{}
			if slot == 1 || isLeaderWindowStart(slot) {
				events = append(events, ConsensusEvent{Kind: ConsensusEventSafeToNotar, Slot: slot, Block: block})
			} else if !containsBlock(p.pendingSafeToNotar, block) {
				p.pendingSafeToNotar = append(p.pendingSafeToNotar, block)
			}
		}
	}
	if !state.safeSkipSent && state.localFirstVote.Type == VoteTypeNotarize {
		stake := skipStake + notarTotal - topNotar
		if fractionMeets(40, stake, state.totalStake) {
			state.safeSkipSent = true
			events = append(events, ConsensusEvent{Kind: ConsensusEventSafeToSkip, Slot: slot})
		}
	}
	return events
}

type certificateTarget struct {
	certType CertificateType
	base     consensusTallyKey
	fallback *consensusTallyKey
}

func (p *ConsensusPool) assembleCertificatesLocked(slot uint64, state *slotConsensusState) ([]Certificate, []ConsensusEvent, error) {
	targets := make(map[CertificateKey]certificateTarget)
	add := func(target certificateTarget) {
		key := CertificateKey{Type: target.certType, Slot: slot}
		if target.certType.HasBlock() {
			key.BlockHash = target.base.block
		}
		targets[key] = target
	}
	for key := range state.tallies {
		switch key.voteType {
		case VoteTypeNotarize:
			add(certificateTarget{certType: CertificateNotarize, base: key})
			add(certificateTarget{certType: CertificateFinalizeFast, base: key})
			fallback := consensusTallyKey{voteType: VoteTypeNotarizeFallback, block: key.block}
			add(certificateTarget{certType: CertificateNotarizeFallback, base: key, fallback: &fallback})
		case VoteTypeNotarizeFallback:
			base := consensusTallyKey{voteType: VoteTypeNotarize, block: key.block}
			fallback := key
			add(certificateTarget{certType: CertificateNotarizeFallback, base: base, fallback: &fallback})
		case VoteTypeFinalize:
			add(certificateTarget{certType: CertificateFinalize, base: key})
		case VoteTypeSkip:
			fallback := consensusTallyKey{voteType: VoteTypeSkipFallback}
			add(certificateTarget{certType: CertificateSkip, base: key, fallback: &fallback})
		case VoteTypeSkipFallback:
			base := consensusTallyKey{voteType: VoteTypeSkip}
			fallback := key
			add(certificateTarget{certType: CertificateSkip, base: base, fallback: &fallback})
		case VoteTypeGenesis:
			add(certificateTarget{certType: CertificateGenesis, base: key})
		}
	}

	orderedKeys := make([]CertificateKey, 0, len(targets))
	for key := range targets {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Slice(orderedKeys, func(i, j int) bool {
		pi, pj := certificateAssemblyPriority(orderedKeys[i].Type), certificateAssemblyPriority(orderedKeys[j].Type)
		if pi != pj {
			return pi < pj
		}
		return bytes.Compare(orderedKeys[i].BlockHash[:], orderedKeys[j].BlockHash[:]) < 0
	})

	var certificates []Certificate
	var events []ConsensusEvent
	for _, key := range orderedKeys {
		if key.Type.HasBlock() {
			block := BlockID{Slot: key.Slot, Hash: key.BlockHash}
			if _, invalid := p.invalidBlocks[block]; invalid {
				continue
			}
		}
		target := targets[key]
		if _, exists := p.completed[key]; exists {
			continue
		}
		base := state.tallies[target.base]
		var fallback *consensusTally
		if target.fallback != nil {
			fallback = state.tallies[*target.fallback]
		}
		stake := uint64(0)
		if base != nil {
			stake += base.stake
		}
		if fallback != nil {
			stake += fallback.stake
		}
		threshold := target.certType.RequiredThreshold()
		met, err := threshold.Meets(stake, state.totalStake)
		if err != nil || !met {
			continue
		}
		cert, err := buildVerifiedCertificate(slot, target, base, fallback, state.totalStake)
		if err != nil {
			return nil, nil, err
		}
		certEvents, inserted, err := p.insertCertificateLocked(cert)
		if err != nil {
			return nil, nil, err
		}
		if inserted {
			certificates = append(certificates, cert)
			events = append(events, certEvents...)
		}
	}
	return certificates, events, nil
}

func (p *ConsensusPool) insertCertificateLocked(cert Certificate) ([]ConsensusEvent, bool, error) {
	key := cert.Key()
	if _, exists := p.completed[key]; exists {
		return nil, false, nil
	}
	p.completed[key] = cert
	inserted := false
	defer func() {
		if !inserted {
			delete(p.completed, key)
		}
	}()
	var events []ConsensusEvent
	switch cert.Type {
	case CertificateNotarize:
		block, _ := cert.Block()
		if _, invalid := p.invalidBlocks[block]; !invalid {
			events = append(events, ConsensusEvent{Kind: ConsensusEventBlockNotarized, Slot: cert.Slot, Block: block, CertificateType: cert.Type})
			parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
			if err != nil {
				return nil, false, err
			}
			events = append(events, parentEvents...)
		}
		finalized, err := p.maybeSlowFinalizeLocked(cert.Slot)
		if err != nil {
			return nil, false, err
		}
		events = append(events, finalized...)
	case CertificateNotarizeFallback:
		block, _ := cert.Block()
		if _, invalid := p.invalidBlocks[block]; !invalid {
			parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
			if err != nil {
				return nil, false, err
			}
			events = append(events, parentEvents...)
		}
	case CertificateFinalize:
		finalized, err := p.maybeSlowFinalizeLocked(cert.Slot)
		if err != nil {
			return nil, false, err
		}
		events = append(events, finalized...)
	case CertificateFinalizeFast:
		block, _ := cert.Block()
		if _, invalid := p.invalidBlocks[block]; !invalid {
			parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
			if err != nil {
				return nil, false, err
			}
			events = append(events, parentEvents...)
			if _, conflicted := p.conflicts[cert.Slot]; !conflicted && p.finalized[block] != CertificateFinalizeFast {
				p.finalized[block] = CertificateFinalizeFast
				events = append(events, ConsensusEvent{Kind: ConsensusEventFinalized, Slot: cert.Slot, Block: block, Fast: true, CertificateType: cert.Type})
			}
		}
	case CertificateSkip:
		events = append(events, p.parentReady.AddSkip(cert.Slot)...)
	case CertificateGenesis:
		block, _ := cert.Block()
		if _, invalid := p.invalidBlocks[block]; !invalid {
			parentEvents, err := p.parentReady.AddGenesis(block)
			if err != nil {
				return nil, false, err
			}
			events = append(events, parentEvents...)
		}
	}
	inserted = true
	return events, true, nil
}

func (p *ConsensusPool) maybeSlowFinalizeLocked(slot uint64) ([]ConsensusEvent, error) {
	if _, ok := p.completed[CertificateKey{Type: CertificateFinalize, Slot: slot}]; !ok {
		return nil, nil
	}
	var notarized []BlockID
	for key := range p.completed {
		if key.Slot != slot || key.Type != CertificateNotarize {
			continue
		}
		notarized = append(notarized, BlockID{Slot: slot, Hash: key.BlockHash})
	}
	if len(notarized) == 0 {
		return nil, nil
	}
	if len(notarized) > 1 {
		if _, exists := p.conflicts[slot]; exists {
			return nil, nil
		}
		reason := fmt.Sprintf("slot %d has %d notarize certificates during slow finalization", slot, len(notarized))
		p.conflicts[slot] = reason
		for block := range p.finalized {
			if block.Slot == slot {
				delete(p.finalized, block)
			}
		}
		return []ConsensusEvent{{Kind: ConsensusEventConflict, Slot: slot, Reason: reason}}, nil
	}
	if _, conflicted := p.conflicts[slot]; conflicted {
		return nil, nil
	}
	block := notarized[0]
	if _, invalid := p.invalidBlocks[block]; invalid {
		return nil, nil
	}
	if _, finalized := p.finalized[block]; finalized {
		return nil, nil
	}
	p.finalized[block] = CertificateFinalize
	return []ConsensusEvent{{Kind: ConsensusEventFinalized, Slot: slot, Block: block, CertificateType: CertificateFinalize}}, nil
}

func certificateAssemblyPriority(certType CertificateType) int {
	// This follows Agave's certificate_types_from_vote order. Stable output is
	// important because finality and parent-ready events are consumed in order.
	switch certType {
	case CertificateNotarize:
		return 0
	case CertificateNotarizeFallback:
		return 1
	case CertificateFinalizeFast:
		return 2
	case CertificateFinalize:
		return 3
	case CertificateSkip:
		return 4
	case CertificateGenesis:
		return 5
	default:
		return 6
	}
}

func filterBlocksAbove(blocks []BlockID, root uint64) []BlockID {
	kept := blocks[:0]
	for _, block := range blocks {
		if block.Slot > root {
			kept = append(kept, block)
		}
	}
	return kept
}

func buildVerifiedCertificate(slot uint64, target certificateTarget, base, fallback *consensusTally, totalStake uint64) (Certificate, error) {
	length := 0
	for _, tally := range []*consensusTally{base, fallback} {
		for rank := range tallyVotes(tally) {
			if int(rank)+1 > length {
				length = int(rank) + 1
			}
		}
	}
	baseBits := make([]bool, length)
	fallbackBits := make([]bool, length)
	var aggregate bls12381.G2Affine
	aggregate.SetInfinity()
	var stake uint64
	for rank, record := range tallyVotes(base) {
		baseBits[int(rank)] = true
		stake += record.stake
		if err := addVoteSignature(&aggregate, record.message.Signature); err != nil {
			return Certificate{}, err
		}
	}
	for rank, record := range tallyVotes(fallback) {
		if int(rank) < len(baseBits) && baseBits[int(rank)] {
			return Certificate{}, fmt.Errorf("certificate %s slot %d has rank %d in base and fallback", target.certType, slot, rank)
		}
		fallbackBits[int(rank)] = true
		stake += record.stake
		if err := addVoteSignature(&aggregate, record.message.Signature); err != nil {
			return Certificate{}, err
		}
	}
	if aggregate.IsInfinity() {
		return Certificate{}, fmt.Errorf("certificate %s slot %d has no signatures", target.certType, slot)
	}

	bitmap := SignerBitmap{Encoding: SignerBitmapBase2, Length: length, Base: baseBits}
	if fallback != nil && len(fallback.votes) > 0 {
		bitmap.Encoding = SignerBitmapBase3
		bitmap.Fallback = fallbackBits
	}
	encodedBitmap, err := EncodeSignerStoreBitmap(bitmap)
	if err != nil {
		return Certificate{}, err
	}
	raw := aggregate.RawBytes()
	cert := Certificate{
		Type:              target.certType,
		Slot:              slot,
		Signature:         append([]byte(nil), raw[:]...),
		Bitmap:            encodedBitmap,
		IncludedStake:     stake,
		TotalStake:        totalStake,
		StakeVerified:     true,
		SignatureVerified: true,
	}
	if target.certType.HasBlock() {
		cert.BlockHash = target.base.block
	}
	return cert, nil
}

func tallyVotes(tally *consensusTally) map[uint16]verifiedVoteRecord {
	if tally == nil {
		return nil
	}
	return tally.votes
}

func addVoteSignature(aggregate *bls12381.G2Affine, raw []byte) error {
	var signature bls12381.G2Affine
	if _, err := signature.SetBytes(raw); err != nil {
		return fmt.Errorf("decode verified BLS signature: %w", err)
	}
	if signature.IsInfinity() {
		return fmt.Errorf("verified BLS signature is infinity")
	}
	aggregate.Add(aggregate, &signature)
	return nil
}

func tallyStake(state *slotConsensusState, voteType VoteType, block solana.Hash) uint64 {
	tally := state.tallies[consensusTallyKey{voteType: voteType, block: block}]
	if tally == nil {
		return 0
	}
	return tally.stake
}

func fractionMeets(percent, stake, total uint64) bool {
	met, err := FractionFromPercentage(percent).Meets(stake, total)
	return err == nil && met
}

func countSlotVotes(state *slotConsensusState) int {
	count := 0
	for _, tally := range state.tallies {
		count += len(tally.votes)
	}
	return count
}
