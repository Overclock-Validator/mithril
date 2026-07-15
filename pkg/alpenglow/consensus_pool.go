package alpenglow

import (
	"encoding/binary"
	"fmt"
	"sync"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/gagliardetto/solana-go"
)

const AgaveVoteVerificationWindow = uint64(90_000)

type ConsensusEventKind string

const (
	ConsensusEventBlockNotarized ConsensusEventKind = "block-notarized"
	ConsensusEventParentReady    ConsensusEventKind = "parent-ready"
	ConsensusEventFinalized      ConsensusEventKind = "finalized"
	ConsensusEventSafeToNotar    ConsensusEventKind = "safe-to-notar"
	ConsensusEventSafeToSkip     ConsensusEventKind = "safe-to-skip"
)

type ConsensusEvent struct {
	Kind            ConsensusEventKind
	Slot            uint64
	Block           BlockID
	Fast            bool
	CertificateType CertificateType
}

type ConsensusPoolConfig struct {
	RootBlock        BlockID
	MaxSlotsAhead    uint64
	MaxTrackedSlots  int
	MaxVerifiedVotes int
	MaxEquivocations int
}

func DefaultConsensusPoolConfig() ConsensusPoolConfig {
	return ConsensusPoolConfig{
		MaxSlotsAhead:    AgaveVoteVerificationWindow,
		MaxTrackedSlots:  int(AgaveVoteVerificationWindow),
		MaxVerifiedVotes: 262_144,
		MaxEquivocations: 4_096,
	}
}

type VerifiedVote struct {
	Message VoteMessage
	Result  VoteVerifyResult
	Local   bool
}

type ConsensusUpdate struct {
	Certificates []Certificate
	Events       []ConsensusEvent
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
	safeNotarSent  map[solana.Hash]struct{}
	safeSkipSent   bool
}

// ConsensusPool is the verified-message Alpenglow bean counter. It mirrors
// Agave's certificate thresholds, conflicting vote rules, ParentReady state,
// and slow/fast finalization event ordering. Raw network messages must never
// be passed here.
type ConsensusPool struct {
	mu sync.Mutex

	cfg         ConsensusPoolConfig
	root        BlockID
	liveSlot    uint64
	slots       map[uint64]*slotConsensusState
	completed   map[CertificateKey]Certificate
	finalized   map[BlockID]bool
	parentReady *ParentReadyTracker
	evidence    []VoteEvidence

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
	return &ConsensusPool{
		cfg:         cfg,
		root:        cfg.RootBlock,
		slots:       make(map[uint64]*slotConsensusState),
		completed:   make(map[CertificateKey]Certificate),
		finalized:   make(map[BlockID]bool),
		parentReady: NewParentReadyTracker(cfg.RootBlock),
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
	if root.Slot < p.root.Slot {
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
	if p.verifiedVotes < 0 {
		p.verifiedVotes = 0
	}
	for key := range p.completed {
		if key.Slot < root.Slot {
			delete(p.completed, key)
		}
	}
}

func (p *ConsensusPool) RestoreParentReady(slot uint64, parent BlockID) {
	p.mu.Lock()
	p.parentReady.Restore(slot, parent)
	p.root = BlockID{Slot: parent.Slot, Hash: parent.Hash}
	p.mu.Unlock()
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
		return ConsensusUpdate{}, nil
	}
	anchor := p.liveSlot
	if p.root.Slot > anchor {
		anchor = p.root.Slot
	}
	if anchor != 0 && slot > anchor+p.cfg.MaxSlotsAhead {
		p.rejectedVotes++
		return ConsensusUpdate{}, nil
	}

	state := p.slots[slot]
	if state == nil {
		if len(p.slots) >= p.cfg.MaxTrackedSlots {
			p.rejectedVotes++
			return ConsensusUpdate{}, nil
		}
		state = &slotConsensusState{
			epoch:         v.Result.Epoch,
			totalStake:    v.Result.TotalStake,
			tallies:       make(map[consensusTallyKey]*consensusTally),
			byRank:        make(map[uint16]map[VoteType][]solana.Hash),
			safeNotarSent: make(map[solana.Hash]struct{}),
		}
		p.slots[slot] = state
	}
	if state.epoch != v.Result.Epoch || state.totalStake != v.Result.TotalStake {
		p.rejectedVotes++
		return ConsensusUpdate{}, fmt.Errorf("slot %d validator-set mismatch: epoch/total %d/%d, got %d/%d", slot, state.epoch, state.totalStake, v.Result.Epoch, v.Result.TotalStake)
	}
	if p.verifiedVotes >= p.cfg.MaxVerifiedVotes {
		p.rejectedVotes++
		return ConsensusUpdate{}, nil
	}

	accepted, duplicate, err := p.acceptVoteLocked(state, v)
	if err != nil {
		p.rejectedVotes++
		return ConsensusUpdate{}, err
	}
	if duplicate {
		p.duplicateVotes++
		return ConsensusUpdate{}, nil
	}
	if !accepted {
		p.rejectedVotes++
		return ConsensusUpdate{}, nil
	}

	p.verifiedVotes++
	p.verifiedTotal++
	if v.Local && state.localFirstVote == nil {
		vote := v.Message.Vote
		state.localFirstVote = &vote
	}

	update := ConsensusUpdate{}
	update.Events = append(update.Events, p.safeEventsLocked(slot, state)...)
	certs, events, err := p.assembleCertificatesLocked(slot, state)
	if err != nil {
		return ConsensusUpdate{}, err
	}
	update.Certificates = append(update.Certificates, certs...)
	update.Events = append(update.Events, events...)
	return update, nil
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
	events, _, err := p.insertCertificateLocked(cert)
	return ConsensusUpdate{Events: events}, err
}

func (p *ConsensusPool) BlockProductionParent(slot uint64) BlockProductionParent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.parentReady.BlockProductionParent(slot)
}

func (p *ConsensusPool) HasNotarFallbackOrStronger(block BlockID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.parentReady.HasNotarFallbackOrStronger(block)
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

	budget := 1
	if msg.Vote.Type == VoteTypeNotarizeFallback {
		budget = 3
	}
	if len(rankVotes[msg.Vote.Type]) >= budget {
		p.recordEvidenceLocked(VoteEvidence{Slot: msg.Vote.Slot, Rank: msg.Rank, VoteType: msg.Vote.Type, Conflicting: msg.Vote.Type, FirstBlockHash: rankVotes[msg.Vote.Type][0], SecondBlockHash: hash})
		return false, false, nil
	}
	if conflict, first, ok := conflictingVerifiedVote(rankVotes, msg.Vote); ok {
		p.recordEvidenceLocked(VoteEvidence{Slot: msg.Vote.Slot, Rank: msg.Rank, VoteType: msg.Vote.Type, Conflicting: conflict, FirstBlockHash: first, SecondBlockHash: hash})
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
		return []VoteType{VoteTypeNotarizeFallback, VoteTypeSkip}
	case VoteTypeNotarize:
		return []VoteType{VoteTypeSkip, VoteTypeNotarizeFallback}
	case VoteTypeNotarizeFallback:
		return []VoteType{VoteTypeFinalize, VoteTypeNotarize}
	case VoteTypeSkip:
		return []VoteType{VoteTypeFinalize, VoteTypeNotarize, VoteTypeSkipFallback}
	case VoteTypeSkipFallback:
		return []VoteType{VoteTypeSkip}
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
		if _, sent := state.safeNotarSent[key.block]; sent {
			continue
		}
		if state.localFirstVote.Type == VoteTypeNotarize && state.localFirstVote.BlockHash == key.block {
			continue
		}
		if fractionMeets(40, tally.stake, state.totalStake) ||
			(fractionMeets(20, tally.stake, state.totalStake) && fractionMeets(60, tally.stake+skipStake, state.totalStake)) {
			state.safeNotarSent[key.block] = struct{}{}
			events = append(events, ConsensusEvent{Kind: ConsensusEventSafeToNotar, Slot: slot, Block: BlockID{Slot: slot, Hash: key.block}})
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

	var certificates []Certificate
	var events []ConsensusEvent
	for key, target := range targets {
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
	var events []ConsensusEvent
	switch cert.Type {
	case CertificateNotarize:
		block, _ := cert.Block()
		events = append(events, ConsensusEvent{Kind: ConsensusEventBlockNotarized, Slot: cert.Slot, Block: block, CertificateType: cert.Type})
		parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
		if err != nil {
			return nil, false, err
		}
		events = append(events, parentEvents...)
		events = append(events, p.maybeSlowFinalizeLocked(cert.Slot)...)
	case CertificateNotarizeFallback:
		block, _ := cert.Block()
		parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
		if err != nil {
			return nil, false, err
		}
		events = append(events, parentEvents...)
	case CertificateFinalize:
		events = append(events, p.maybeSlowFinalizeLocked(cert.Slot)...)
	case CertificateFinalizeFast:
		block, _ := cert.Block()
		parentEvents, err := p.parentReady.AddNotarFallbackOrStronger(block)
		if err != nil {
			return nil, false, err
		}
		events = append(events, parentEvents...)
		if _, done := p.finalized[block]; !done {
			p.finalized[block] = true
			events = append(events, ConsensusEvent{Kind: ConsensusEventFinalized, Slot: cert.Slot, Block: block, Fast: true, CertificateType: cert.Type})
		}
	case CertificateSkip:
		events = append(events, p.parentReady.AddSkip(cert.Slot)...)
	case CertificateGenesis:
		block, _ := cert.Block()
		parentEvents, err := p.parentReady.AddGenesis(block)
		if err != nil {
			return nil, false, err
		}
		events = append(events, parentEvents...)
	}
	return events, true, nil
}

func (p *ConsensusPool) maybeSlowFinalizeLocked(slot uint64) []ConsensusEvent {
	if _, ok := p.completed[CertificateKey{Type: CertificateFinalize, Slot: slot}]; !ok {
		return nil
	}
	for key := range p.completed {
		if key.Slot != slot || key.Type != CertificateNotarize {
			continue
		}
		block := BlockID{Slot: slot, Hash: key.BlockHash}
		if p.finalized[block] {
			return nil
		}
		p.finalized[block] = false
		return []ConsensusEvent{{Kind: ConsensusEventFinalized, Slot: slot, Block: block, CertificateType: CertificateFinalize}}
	}
	return nil
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

func EncodeSignerStoreBitmap(bitmap SignerBitmap) ([]byte, error) {
	if bitmap.Length < 0 || bitmap.Length > MaximumValidators || bitmap.Length > int(^uint16(0)) {
		return nil, fmt.Errorf("invalid signer bitmap length %d", bitmap.Length)
	}
	if len(bitmap.Base) < bitmap.Length {
		return nil, fmt.Errorf("base signer bitmap length %d below declared %d", len(bitmap.Base), bitmap.Length)
	}
	out := make([]byte, signerStoreHeaderLen)
	binary.LittleEndian.PutUint16(out[1:3], uint16(bitmap.Length))
	switch bitmap.Encoding {
	case SignerBitmapBase2:
		out[0] = signerStoreVersionBase2
		payload := make([]byte, (bitmap.Length+7)/8)
		for rank := 0; rank < bitmap.Length; rank++ {
			if bitmap.Base[rank] {
				payload[rank/8] |= 1 << uint(rank%8)
			}
		}
		return append(out, payload...), nil
	case SignerBitmapBase3:
		if len(bitmap.Fallback) < bitmap.Length {
			return nil, fmt.Errorf("fallback signer bitmap length %d below declared %d", len(bitmap.Fallback), bitmap.Length)
		}
		out[0] = signerStoreVersionBase3
		payload := make([]byte, (bitmap.Length+base3SymbolsPerByte-1)/base3SymbolsPerByte)
		powers := [...]byte{1, 3, 9, 27, 81}
		for rank := 0; rank < bitmap.Length; rank++ {
			if bitmap.Base[rank] && bitmap.Fallback[rank] {
				return nil, fmt.Errorf("rank %d appears in base and fallback signer sets", rank)
			}
			value := byte(0)
			if bitmap.Base[rank] {
				value = 1
			} else if bitmap.Fallback[rank] {
				value = 2
			}
			payload[rank/base3SymbolsPerByte] += value * powers[rank%base3SymbolsPerByte]
		}
		return append(out, payload...), nil
	default:
		return nil, fmt.Errorf("unsupported signer bitmap encoding %q", bitmap.Encoding)
	}
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
