package alpenglow

import (
	"fmt"
	"sync"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// The certificate pool assembles Alpenglow certificates LOCALLY from raw
// votor votes, the way Agave's votor certificate pool does. Waiting for a
// peer's aggregated certificate costs an extra network hop and depends on
// peers' broadcast behavior; the pool instead tallies verified stake per
// (vote type, block) and emits a certificate the moment a threshold crosses.
// Footer certificates remain the guaranteed input for unstaked observers —
// the pool is the latency accelerator when raw Votor traffic is visible.
//
// TRUST BOUNDARY: raw votes are unauthenticated network input, not consensus
// facts. Ingest (AddVote) only shape-checks, bounds buffering, and parks the
// vote — it NEVER mutates dedupe, equivocation, disjointness, or tally state
// that assembly or fork choice depends on. Those mutate only AFTER the BLS
// signature verifies (foldTallyLocked). This prevents a bogus vote for a
// victim rank from suppressing that validator's real vote (dedupe poisoning)
// or forging equivocation evidence against an honest validator.
//
// Verification is lazy and batched: votes buffer unverified (attacker-cheap),
// and only when a tally's CANDIDATE stake first crosses its threshold does
// the pool fold the pending set — all votes in a tally sign the identical
// payload, so folding is sum(pubkeys) + sum(signatures) + ONE pairing check.
// A failed batch bisects to isolate and evidence-log the bad votes. This
// keeps steady-state pairing cost at a handful per slot instead of one per
// vote, and spends nothing at all on tallies that never approach quorum.

// CertPoolConfig bounds the pool's attacker-controlled surfaces. All bounds
// are enforced on ingest (slots and ranks arrive pre-authentication).
type CertPoolConfig struct {
	// MaxSlotsAhead rejects votes for slots beyond the trusted live watermark
	// (max of the finalized floor and the replay-observed slot) plus this bound.
	MaxSlotsAhead uint64
	// MaxPendingVotesPerSlot caps buffered unverified votes per slot across all
	// tallies.
	MaxPendingVotesPerSlot int
	// MaxLiveSlots caps how many distinct slots the pool retains buffers for; a
	// hard bound on pool memory independent of the slot window.
	MaxLiveSlots int
	// MaxPendingVotesTotal caps buffered unverified votes across ALL slots — the
	// global memory ceiling.
	MaxPendingVotesTotal int
	// EquivocationCap caps retained equivocation evidence entries (FIFO).
	EquivocationCap int
}

// DefaultCertPoolConfig mirrors the deferred-cert bounds used elsewhere.
func DefaultCertPoolConfig() CertPoolConfig {
	return CertPoolConfig{
		MaxSlotsAhead:          512,
		MaxPendingVotesPerSlot: 8192,
		MaxLiveSlots:           1024,
		MaxPendingVotesTotal:   262144,
		EquivocationCap:        4096,
	}
}

// EquivocationEvidence records a rank casting conflicting VERIFIED votes —
// retained (capped) even across pruning, surfaced via Snapshot. Because it is
// recorded only after both signatures verify, it cannot be forged against an
// honest validator.
type EquivocationEvidence struct {
	Slot   uint64
	Rank   uint16
	Type   VoteType
	First  solana.Hash
	Second solana.Hash
}

// CertPoolSnapshot summarizes pool state for logs/monitoring.
type CertPoolSnapshot struct {
	Slots            int
	VotesAccepted    uint64
	VotesRejected    uint64
	VotesEquivocated uint64
	BatchesVerified  uint64
	BadSignatures    uint64
	CertsEmitted     uint64
	Floor            uint64
	HighestSlot      uint64
	LiveSlot         uint64
	PendingTotal     int
}

// tally accumulates one (vote type, block hash) target within a slot.
type tally struct {
	pending  map[uint16]VoteMessage // unverified, by rank
	verified map[uint16]struct{}    // ranks folded into the aggregate
	aggSig   bls12381.G2Affine      // sum of verified signatures
	aggPub   bls12381.G1Affine      // sum of verified pubkeys
	stake    uint64                 // verified stake
}

func newTally() *tally {
	t := &tally{pending: make(map[uint16]VoteMessage), verified: make(map[uint16]struct{})}
	t.aggSig.SetInfinity()
	t.aggPub.SetInfinity()
	return t
}

func (t *tally) candidateStake(set *ValidatorSet) uint64 {
	total := t.stake
	for rank := range t.pending {
		if int(rank) < len(set.Validators) {
			total += set.Validators[rank].Stake
		}
	}
	return total
}

type tallyKey struct {
	Type VoteType
	Hash solana.Hash // zero for slot-only vote types
}

type poolSlot struct {
	tallies map[tallyKey]*tally
	// verifiedHash tracks the block hashes a rank has cast VERIFIED votes for,
	// per (rank, type), for equivocation/vote-budget enforcement. Populated only
	// after signature verification — never from raw ingest — so a bogus vote can
	// neither poison dedupe nor forge equivocation. Notarize-fallback legally
	// allows up to 3 distinct hashes per rank (Def. 12 vote budget); every other
	// type is single-shot.
	verifiedHash map[voteDedupKey][]solana.Hash
	pendingCount int
}

type voteDedupKey struct {
	Rank uint16
	Type VoteType
}

// CertPool assembles certificates from raw votes. Emit receives fully
// stake+signature-verified certificates (identical trust to a verified wire
// cert) and must be fast (it runs after unlock, but serially).
type CertPool struct {
	cfg      CertPoolConfig
	verifier *CertificateVerifier
	emit     func(Certificate)

	mu           sync.Mutex
	epochForSlot func(slot uint64) uint64
	slots        map[uint64]*poolSlot
	emitted      map[CertificateKey]struct{}
	floor        uint64
	liveSlot     uint64 // trusted replay/observed watermark (NOT advanced by raw votes)
	highestSlot  uint64 // observability only: highest vote slot seen
	totalPending int
	equivocation []EquivocationEvidence
	snap         CertPoolSnapshot
}

// NewCertPool constructs a pool over the shared certificate verifier's
// validator sets. emit may be nil (tally-only mode).
func NewCertPool(cfg CertPoolConfig, verifier *CertificateVerifier, emit func(Certificate)) *CertPool {
	if cfg.MaxSlotsAhead == 0 {
		cfg.MaxSlotsAhead = 512
	}
	if cfg.MaxPendingVotesPerSlot <= 0 {
		cfg.MaxPendingVotesPerSlot = 8192
	}
	if cfg.MaxLiveSlots <= 0 {
		cfg.MaxLiveSlots = 1024
	}
	if cfg.MaxPendingVotesTotal <= 0 {
		cfg.MaxPendingVotesTotal = 262144
	}
	if cfg.EquivocationCap <= 0 {
		cfg.EquivocationCap = 4096
	}
	return &CertPool{
		cfg:      cfg,
		verifier: verifier,
		emit:     emit,
		slots:    make(map[uint64]*poolSlot),
		emitted:  make(map[CertificateKey]struct{}),
	}
}

// SetEpochLookup wires slot→epoch resolution (validator set selection).
func (p *CertPool) SetEpochLookup(fn func(slot uint64) uint64) {
	p.mu.Lock()
	p.epochForSlot = fn
	p.mu.Unlock()
}

// NoteLiveSlot advances the trusted live watermark that anchors the vote window
// (from replay progress / observed finality — never from raw votes, so an
// attacker cannot slide the window forward). Monotonic.
func (p *CertPool) NoteLiveSlot(slot uint64) {
	p.mu.Lock()
	if slot > p.liveSlot {
		p.liveSlot = slot
	}
	p.mu.Unlock()
}

// windowAnchorLocked is the trusted upper anchor of the live vote window: the
// higher of the finalized floor and the replay-observed live slot. It is NOT
// derived from raw votes, so ingest cannot advance it.
func (p *CertPool) windowAnchorLocked() uint64 {
	if p.floor > p.liveSlot {
		return p.floor
	}
	return p.liveSlot
}

// setForSlotLocked resolves the validator set covering slot. Returns nil (votes
// stay buffered) when the slot→epoch map is not installed or the epoch's set is
// unknown — the pool NEVER falls back to a guessed epoch, which could assemble
// against the wrong validator set across an epoch boundary.
func (p *CertPool) setForSlotLocked(slot uint64) *ValidatorSet {
	if p.epochForSlot == nil {
		return nil
	}
	set, ok := p.verifier.ValidatorSetForEpoch(p.epochForSlot(slot))
	if !ok {
		return nil
	}
	return &set
}

// AddVote ingests one raw (unverified) votor vote. It ONLY shape-checks, bounds
// buffering, and parks the vote in its (type, hash) tally. It does NOT touch
// dedupe/equivocation/disjointness state — those mutate only after the vote's
// signature verifies (foldTallyLocked). A malformed vote, a vote outside the
// trusted slot window, or a vote past a memory bound is dropped.
func (p *CertPool) AddVote(msg VoteMessage) {
	if msg.Vote.ValidateBasic() != nil || len(msg.Signature) != BLSSignatureSize {
		return
	}
	slot := msg.Vote.Slot

	var emits []Certificate
	p.mu.Lock()
	if slot <= p.floor {
		p.snap.VotesRejected++
		p.mu.Unlock()
		return
	}
	// Window anchored to the TRUSTED watermark (floor / replay-observed), not to
	// the highest vote slot seen — otherwise an attacker could slide it forward
	// vote by vote and retain arbitrarily many future slots.
	if anchor := p.windowAnchorLocked(); anchor > 0 && slot > anchor+p.cfg.MaxSlotsAhead {
		p.snap.VotesRejected++
		p.mu.Unlock()
		return
	}

	ps := p.slots[slot]
	if ps == nil {
		// Hard global bound on retained slots (independent of the window).
		if len(p.slots) >= p.cfg.MaxLiveSlots {
			p.snap.VotesRejected++
			p.mu.Unlock()
			return
		}
		ps = &poolSlot{tallies: make(map[tallyKey]*tally), verifiedHash: make(map[voteDedupKey][]solana.Hash)}
		p.slots[slot] = ps
	}

	// Memory bounds only — no trust-sensitive state is touched here.
	if ps.pendingCount >= p.cfg.MaxPendingVotesPerSlot || p.totalPending >= p.cfg.MaxPendingVotesTotal {
		p.snap.VotesRejected++
		p.mu.Unlock()
		return
	}

	tk := tallyKey{Type: msg.Vote.Type, Hash: msg.Vote.BlockHash}
	tl := ps.tallies[tk]
	if tl == nil {
		tl = newTally()
		ps.tallies[tk] = tl
	}
	// One pending vote per rank per tally; a rank already verified in this tally
	// needs no re-buffer. (Different block hashes are DIFFERENT tallies, so a
	// bogus hash cannot displace the real one — no cross-tally poisoning.)
	if _, dup := tl.pending[msg.Rank]; !dup {
		if _, done := tl.verified[msg.Rank]; !done {
			tl.pending[msg.Rank] = msg
			ps.pendingCount++
			p.totalPending++
			p.snap.VotesAccepted++
		}
	}
	if slot > p.highestSlot {
		p.highestSlot = slot
	}

	// Keep verified stake fresh for the Votor fallback triggers. Only plain
	// notarize/skip arrivals can change the trigger predicates.
	if msg.Vote.Type == VoteTypeNotarize || msg.Vote.Type == VoteTypeSkip {
		p.maybeFoldTriggersLocked(slot, ps)
	}
	emits = p.maybeAssembleLocked(slot, ps)
	p.mu.Unlock()

	for _, cert := range emits {
		p.emitCert(cert)
	}
}

// OnValidatorSetInstalled retries assembly for buffered slots that resolve to
// the newly-installed epoch. Requires a real slot→epoch lookup; without one no
// slot can be safely attributed to the epoch, so nothing is retried.
func (p *CertPool) OnValidatorSetInstalled(epoch uint64) {
	var emits []Certificate
	p.mu.Lock()
	if p.epochForSlot != nil {
		for slot, ps := range p.slots {
			if p.epochForSlot(slot) != epoch {
				continue
			}
			p.maybeFoldTriggersLocked(slot, ps)
			emits = append(emits, p.maybeAssembleLocked(slot, ps)...)
		}
	}
	p.mu.Unlock()
	for _, cert := range emits {
		p.emitCert(cert)
	}
}

// ObserveFloor prunes slots <= finalizedSlot (equivocation evidence is exempt).
func (p *CertPool) ObserveFloor(finalizedSlot uint64) {
	p.mu.Lock()
	if finalizedSlot > p.floor {
		p.floor = finalizedSlot
		for slot, ps := range p.slots {
			if slot <= finalizedSlot {
				p.totalPending -= ps.pendingCount
				delete(p.slots, slot)
			}
		}
		if p.totalPending < 0 {
			p.totalPending = 0
		}
		for key := range p.emitted {
			if key.Slot <= finalizedSlot {
				delete(p.emitted, key)
			}
		}
	}
	p.mu.Unlock()
}

// EquivocationEvidence returns the retained evidence (copy).
func (p *CertPool) EquivocationEvidence() []EquivocationEvidence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]EquivocationEvidence(nil), p.equivocation...)
}

// Snapshot returns pool counters.
func (p *CertPool) Snapshot() CertPoolSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := p.snap
	snap.Slots = len(p.slots)
	snap.Floor = p.floor
	snap.HighestSlot = p.highestSlot
	snap.LiveSlot = p.liveSlot
	snap.PendingTotal = p.totalPending
	return snap
}

func (p *CertPool) recordEquivocationLocked(ev EquivocationEvidence) {
	p.snap.VotesEquivocated++
	if len(p.equivocation) >= p.cfg.EquivocationCap {
		p.equivocation = p.equivocation[1:]
	}
	p.equivocation = append(p.equivocation, ev)
	mlog.Log.Warnf("ALPENGLOW cert pool: equivocation evidence — rank %d cast conflicting VERIFIED %s votes at slot %d (%s vs %s)",
		ev.Rank, ev.Type, ev.Slot, ev.First, ev.Second)
}

// Votor fallback-trigger thresholds, transcribed from Agave
// votor/src/common.rs (Fraction::from_percentage):
//
//	SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY                 = 40
//	SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP = 20
//	SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP             = 60
//	SAFE_TO_SKIP_THRESHOLD                          = 40
//
// SafeToNotar(b): notar(b) >= 40%  OR  (notar(b) >= 20% AND notar(b)+skip >= 60%).
// SafeToSkip:     skip + (notarTotal - topNotar) >= 40%.
// Only PLAIN notarize/skip votes feed these counters (fallback votes are
// excluded — Agave slot_stake_counters.rs "not interested in other vote types").
var (
	safeToNotarMinNotarizeOnly    = FractionFromPercentage(40)
	safeToNotarMinNotarizeForMix  = FractionFromPercentage(20)
	safeToNotarMinNotarizeAndSkip = FractionFromPercentage(60)
	safeToSkipThreshold           = FractionFromPercentage(40)
)

// votorTriggerView is one slot's trigger-relevant stake, candidate (verified +
// buffered, an over-approximation) and verified, for plain notarize/skip only.
type votorTriggerView struct {
	candNotar map[solana.Hash]uint64
	verNotar  map[solana.Hash]uint64
	candSkip  uint64
	verSkip   uint64
	candTotal uint64 // sum of candNotar
	verTotal  uint64
	candTop   uint64 // max of candNotar
	verTop    uint64
}

func buildTriggerViewLocked(ps *poolSlot, set *ValidatorSet) votorTriggerView {
	v := votorTriggerView{
		candNotar: make(map[solana.Hash]uint64),
		verNotar:  make(map[solana.Hash]uint64),
	}
	for tk, tl := range ps.tallies {
		switch tk.Type {
		case VoteTypeNotarize:
			cand := tl.candidateStake(set)
			v.candNotar[tk.Hash] = cand
			v.verNotar[tk.Hash] = tl.stake
			v.candTotal += cand
			v.verTotal += tl.stake
			if cand > v.candTop {
				v.candTop = cand
			}
			if tl.stake > v.verTop {
				v.verTop = tl.stake
			}
		case VoteTypeSkip:
			v.candSkip = tl.candidateStake(set)
			v.verSkip = tl.stake
		}
	}
	return v
}

func meets(f Fraction, stake, total uint64) bool {
	met, err := f.Meets(stake, total)
	return err == nil && met
}

// maybeFoldTriggersLocked keeps verified stake fresh for Votor's fallback
// triggers: whenever a trigger predicate passes on CANDIDATE stake but not yet
// on VERIFIED stake, the involved tallies fold (batch-verify) immediately.
// Candidate stake over-approximates verified stake, so a predicate crossing is
// never observed late — the voting engine evaluating the same predicates on
// VerifiedVotorStakes sees them at the same time an eager-verification
// implementation (Agave) would. This is the ONLY fold policy: one fork-choice
// behavior for observer and voting nodes alike; sub-trigger tallies still
// cost nothing.
func (p *CertPool) maybeFoldTriggersLocked(slot uint64, ps *poolSlot) {
	set := p.setForSlotLocked(slot)
	if set == nil {
		return // votes stay buffered; retried on OnValidatorSetInstalled
	}
	total := set.TotalStake
	v := buildTriggerViewLocked(ps, set)

	foldNotar := make(map[solana.Hash]bool)
	foldSkip := false
	foldAllNotar := false

	for hash, cand := range v.candNotar {
		ver := v.verNotar[hash]
		// SafeToNotar(i): notar(b) alone.
		if meets(safeToNotarMinNotarizeOnly, cand, total) && !meets(safeToNotarMinNotarizeOnly, ver, total) {
			foldNotar[hash] = true
		}
		// SafeToNotar(ii): notar(b) >= 20% AND notar(b)+skip >= 60%.
		candMix := meets(safeToNotarMinNotarizeForMix, cand, total) &&
			meets(safeToNotarMinNotarizeAndSkip, cand+v.candSkip, total)
		verMix := meets(safeToNotarMinNotarizeForMix, ver, total) &&
			meets(safeToNotarMinNotarizeAndSkip, ver+v.verSkip, total)
		if candMix && !verMix {
			foldNotar[hash] = true
			foldSkip = true
		}
	}
	// SafeToSkip: skip + (notarTotal - topNotar) >= 40%. Involves every notarize
	// tally (the total and the max), so a crossing folds them all.
	candSTS := meets(safeToSkipThreshold, v.candSkip+(v.candTotal-v.candTop), total)
	verSTS := meets(safeToSkipThreshold, v.verSkip+(v.verTotal-v.verTop), total)
	if candSTS && !verSTS {
		foldSkip = true
		foldAllNotar = true
	}

	if !foldSkip && !foldAllNotar && len(foldNotar) == 0 {
		return
	}
	for tk, tl := range ps.tallies {
		switch tk.Type {
		case VoteTypeNotarize:
			if foldAllNotar || foldNotar[tk.Hash] {
				p.foldTallyLocked(slot, ps, tl, set)
			}
		case VoteTypeSkip:
			if foldSkip {
				p.foldTallyLocked(slot, ps, tl, set)
			}
		}
	}
}

// VotorStakes is the verified-stake observation Votor's fallback-trigger
// predicates consume (plain notarize/skip only, matching Agave's
// slot_stake_counters). The pool guarantees freshness: whenever a trigger
// predicate could pass, the involved stake has already been verified — a
// voting engine layered on top evaluates predicates on these numbers with no
// additional verification machinery.
type VotorStakes struct {
	Notarize      map[solana.Hash]uint64
	NotarizeTotal uint64
	TopNotarize   uint64
	Skip          uint64
	TotalStake    uint64
}

// VerifiedVotorStakes returns the slot's verified trigger stakes. ok is false
// when the slot's epoch/validator set is not resolvable yet.
func (p *CertPool) VerifiedVotorStakes(slot uint64) (VotorStakes, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	set := p.setForSlotLocked(slot)
	if set == nil {
		return VotorStakes{}, false
	}
	out := VotorStakes{Notarize: make(map[solana.Hash]uint64), TotalStake: set.TotalStake}
	ps := p.slots[slot]
	if ps == nil {
		return out, true
	}
	for tk, tl := range ps.tallies {
		switch tk.Type {
		case VoteTypeNotarize:
			out.Notarize[tk.Hash] = tl.stake
			out.NotarizeTotal += tl.stake
			if tl.stake > out.TopNotarize {
				out.TopNotarize = tl.stake
			}
		case VoteTypeSkip:
			out.Skip = tl.stake
		}
	}
	return out, true
}

// certTarget describes one assemblable certificate: a base tally plus an
// optional fallback tally (base3 encoding).
type certTarget struct {
	certType CertificateType
	base     tallyKey
	fallback *tallyKey
}

func targetsForSlot(ps *poolSlot) []certTarget {
	var out []certTarget
	// Union targets (notar-fallback, skip) must be generated whether the BASE
	// tally, the FALLBACK tally, or both exist: Agave's certificate builder
	// assembles a NotarizeFallback/Skip cert from fallback votes alone (base3
	// with an empty base bitmap), and a contested slot is exactly where fallback
	// votes can dominate. Dedupe so a slot holding both tallies yields one target.
	seen := make(map[CertificateKey]struct{}, len(ps.tallies))
	add := func(t certTarget) {
		k := CertificateKey{Type: t.certType}
		if t.certType.HasBlock() {
			k.BlockHash = t.base.Hash
		}
		if _, dup := seen[k]; dup {
			return
		}
		seen[k] = struct{}{}
		out = append(out, t)
	}
	for tk := range ps.tallies {
		switch tk.Type {
		case VoteTypeNotarize:
			add(certTarget{certType: CertificateNotarize, base: tk})
			add(certTarget{certType: CertificateFinalizeFast, base: tk})
			add(certTarget{certType: CertificateNotarizeFallback, base: tk,
				fallback: &tallyKey{Type: VoteTypeNotarizeFallback, Hash: tk.Hash}})
		case VoteTypeNotarizeFallback:
			add(certTarget{certType: CertificateNotarizeFallback,
				base:     tallyKey{Type: VoteTypeNotarize, Hash: tk.Hash},
				fallback: &tallyKey{Type: VoteTypeNotarizeFallback, Hash: tk.Hash}})
		case VoteTypeFinalize:
			add(certTarget{certType: CertificateFinalize, base: tk})
		case VoteTypeSkip:
			add(certTarget{certType: CertificateSkip, base: tk,
				fallback: &tallyKey{Type: VoteTypeSkipFallback, Hash: solana.Hash{}}})
		case VoteTypeSkipFallback:
			add(certTarget{certType: CertificateSkip,
				base:     tallyKey{Type: VoteTypeSkip},
				fallback: &tallyKey{Type: VoteTypeSkipFallback, Hash: solana.Hash{}}})
		case VoteTypeGenesis:
			add(certTarget{certType: CertificateGenesis, base: tk})
		}
	}
	return out
}

// maybeAssembleLocked checks every assemblable target for the slot: folds
// pending votes (batch verification) once candidate stake crosses the
// threshold, and returns any newly assembled certificates for emission.
func (p *CertPool) maybeAssembleLocked(slot uint64, ps *poolSlot) []Certificate {
	set := p.setForSlotLocked(slot)
	if set == nil {
		return nil // validator set / epoch not resolvable yet; votes stay buffered
	}

	var emits []Certificate
	for _, target := range targetsForSlot(ps) {
		key := CertificateKey{Type: target.certType, Slot: slot}
		if target.certType.HasBlock() {
			key.BlockHash = target.base.Hash
		}
		if _, done := p.emitted[key]; done {
			continue
		}

		// The base tally may be absent for union targets (fallback-only
		// assembly); a nil base contributes nothing and encodes as an empty
		// base group, exactly like Agave's empty bitmap0.
		base := ps.tallies[target.base]
		var fb *tally
		if target.fallback != nil {
			fb = ps.tallies[*target.fallback]
		}
		if base == nil && fb == nil {
			continue
		}

		threshold := target.certType.RequiredThreshold()
		candidate := uint64(0)
		if base != nil {
			candidate += base.candidateStake(set)
		}
		if fb != nil {
			candidate += fb.candidateStake(set)
		}
		if met, err := threshold.Meets(candidate, set.TotalStake); err != nil || !met {
			continue
		}

		// Candidate stake crossed: fold pending votes (one pairing per tally).
		p.foldTallyLocked(slot, ps, base, set)
		p.foldTallyLocked(slot, ps, fb, set)

		verifiedStake := uint64(0)
		if base != nil {
			verifiedStake += base.stake
		}
		if fb != nil {
			verifiedStake += fb.stake
		}
		if met, err := threshold.Meets(verifiedStake, set.TotalStake); err != nil || !met {
			continue // bad signatures dropped below the bar; wait for more votes
		}

		cert, err := p.assembleLocked(slot, target, base, fb, set, verifiedStake)
		if err != nil {
			mlog.Log.Warnf("ALPENGLOW cert pool: assembly of %s@%d failed: %v", target.certType, slot, err)
			continue
		}
		p.emitted[key] = struct{}{}
		p.snap.CertsEmitted++
		emits = append(emits, cert)
	}
	return emits
}

// foldTallyLocked batch-verifies a tally's pending votes: all sign the same
// payload, so verification is sum(pubkeys)+sum(sigs)+one pairing; failures
// bisect to isolate the bad votes (dropped + counted). Only AFTER a vote
// verifies does it update the durable per-slot state — the vote-budget /
// equivocation ledger (verifiedHash) and base↔fallback disjointness — so raw
// votes can never poison those.
func (p *CertPool) foldTallyLocked(slot uint64, ps *poolSlot, tl *tally, set *ValidatorSet) {
	if tl == nil || len(tl.pending) == 0 {
		return
	}
	batch := make([]VoteMessage, 0, len(tl.pending))
	for rank, msg := range tl.pending {
		if int(rank) >= len(set.Validators) {
			delete(tl.pending, rank)
			ps.pendingCount--
			p.totalPending--
			p.snap.VotesRejected++
			continue
		}
		batch = append(batch, msg)
	}
	good := p.verifyBatch(batch, set)
	for _, msg := range batch {
		delete(tl.pending, msg.Rank)
		ps.pendingCount--
		p.totalPending--
	}

	for _, msg := range good {
		// Vote budget + equivocation, on VERIFIED votes only. A rank may cast a
		// notarize-fallback for up to 3 distinct hashes; every other type is
		// single-shot. A conflicting VERIFIED vote beyond budget is cryptographic
		// equivocation evidence and is not counted.
		dk := voteDedupKey{Rank: msg.Rank, Type: msg.Vote.Type}
		seen := ps.verifiedHash[dk]
		dupHash := false
		for _, h := range seen {
			if h == msg.Vote.BlockHash {
				dupHash = true
				break
			}
		}
		if dupHash {
			continue // already counted this exact verified vote
		}
		budget := 1
		if msg.Vote.Type == VoteTypeNotarizeFallback {
			budget = 3
		}
		if len(seen) >= budget {
			p.recordEquivocationLocked(EquivocationEvidence{
				Slot: slot, Rank: msg.Rank, Type: msg.Vote.Type,
				First: seen[0], Second: msg.Vote.BlockHash,
			})
			continue
		}
		// Base↔fallback disjointness: a rank already counted in the paired tally
		// (notarize↔notarize-fallback on the same block, skip↔skip-fallback) must
		// not also count here, or the base3 bitmap would double-encode it.
		if pairKey, paired := pairedTallyKey(msg.Vote); paired {
			if pt := ps.tallies[pairKey]; pt != nil {
				if _, in := pt.verified[msg.Rank]; in {
					continue
				}
			}
		}

		var pub bls12381.G1Affine
		if _, err := pub.SetBytes(set.Validators[msg.Rank].BlsPubkeyCompressed[:]); err != nil {
			continue
		}
		var sig bls12381.G2Affine
		if _, err := sig.SetBytes(msg.Signature); err != nil {
			continue
		}
		tl.aggPub.Add(&tl.aggPub, &pub)
		tl.aggSig.Add(&tl.aggSig, &sig)
		tl.verified[msg.Rank] = struct{}{}
		tl.stake += set.Validators[msg.Rank].Stake
		ps.verifiedHash[dk] = append(seen, msg.Vote.BlockHash)
	}
	p.snap.BadSignatures += uint64(len(batch) - len(good))
}

// verifyBatch verifies same-payload votes with one aggregate pairing check,
// bisecting on failure. Returns the subset that verified.
func (p *CertPool) verifyBatch(batch []VoteMessage, set *ValidatorSet) []VoteMessage {
	if len(batch) == 0 {
		return nil
	}
	p.snap.BatchesVerified++
	payload, err := EncodeVote(batch[0].Vote)
	if err != nil {
		return nil
	}

	var aggPub bls12381.G1Affine
	var aggSig bls12381.G2Affine
	aggPub.SetInfinity()
	aggSig.SetInfinity()
	parsed := make([]struct {
		pub bls12381.G1Affine
		sig bls12381.G2Affine
		ok  bool
	}, len(batch))
	for i, msg := range batch {
		if _, err := parsed[i].pub.SetBytes(set.Validators[msg.Rank].BlsPubkeyCompressed[:]); err != nil || parsed[i].pub.IsInfinity() {
			continue
		}
		if _, err := parsed[i].sig.SetBytes(msg.Signature); err != nil || parsed[i].sig.IsInfinity() {
			continue
		}
		parsed[i].ok = true
		aggPub.Add(&aggPub, &parsed[i].pub)
		aggSig.Add(&aggSig, &parsed[i].sig)
	}

	valid := make([]VoteMessage, 0, len(batch))
	structurallyOK := make([]VoteMessage, 0, len(batch))
	for i := range batch {
		if parsed[i].ok {
			structurallyOK = append(structurallyOK, batch[i])
		}
	}
	if len(structurallyOK) == 0 {
		return nil
	}
	if len(structurallyOK) == len(batch) {
		if aggregatePairingOK(aggPub, payload, aggSig) {
			return batch
		}
	} else {
		// Rebuild aggregates over the structurally-valid subset only.
		aggPub.SetInfinity()
		aggSig.SetInfinity()
		for i := range batch {
			if parsed[i].ok {
				aggPub.Add(&aggPub, &parsed[i].pub)
				aggSig.Add(&aggSig, &parsed[i].sig)
			}
		}
		if aggregatePairingOK(aggPub, payload, aggSig) {
			return structurallyOK
		}
	}

	// Aggregate failed: bisect the structurally-valid subset.
	if len(structurallyOK) == 1 {
		return valid
	}
	mid := len(structurallyOK) / 2
	valid = append(valid, p.verifyBatch(structurallyOK[:mid], set)...)
	valid = append(valid, p.verifyBatch(structurallyOK[mid:], set)...)
	return valid
}

func aggregatePairingOK(aggPub bls12381.G1Affine, payload []byte, aggSig bls12381.G2Affine) bool {
	if aggPub.IsInfinity() {
		return false
	}
	return verifyBLSSignature(aggPub, payload, aggSig) == nil
}

// assembleLocked builds the wire-shaped certificate: aggregate signature =
// G2 sum across contributing tallies; bitmap base2 (base votes only) or base3
// (fallback group non-empty — including a fallback-only cert with an EMPTY
// base group, mirroring Agave's empty-bitmap0 base3 encoding). Disjointness is
// guaranteed by per-rank verified-time tally membership.
func (p *CertPool) assembleLocked(slot uint64, target certTarget, base, fb *tally, set *ValidatorSet, verifiedStake uint64) (Certificate, error) {
	length := len(set.Validators)
	baseRanks := make([]bool, length)
	var aggSig bls12381.G2Affine
	aggSig.SetInfinity()
	if base != nil {
		for rank := range base.verified {
			baseRanks[int(rank)] = true
		}
		aggSig.Add(&aggSig, &base.aggSig)
	}
	var bitmap []byte
	var err error
	if fb != nil && len(fb.verified) > 0 {
		fbRanks := make([]bool, length)
		for rank := range fb.verified {
			if baseRanks[int(rank)] {
				// Verified-time disjointness makes this unreachable; fail closed
				// rather than emit a non-disjoint base3 bitmap.
				return Certificate{}, fmt.Errorf("rank %d present in both base and fallback tallies", rank)
			}
			fbRanks[int(rank)] = true
		}
		aggSig.Add(&aggSig, &fb.aggSig)
		bitmap, err = EncodeSignerStoreBitmap(SignerBitmap{Encoding: SignerBitmapBase3, Length: length, Base: baseRanks, Fallback: fbRanks})
	} else {
		bitmap, err = EncodeSignerStoreBitmap(SignerBitmap{Encoding: SignerBitmapBase2, Length: length, Base: baseRanks})
	}
	if err != nil {
		return Certificate{}, err
	}

	raw := aggSig.RawBytes()
	cert := Certificate{
		Type:              target.certType,
		Slot:              slot,
		Signature:         raw[:],
		Bitmap:            bitmap,
		IncludedStake:     verifiedStake,
		TotalStake:        set.TotalStake,
		StakeVerified:     true,
		SignatureVerified: true,
	}
	if target.certType.HasBlock() {
		cert.BlockHash = target.base.Hash
	}
	return cert, nil
}

func (p *CertPool) emitCert(cert Certificate) {
	if p.emit == nil {
		return
	}
	mlog.Log.FileOnlyf("ALPENGLOW cert pool: assembled %s certificate for slot %d (stake %d/%d)",
		cert.Type, cert.Slot, cert.IncludedStake, cert.TotalStake)
	p.emit(cert)
}

// pairedTallyKey returns the tally whose votes share a union certificate with
// this vote's tally (notarize <-> notarize-fallback on the same block, skip
// <-> skip-fallback), for the verified-time disjointness guard.
func pairedTallyKey(v Vote) (tallyKey, bool) {
	switch v.Type {
	case VoteTypeNotarize:
		return tallyKey{Type: VoteTypeNotarizeFallback, Hash: v.BlockHash}, true
	case VoteTypeNotarizeFallback:
		return tallyKey{Type: VoteTypeNotarize, Hash: v.BlockHash}, true
	case VoteTypeSkip:
		return tallyKey{Type: VoteTypeSkipFallback}, true
	case VoteTypeSkipFallback:
		return tallyKey{Type: VoteTypeSkip}, true
	default:
		return tallyKey{}, false
	}
}
