package alpenglow

import (
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const (
	DefaultMaxTrackedVotes        = 4096
	DefaultMaxTrackedCertificates = 4096
	DefaultMaxTrackedReplayBlocks = 4096
)

type ObserverConfig struct {
	MaxTrackedVotes        int
	MaxTrackedCertificates int
	MaxTrackedReplayBlocks int
}

func DefaultObserverConfig() ObserverConfig {
	return ObserverConfig{
		MaxTrackedVotes:        DefaultMaxTrackedVotes,
		MaxTrackedCertificates: DefaultMaxTrackedCertificates,
		MaxTrackedReplayBlocks: DefaultMaxTrackedReplayBlocks,
	}
}

type ReplayBlockObservation struct {
	Block      BlockID     `json:"block"`
	ParentSlot uint64      `json:"parent_slot,omitempty"`
	ParentHash solana.Hash `json:"parent_hash,omitempty"`
	Source     string      `json:"source,omitempty"`
	At         time.Time   `json:"at,omitempty"`
}

type ReplayResultObservation struct {
	Slot     uint64      `json:"slot"`
	Bankhash solana.Hash `json:"bankhash"`
	Source   string      `json:"source,omitempty"`
	At       time.Time   `json:"at,omitempty"`
}

type Observation struct {
	New      bool     `json:"new"`
	Snapshot Snapshot `json:"snapshot"`
}

type Snapshot struct {
	VotesObserved                      uint64  `json:"votes_observed"`
	CertificatesObserved               uint64  `json:"certificates_observed"`
	VotesRetained                      uint64  `json:"votes_retained"`
	CertificatesRetained               uint64  `json:"certificates_retained"`
	ReplayBlocksObserved               uint64  `json:"replay_blocks_observed"`
	ReplayBlocksRetained               uint64  `json:"replay_blocks_retained"`
	ReplayResultsObserved              uint64  `json:"replay_results_observed"`
	CertificateReplayMatches           uint64  `json:"certificate_replay_matches"`
	CertificateReplayMismatches        uint64  `json:"certificate_replay_mismatches"`
	CertificateReplayPending           uint64  `json:"certificate_replay_pending"`
	CertificateReplayMaturePending     uint64  `json:"certificate_replay_mature_pending"`
	CertificateReplayPreWindowPending  uint64  `json:"certificate_replay_pre_window_pending"`
	CertificateReplayPendingOldestSlot uint64  `json:"certificate_replay_pending_oldest_slot,omitempty"`
	CertificateReplayPendingNewestSlot uint64  `json:"certificate_replay_pending_newest_slot,omitempty"`
	CertificateReplayMatureOldestSlot  uint64  `json:"certificate_replay_mature_oldest_slot,omitempty"`
	StakeCheckedCertificates           uint64  `json:"stake_checked_certificates"`
	SignatureVerifiedCertificates      uint64  `json:"signature_verified_certificates"`
	LatestVoteSlot                     uint64  `json:"latest_vote_slot,omitempty"`
	LatestCertificateSlot              uint64  `json:"latest_certificate_slot,omitempty"`
	OldestReplayBlockSlot              uint64  `json:"oldest_replay_block_slot,omitempty"`
	LatestReplayBlockSlot              uint64  `json:"latest_replay_block_slot,omitempty"`
	LatestReplayBlock                  BlockID `json:"latest_replay_block,omitempty"`
	LatestReplayResultSlot             uint64  `json:"latest_replay_result_slot,omitempty"`
	LatestCertificateReplayMatch       BlockID `json:"latest_certificate_replay_match,omitempty"`
	LatestCertificateReplayMiss        BlockID `json:"latest_certificate_replay_miss,omitempty"`
	LatestReplayMiss                   BlockID `json:"latest_replay_miss,omitempty"`
	LatestNotarizedBlock               BlockID `json:"latest_notarized_block,omitempty"`
	LatestFallbackBlock                BlockID `json:"latest_fallback_block,omitempty"`
	LatestFastFinalizedBlock           BlockID `json:"latest_fast_finalized_block,omitempty"`
	LatestFinalizedSlot                uint64  `json:"latest_finalized_slot,omitempty"`
	LatestSkippedSlot                  uint64  `json:"latest_skipped_slot,omitempty"`
	GenesisBlock                       BlockID `json:"genesis_block,omitempty"`
}

type Observer struct {
	mu sync.RWMutex

	cfg ObserverConfig

	votes        map[VoteMessageKey]VoteMessage
	voteOrder    []VoteMessageKey
	certificates map[CertificateKey]Certificate
	certOrder    []CertificateKey
	replayBlocks map[uint64]BlockID
	replayOrder  []uint64
	replayChecks map[CertificateKey]certificateReplayCheck
	// Votes do not change certificate/replay reconciliation. Reuse its exact
	// statistics until one of those inputs changes instead of scanning every
	// retained certificate for each incoming vote.
	pendingStats      certificateReplayPendingStats
	pendingStatsValid bool

	votesObserved                 uint64
	certificatesObserved          uint64
	replayBlocksObserved          uint64
	replayResultsObserved         uint64
	certificateReplayMatches      uint64
	certificateReplayMismatches   uint64
	stakeCheckedCertificates      uint64
	signatureVerifiedCertificates uint64
	latestVoteSlot                uint64
	latestCertificateSlot         uint64
	oldestReplayBlockSlot         uint64
	latestReplayBlockSlot         uint64
	latestReplayBlock             BlockID
	latestReplayResultSlot        uint64
	latestCertificateReplayMatch  BlockID
	latestCertificateReplayMiss   BlockID
	latestReplayMiss              BlockID
	latestNotarizedBlock          BlockID
	latestFallbackBlock           BlockID
	latestFastFinalizedBlock      BlockID
	latestFinalizedSlot           uint64
	latestSkippedSlot             uint64
	genesisBlock                  BlockID
}

type certificateReplayStatus uint8

const (
	certificateReplayMatch certificateReplayStatus = iota + 1
	certificateReplayMismatch
)

type certificateReplayCheck struct {
	status           certificateReplayStatus
	certificateBlock BlockID
	replayBlock      BlockID
}

func NewObserver() *Observer {
	return NewObserverWithConfig(DefaultObserverConfig())
}

func NewObserverWithConfig(cfg ObserverConfig) *Observer {
	if cfg.MaxTrackedVotes < 0 {
		cfg.MaxTrackedVotes = DefaultMaxTrackedVotes
	}
	if cfg.MaxTrackedCertificates < 0 {
		cfg.MaxTrackedCertificates = DefaultMaxTrackedCertificates
	}
	if cfg.MaxTrackedReplayBlocks < 0 {
		cfg.MaxTrackedReplayBlocks = DefaultMaxTrackedReplayBlocks
	}
	return &Observer{
		cfg:          cfg,
		votes:        make(map[VoteMessageKey]VoteMessage),
		certificates: make(map[CertificateKey]Certificate),
		replayBlocks: make(map[uint64]BlockID),
		replayChecks: make(map[CertificateKey]certificateReplayCheck),
	}
}

func (o *Observer) ObserveMessage(msg Message) (Observation, error) {
	if err := msg.ValidateBasic(); err != nil {
		return Observation{Snapshot: o.Snapshot()}, err
	}
	if msg.Vote != nil {
		return o.ObserveVote(*msg.Vote)
	}
	return o.ObserveCertificate(*msg.Certificate)
}

func (o *Observer) ObserveWireMessage(data []byte) (Observation, error) {
	return o.ObserveWireMessageWithOptions(data, DefaultDecodeOptions())
}

func (o *Observer) ObserveWireMessageWithOptions(data []byte, opts DecodeOptions) (Observation, error) {
	msg, err := DecodeMessageWithOptions(data, opts)
	if err != nil {
		return Observation{Snapshot: o.Snapshot()}, err
	}
	return o.ObserveMessage(msg)
}

func (o *Observer) ObserveVote(msg VoteMessage) (Observation, error) {
	if err := msg.ValidateBasic(); err != nil {
		return Observation{Snapshot: o.Snapshot()}, err
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	key := msg.Key()
	_, exists := o.votes[key]
	if !exists {
		o.trackVoteLocked(key, msg)
		o.votesObserved++
		if msg.Vote.Slot > o.latestVoteSlot {
			o.latestVoteSlot = msg.Vote.Slot
		}
	}

	return Observation{New: !exists, Snapshot: o.snapshotLocked()}, nil
}

func (o *Observer) ObserveCertificate(cert Certificate) (Observation, error) {
	if err := cert.ValidateBasic(); err != nil {
		return Observation{Snapshot: o.Snapshot()}, err
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	key := cert.Key()
	_, exists := o.certificates[key]
	if !exists {
		o.pendingStatsValid = false
		tracked := o.trackCertificateLocked(key, cert)
		o.certificatesObserved++
		o.applyCertificateLocked(cert)
		if tracked {
			o.checkCertificateReplayLocked(key, cert)
		}
	}

	return Observation{New: !exists, Snapshot: o.snapshotLocked()}, nil
}

func (o *Observer) ObserveReplayBlock(obs ReplayBlockObservation) Observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pendingStatsValid = false

	if obs.At.IsZero() {
		obs.At = time.Now()
	}
	o.replayBlocksObserved++
	if o.trackReplayBlockLocked(obs.Block) {
		o.checkReplayBlockCertificatesLocked(obs.Block)
	}
	if obs.Block.Slot != 0 && (o.oldestReplayBlockSlot == 0 || obs.Block.Slot < o.oldestReplayBlockSlot) {
		o.oldestReplayBlockSlot = obs.Block.Slot
	}
	if obs.Block.Slot > o.latestReplayBlockSlot {
		o.latestReplayBlockSlot = obs.Block.Slot
		o.latestReplayBlock = obs.Block
	}

	return Observation{New: true, Snapshot: o.snapshotLocked()}
}

func (o *Observer) ObserveReplayResult(obs ReplayResultObservation) Observation {
	o.mu.Lock()
	defer o.mu.Unlock()

	if obs.At.IsZero() {
		obs.At = time.Now()
	}
	o.replayResultsObserved++
	if obs.Slot > o.latestReplayResultSlot {
		o.latestReplayResultSlot = obs.Slot
	}

	return Observation{New: true, Snapshot: o.snapshotLocked()}
}

func (o *Observer) Snapshot() Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snapshotLocked()
}

func (o *Observer) Certificate(key CertificateKey) (Certificate, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	cert, ok := o.certificates[key]
	return cert, ok
}

func (o *Observer) Vote(key VoteMessageKey) (VoteMessage, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vote, ok := o.votes[key]
	return vote, ok
}

func (o *Observer) applyCertificateLocked(cert Certificate) {
	if cert.Slot > o.latestCertificateSlot {
		o.latestCertificateSlot = cert.Slot
	}
	if cert.TotalStake != 0 {
		o.stakeCheckedCertificates++
	}
	if cert.SignatureVerified {
		o.signatureVerifiedCertificates++
	}

	switch cert.Type {
	case CertificateNotarize:
		if block, ok := cert.Block(); ok {
			o.latestNotarizedBlock = newerBlock(o.latestNotarizedBlock, block)
		}
	case CertificateNotarizeFallback:
		if block, ok := cert.Block(); ok {
			o.latestFallbackBlock = newerBlock(o.latestFallbackBlock, block)
		}
	case CertificateFinalizeFast:
		if block, ok := cert.Block(); ok {
			o.latestFastFinalizedBlock = newerBlock(o.latestFastFinalizedBlock, block)
		}
		if cert.Slot > o.latestFinalizedSlot {
			o.latestFinalizedSlot = cert.Slot
		}
	case CertificateFinalize:
		if cert.Slot > o.latestFinalizedSlot {
			o.latestFinalizedSlot = cert.Slot
		}
	case CertificateSkip:
		if cert.Slot > o.latestSkippedSlot {
			o.latestSkippedSlot = cert.Slot
		}
	case CertificateGenesis:
		if block, ok := cert.Block(); ok {
			o.genesisBlock = newerBlock(o.genesisBlock, block)
		}
	}
}

func (o *Observer) trackVoteLocked(key VoteMessageKey, msg VoteMessage) {
	if o.cfg.MaxTrackedVotes == 0 {
		return
	}
	o.votes[key] = msg
	o.voteOrder = append(o.voteOrder, key)
	for len(o.votes) > o.cfg.MaxTrackedVotes {
		old := o.voteOrder[0]
		o.voteOrder = o.voteOrder[1:]
		delete(o.votes, old)
	}
}

func (o *Observer) trackCertificateLocked(key CertificateKey, cert Certificate) bool {
	if o.cfg.MaxTrackedCertificates == 0 {
		return false
	}
	o.certificates[key] = cert
	o.certOrder = append(o.certOrder, key)
	for len(o.certificates) > o.cfg.MaxTrackedCertificates {
		old := o.certOrder[0]
		o.certOrder = o.certOrder[1:]
		delete(o.certificates, old)
		delete(o.replayChecks, old)
	}
	return true
}

func (o *Observer) trackReplayBlockLocked(block BlockID) bool {
	if o.cfg.MaxTrackedReplayBlocks == 0 || block.Slot == 0 {
		return false
	}
	if _, exists := o.replayBlocks[block.Slot]; !exists {
		o.replayOrder = append(o.replayOrder, block.Slot)
	}
	o.replayBlocks[block.Slot] = block
	for len(o.replayBlocks) > o.cfg.MaxTrackedReplayBlocks {
		old := o.replayOrder[0]
		o.replayOrder = o.replayOrder[1:]
		delete(o.replayBlocks, old)
	}
	return true
}

func (o *Observer) checkReplayBlockCertificatesLocked(block BlockID) {
	if !block.HasHash() {
		return
	}
	for key, cert := range o.certificates {
		certBlock, ok := cert.Block()
		if !ok || certBlock.Slot != block.Slot {
			continue
		}
		o.checkCertificateReplayLocked(key, cert)
	}
}

func (o *Observer) checkCertificateReplayLocked(key CertificateKey, cert Certificate) {
	if _, exists := o.replayChecks[key]; exists {
		return
	}
	certBlock, ok := cert.Block()
	if !ok || !certBlock.HasHash() {
		return
	}
	replayBlock, ok := o.replayBlocks[certBlock.Slot]
	if !ok || !replayBlock.HasHash() {
		return
	}

	check := certificateReplayCheck{
		certificateBlock: certBlock,
		replayBlock:      replayBlock,
	}
	if certBlock.Hash == replayBlock.Hash {
		check.status = certificateReplayMatch
		o.certificateReplayMatches++
		o.latestCertificateReplayMatch = newerBlock(o.latestCertificateReplayMatch, certBlock)
	} else {
		check.status = certificateReplayMismatch
		o.certificateReplayMismatches++
		o.latestCertificateReplayMiss = newerBlock(o.latestCertificateReplayMiss, certBlock)
		o.latestReplayMiss = newerBlock(o.latestReplayMiss, replayBlock)
		if o.certificateReplayMismatches <= 16 || o.certificateReplayMismatches%128 == 0 {
			mlog.Log.FileOnlyf("ALPENGLOW observer: certificate replay mismatch #%d: slot=%d cert=%s replay=%s",
				o.certificateReplayMismatches, certBlock.Slot, certBlock.Hash, replayBlock.Hash)
		}
	}
	o.replayChecks[key] = check
}

type certificateReplayPendingStats struct {
	count            uint64
	mature           uint64
	preWindow        uint64
	oldestSlot       uint64
	newestSlot       uint64
	matureOldestSlot uint64
}

func (o *Observer) certificateReplayPendingStatsLocked() certificateReplayPendingStats {
	var stats certificateReplayPendingStats
	for key, cert := range o.certificates {
		if _, checked := o.replayChecks[key]; checked {
			continue
		}
		certBlock, ok := cert.Block()
		if ok && certBlock.HasHash() {
			stats.count++
			if stats.oldestSlot == 0 || certBlock.Slot < stats.oldestSlot {
				stats.oldestSlot = certBlock.Slot
			}
			if certBlock.Slot > stats.newestSlot {
				stats.newestSlot = certBlock.Slot
			}
			if o.oldestReplayBlockSlot != 0 && certBlock.Slot < o.oldestReplayBlockSlot {
				stats.preWindow++
			}
			if o.oldestReplayBlockSlot != 0 &&
				o.latestReplayBlockSlot != 0 &&
				certBlock.Slot >= o.oldestReplayBlockSlot &&
				certBlock.Slot <= o.latestReplayBlockSlot {
				stats.mature++
				if stats.matureOldestSlot == 0 || certBlock.Slot < stats.matureOldestSlot {
					stats.matureOldestSlot = certBlock.Slot
				}
			}
		}
	}
	return stats
}

func (o *Observer) snapshotLocked() Snapshot {
	if !o.pendingStatsValid {
		o.pendingStats = o.certificateReplayPendingStatsLocked()
		o.pendingStatsValid = true
	}
	pending := o.pendingStats
	return Snapshot{
		VotesObserved:                      o.votesObserved,
		CertificatesObserved:               o.certificatesObserved,
		VotesRetained:                      uint64(len(o.votes)),
		CertificatesRetained:               uint64(len(o.certificates)),
		ReplayBlocksObserved:               o.replayBlocksObserved,
		ReplayBlocksRetained:               uint64(len(o.replayBlocks)),
		ReplayResultsObserved:              o.replayResultsObserved,
		CertificateReplayMatches:           o.certificateReplayMatches,
		CertificateReplayMismatches:        o.certificateReplayMismatches,
		CertificateReplayPending:           pending.count,
		CertificateReplayMaturePending:     pending.mature,
		CertificateReplayPreWindowPending:  pending.preWindow,
		CertificateReplayPendingOldestSlot: pending.oldestSlot,
		CertificateReplayPendingNewestSlot: pending.newestSlot,
		CertificateReplayMatureOldestSlot:  pending.matureOldestSlot,
		StakeCheckedCertificates:           o.stakeCheckedCertificates,
		SignatureVerifiedCertificates:      o.signatureVerifiedCertificates,
		LatestVoteSlot:                     o.latestVoteSlot,
		LatestCertificateSlot:              o.latestCertificateSlot,
		OldestReplayBlockSlot:              o.oldestReplayBlockSlot,
		LatestReplayBlockSlot:              o.latestReplayBlockSlot,
		LatestReplayBlock:                  o.latestReplayBlock,
		LatestReplayResultSlot:             o.latestReplayResultSlot,
		LatestCertificateReplayMatch:       o.latestCertificateReplayMatch,
		LatestCertificateReplayMiss:        o.latestCertificateReplayMiss,
		LatestReplayMiss:                   o.latestReplayMiss,
		LatestNotarizedBlock:               o.latestNotarizedBlock,
		LatestFallbackBlock:                o.latestFallbackBlock,
		LatestFastFinalizedBlock:           o.latestFastFinalizedBlock,
		LatestFinalizedSlot:                o.latestFinalizedSlot,
		LatestSkippedSlot:                  o.latestSkippedSlot,
		GenesisBlock:                       o.genesisBlock,
	}
}

func newerBlock(current, next BlockID) BlockID {
	if next.Slot >= current.Slot {
		return next
	}
	return current
}
