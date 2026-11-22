package replay

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

// Progress retains one immutable observation from the replay loop. The loop
// is its only writer; readers never inspect a live SlotCtx or reset timings.
// Nil disables publication, including allocations on the per-block path.
type Progress struct {
	latest         atomic.Pointer[ProgressSnapshot]
	source         atomic.Pointer[blockstream.BlockSource]
	networkDetails bool
	gossipObserver func(*gossip.Client)
	gossipMu       sync.Mutex
	// nextAssignment is staged and read only by the replay owner.
	nextAssignment  NextKnownAssignment
	accountSampler  AccountFactsSampler
	accountFacts    AccountFacts
	hasAccountFacts bool
}

// ProgressSnapshot is the last replay-loop observation for one attempt.
type ProgressSnapshot struct {
	Attempt             uint64
	StartedAt           time.Time
	ReadyAt             time.Time
	UpdatedAt           time.Time
	Phase               string
	StartSlot           uint64
	HasResumeParent     bool
	ResumeParent        uint64
	ExecutedBlocks      uint64
	SkippedSlots        uint64
	Rewinds             uint64
	DurableSlot         uint64
	HasDurableSlot      bool
	HasCompleted        bool
	Completed           CompletedBlock
	CertificateOutcomes CertificateOutcomeSummary
	LeaderOutcomes      LeaderOutcomeSummary
	NextKnownAssignment NextKnownAssignment
	SlotHistory         SlotHistorySummary
	ChainActivity       ChainActivitySummary
	EpochOutcomes       EpochOutcomeSummary
	RewardHealth        RewardHealthSummary
	AccountFacts        AccountFacts
	HasAccountFacts     bool

	// ExecutedTransactions and ExecutedComputeUnits count successful local work,
	// including re-execution, not unique/canonical work. They reset each attempt.
	ExecutedTransactions uint64
	ExecutedComputeUnits uint64
	// WorkCountersSaturated is sticky when a source or accumulated total overflows.
	// Saturated totals cannot be used to calculate further rates.
	WorkCountersSaturated bool
	// completedAt preserves the source clock before public UTC normalization.
	completedAt time.Time
}

// CompletedBlock is successful local execution, not consensus finality or an
// assertion that this block remains on the selected fork after a later rewind.
type CompletedBlock struct {
	At                    time.Time
	Slot                  uint64
	Bankhash              solana.Hash
	BlockHeight           uint64
	Epoch                 uint64
	SlotIndex             uint64
	SlotsInEpoch          uint64
	Transactions          uint64
	ComputeUnits          uint64
	WorkCountersSaturated bool
	Execution             time.Duration
	Wait                  time.Duration
	// Certificates is an unrooted, successful carrier observation, not a score.
	Certificates    block.CertificateObservation
	HasCertificates bool
}

// Snapshot returns a value copy, or false before the first attempt.
func (p *Progress) Snapshot() (ProgressSnapshot, bool) {
	if p != nil {
		if s := p.latest.Load(); s != nil {
			return *s, true
		}
	}
	return ProgressSnapshot{}, false
}

// NetworkSnapshot reads the current source's bounded cache, never its receiver.
func (p *Progress) NetworkSnapshot() (blockstream.NetworkSnapshot, bool) {
	if p != nil {
		return p.source.Load().NetworkSnapshot()
	}
	return blockstream.NetworkSnapshot{}, false
}

// EnableNetworkDetails selects owner socket observations before replay starts.
func (p *Progress) EnableNetworkDetails() {
	if p != nil {
		p.networkDetails = true
	}
}

// SetGossipObserver selects a source attachment callback before replay starts.
func (p *Progress) SetGossipObserver(observer func(*gossip.Client)) {
	if p != nil {
		p.gossipObserver = observer
	}
}

func (p *Progress) observeSourceGossip(source *blockstream.BlockSource, client *gossip.Client) {
	if p == nil || source == nil || client == nil {
		return
	}
	p.gossipMu.Lock()
	defer p.gossipMu.Unlock()
	if p.source.Load() == source && p.gossipObserver != nil {
		p.gossipObserver(client)
	}
}

func (p *Progress) sourceStarted(source *blockstream.BlockSource) {
	if p == nil {
		return
	}
	if source != nil {
		source.EnableMonitoring()
		if p.gossipObserver != nil {
			source.SetGossipObserver(func(client *gossip.Client) { p.observeSourceGossip(source, client) })
		}
		if p.networkDetails {
			source.EnableSocketMemoryMonitoring()
		}
	}
	p.gossipMu.Lock()
	p.source.Store(source)
	p.gossipMu.Unlock()
}

func (p *Progress) begin(start uint64, resume *ResumeState) {
	if p == nil {
		return
	}
	previous, _ := p.Snapshot()
	p.nextAssignment = NextKnownAssignment{}
	p.accountSampler.Reset()
	p.accountFacts, p.hasAccountFacts = AccountFacts{}, false
	p.gossipMu.Lock()
	p.source.Store(nil)
	p.gossipMu.Unlock()
	now := time.Now().UTC()
	s := &ProgressSnapshot{Attempt: previous.Attempt + 1, StartedAt: now, UpdatedAt: now, Phase: "starting", StartSlot: start}
	if resume != nil {
		s.HasResumeParent, s.ResumeParent = true, resume.ParentSlot
	}
	p.latest.Store(s)
}

func (p *Progress) phase(phase string) {
	if p == nil {
		return
	}
	s, ok := p.Snapshot()
	if !ok {
		return
	}
	s.Phase, s.UpdatedAt = phase, time.Now().UTC()
	if phase == "running" {
		s.ReadyAt = s.UpdatedAt
	}
	p.latest.Store(&s)
}

func (p *Progress) completed(block CompletedBlock, durable uint64, hasDurable bool) {
	if p == nil {
		return
	}
	s, _ := p.Snapshot()
	s.completedAt = block.At
	block.At = block.At.UTC()
	s.Completed, s.HasCompleted, s.UpdatedAt, s.DurableSlot = block, true, block.At, durable
	s.NextKnownAssignment = p.nextAssignment
	s.AccountFacts, s.HasAccountFacts = p.accountFacts, p.hasAccountFacts
	if s.HasAccountFacts && s.AccountFacts.SampledAt.After(s.UpdatedAt) {
		s.UpdatedAt = s.AccountFacts.SampledAt
	}
	s.HasDurableSlot = hasDurable
	s.ExecutedBlocks++
	s.WorkCountersSaturated = s.WorkCountersSaturated || block.WorkCountersSaturated
	if block.Transactions > math.MaxUint64-s.ExecutedTransactions {
		s.ExecutedTransactions, s.WorkCountersSaturated = math.MaxUint64, true
	} else {
		s.ExecutedTransactions += block.Transactions
	}
	if block.ComputeUnits > math.MaxUint64-s.ExecutedComputeUnits {
		s.ExecutedComputeUnits, s.WorkCountersSaturated = math.MaxUint64, true
	} else {
		s.ExecutedComputeUnits += block.ComputeUnits
	}
	p.latest.Store(&s)
}

func (p *Progress) skipped(durable uint64, hasDurable bool) {
	if p == nil {
		return
	}
	s, _ := p.Snapshot()
	s.SkippedSlots++
	s.NextKnownAssignment = p.nextAssignment
	s.DurableSlot, s.UpdatedAt = durable, time.Now().UTC()
	s.HasDurableSlot = hasDurable
	p.latest.Store(&s)
}

func (p *Progress) certificateOutcomes(outcomes CertificateOutcomeSummary) {
	if p == nil {
		return
	}
	s, _ := p.Snapshot()
	s.CertificateOutcomes = outcomes
	p.latest.Store(&s)
}

func (p *Progress) settledOutcomes(durable uint64, certificates CertificateOutcomeSummary, leaders LeaderOutcomeSummary, slots SlotHistorySummary, activity ChainActivitySummary, epochs EpochOutcomeSummary, health RewardHealthSummary) {
	if p == nil {
		return
	}
	s, _ := p.Snapshot()
	// A successful fold may be the last event before waiting or shutdown.
	// Publish its frontier with the observations, without requiring another block.
	s.DurableSlot, s.HasDurableSlot, s.UpdatedAt = durable, true, time.Now().UTC()
	s.CertificateOutcomes, s.LeaderOutcomes = certificates, leaders
	s.SlotHistory = slots
	s.ChainActivity = activity
	s.EpochOutcomes = epochs
	s.RewardHealth = health
	p.latest.Store(&s)
}

func (p *Progress) rewound() {
	if p == nil {
		return
	}
	s, _ := p.Snapshot()
	s.Rewinds++
	p.nextAssignment, s.NextKnownAssignment = NextKnownAssignment{}, NextKnownAssignment{}
	p.accountSampler.Reset()
	p.accountFacts, p.hasAccountFacts = AccountFacts{}, false
	s.AccountFacts, s.HasAccountFacts = AccountFacts{}, false
	s.HasCompleted, s.Completed, s.UpdatedAt = false, CompletedBlock{}, time.Now().UTC()
	p.latest.Store(&s)
}
