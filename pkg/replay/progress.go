package replay

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Progress retains one immutable observation from the replay loop. The loop
// is its only writer; readers never inspect a live SlotCtx or reset timings.
// Nil disables publication, including allocations on the per-block path.
type Progress struct {
	latest                    atomic.Pointer[ProgressSnapshot]
	source                    atomic.Pointer[blockstream.BlockSource]
	diagnostics               atomic.Pointer[replayDiagnostics]
	selectedSlots             atomic.Pointer[SelectedSlotSnapshot]
	localEpochReplayAnchor    localEpochReplayAnchor
	hasLocalEpochReplayAnchor bool
	networkDetails            bool                 // Startup-only; retained across replay attempts and source replacement.
	gossipObserver            func(*gossip.Client) // Startup-only; receives only the current source's client.
	gossipMu                  sync.Mutex           // Orders attachments against source replacement/reset.
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
	PaymentOutcomes     PaymentOutcomeSummary
	RewardHealth        RewardHealthSummary
	AccountFacts        AccountFacts
	HasAccountFacts     bool
	LocalEpochReplay    LocalEpochReplayBoundaries

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
	At           time.Time
	Slot         uint64
	Bankhash     solana.Hash
	BlockHeight  uint64
	Epoch        uint64
	SlotIndex    uint64
	SlotsInEpoch uint64
	// EpochSchedule is a scalar copy of this execution's configuration. It is
	// not separately sampled global state or a schedule-assignment assertion.
	EpochSchedule         sealevel.SysvarEpochSchedule
	EpochTargetTiming     EpochTargetTiming
	hasExecutionParent    bool
	executionParentSlot   uint64
	executionParentHash   solana.Hash
	Transactions          uint64
	ComputeUnits          uint64
	WorkCountersSaturated bool
	Execution             time.Duration
	Wait                  time.Duration
	// Certificates is an unrooted, successful carrier observation, not a score.
	Certificates    block.CertificateObservation
	HasCertificates bool
}

// LocalEpochReplayBoundary is one epoch whose local replay start or end was
// observed from successful execution. Zero dates are unknown, not slot zero.
type LocalEpochReplayBoundary struct {
	Epoch     uint64
	FirstSlot uint64
	SlotCount uint64
	StartedAt time.Time
	EndedAt   time.Time
}

// LocalEpochReplayBoundaries retains at most the latest two observed epochs.
// Count distinguishes no observation from a valid epoch and first slot zero.
type LocalEpochReplayBoundaries struct {
	Count uint8
	Rows  [2]LocalEpochReplayBoundary
}

type localEpochReplayAnchor struct {
	at        time.Time
	slot      uint64
	bankhash  solana.Hash
	epoch     uint64
	firstSlot uint64
	slotCount uint64
	lastSlot  uint64
	schedule  sealevel.SysvarEpochSchedule
}

// CheckedEpochSchedule returns a detached configuration only when it agrees
// with this completed bank's existing epoch/index/length fields. Its helpers
// may then be used to derive a different next-epoch length during warmup.
func (b CompletedBlock) CheckedEpochSchedule() (sealevel.SysvarEpochSchedule, bool) {
	s := b.EpochSchedule
	if !admissionScheduleValid(&s) {
		return sealevel.SysvarEpochSchedule{}, false
	}
	epoch, index := s.GetEpochAndSlotIndex(b.Slot)
	if epoch != b.Epoch || index != b.SlotIndex || index > b.Slot || s.SlotsInEpoch(epoch) != b.SlotsInEpoch {
		return sealevel.SysvarEpochSchedule{}, false
	}
	return s, true
}

func localEpochReplayAnchorFor(b CompletedBlock) (localEpochReplayAnchor, bool) {
	s, ok := b.CheckedEpochSchedule()
	if !ok || b.At.IsZero() || b.Bankhash.IsZero() || b.SlotsInEpoch == 0 || b.SlotIndex > b.Slot || b.SlotIndex >= b.SlotsInEpoch {
		return localEpochReplayAnchor{}, false
	}
	first := b.Slot - b.SlotIndex
	lastOffset := b.SlotsInEpoch - 1
	if lastOffset > math.MaxUint64-first {
		return localEpochReplayAnchor{}, false
	}
	return localEpochReplayAnchor{at: b.At.UTC(), slot: b.Slot, bankhash: b.Bankhash,
		epoch: b.Epoch, firstSlot: first, slotCount: b.SlotsInEpoch, lastSlot: first + lastOffset, schedule: s}, true
}

func sameLocalEpochReplayGeometry(a, b sealevel.SysvarEpochSchedule) bool {
	return a.SlotsPerEpoch == b.SlotsPerEpoch && a.Warmup == b.Warmup &&
		a.FirstNormalEpoch == b.FirstNormalEpoch && a.FirstNormalSlot == b.FirstNormalSlot
}

func sameLocalEpochReplayCompletion(a, b localEpochReplayAnchor) bool {
	return a.slot == b.slot && a.bankhash == b.bankhash && a.epoch == b.epoch && a.firstSlot == b.firstSlot &&
		a.slotCount == b.slotCount && a.schedule == b.schedule
}

func (s *LocalEpochReplayBoundaries) row(a localEpochReplayAnchor) (*LocalEpochReplayBoundary, bool) {
	if s.Count > uint8(len(s.Rows)) {
		return nil, false
	}
	for i := range int(s.Count) {
		row := &s.Rows[i]
		if row.Epoch == a.epoch {
			return row, row.FirstSlot == a.firstSlot && row.SlotCount == a.slotCount
		}
	}
	row := LocalEpochReplayBoundary{Epoch: a.epoch, FirstSlot: a.firstSlot, SlotCount: a.slotCount}
	switch s.Count {
	case 0:
		s.Rows[0], s.Count = row, 1
		return &s.Rows[0], true
	case 1:
		if a.epoch <= s.Rows[0].Epoch {
			return nil, false
		}
		s.Rows[1], s.Count = row, 2
		return &s.Rows[1], true
	case 2:
		if a.epoch <= s.Rows[1].Epoch {
			return nil, false
		}
		s.Rows[0], s.Rows[1] = s.Rows[1], row
		return &s.Rows[1], true
	default:
		return nil, false
	}
}

func (s *LocalEpochReplayBoundaries) latch(a localEpochReplayAnchor, at time.Time, start bool) bool {
	row, ok := s.row(a)
	if !ok {
		return false
	}
	if start {
		if row.StartedAt.IsZero() {
			row.StartedAt = at.UTC()
		}
	} else if row.EndedAt.IsZero() {
		row.EndedAt = at.UTC()
	}
	return true
}

func (p *Progress) resetLocalEpochReplay() {
	p.localEpochReplayAnchor = localEpochReplayAnchor{}
	p.hasLocalEpochReplayAnchor = false
}

func (p *Progress) observeLocalEpochReplay(block CompletedBlock, summary LocalEpochReplayBoundaries) LocalEpochReplayBoundaries {
	current, ok := localEpochReplayAnchorFor(block)
	if !ok {
		p.resetLocalEpochReplay()
		return summary
	}
	if p.hasLocalEpochReplayAnchor {
		previous := p.localEpochReplayAnchor
		if previous.slot == current.slot && previous.bankhash == current.bankhash {
			if sameLocalEpochReplayCompletion(previous, current) {
				return summary
			}
			p.resetLocalEpochReplay()
			return summary
		}
		if block.hasExecutionParent && block.executionParentSlot == previous.slot && block.executionParentHash == previous.bankhash &&
			previous.epoch != math.MaxUint64 && current.epoch == previous.epoch+1 && previous.lastSlot != math.MaxUint64 &&
			current.firstSlot == previous.lastSlot+1 && sameLocalEpochReplayGeometry(previous.schedule, current.schedule) {
			next := summary
			if next.latch(previous, previous.at, false) && next.latch(current, previous.at, true) {
				summary = next
			}
		}
	}
	if current.slot == current.lastSlot {
		next := summary
		if next.latch(current, current.at, false) {
			summary = next
		}
	}
	p.localEpochReplayAnchor, p.hasLocalEpochReplayAnchor = current, true
	return summary
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

// EnableNetworkDetails opts into owned socket reads before replay starts.
// Ordinary progress monitoring does not enable these operating-system reads.
func (p *Progress) EnableNetworkDetails() {
	if p != nil {
		p.networkDetails = true
	}
}

// SetGossipObserver configures an optional attachment callback before replay.
// The selected callback remains configured across attempts; nil is disabled.
// The callback must return promptly and must not reset or restart this Progress.
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
	p.selectedSlots.Store(nil)
	p.nextAssignment = NextKnownAssignment{}
	p.accountSampler.Reset()
	p.accountFacts, p.hasAccountFacts = AccountFacts{}, false
	p.resetLocalEpochReplay()
	p.gossipMu.Lock()
	p.source.Store(nil)
	p.gossipMu.Unlock()
	now := time.Now().UTC()
	s := &ProgressSnapshot{Attempt: previous.Attempt + 1, StartedAt: now, UpdatedAt: now, Phase: "starting", StartSlot: start}
	if resume != nil {
		s.HasResumeParent, s.ResumeParent = true, resume.ParentSlot
	}
	p.resetReplayDiagnostics(s)
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
	s.LocalEpochReplay = p.observeLocalEpochReplay(block, s.LocalEpochReplay)
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

func (p *Progress) settledOutcomes(durable uint64, certificates CertificateOutcomeSummary, leaders LeaderOutcomeSummary, slots SlotHistorySummary, activity ChainActivitySummary, epochs EpochOutcomeSummary, payments PaymentOutcomeSummary, health RewardHealthSummary) {
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
	s.PaymentOutcomes = payments
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
	p.resetLocalEpochReplay()
	s.AccountFacts, s.HasAccountFacts = AccountFacts{}, false
	s.LocalEpochReplay = LocalEpochReplayBoundaries{}
	s.HasCompleted, s.Completed, s.UpdatedAt = false, CompletedBlock{}, time.Now().UTC()
	p.resetReplayDiagnostics(&s)
	p.latest.Store(&s)
}
