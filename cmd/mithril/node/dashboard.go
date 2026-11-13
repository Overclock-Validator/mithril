package node

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/gagliardetto/solana-go"
)

type nodeDashboard struct {
	mu                    sync.Mutex
	state                 dashboard.Snapshot
	runStartedAt          time.Time // Original monotonic clock, never serialized.
	identity              *identity.Holder
	feed                  *dashboard.Feed
	accountsPath          string
	ledgerPath            string
	progress              replay.Progress
	heldSchedules         nodeHeldScheduleCache
	cache                 *cacheDashboardMonitor
	consensus             *consensus.AlpenglowObserverEngine
	rpc                   *rpcserver.RpcServer
	tpu                   *tpu.TPU
	scheduler             *scheduler.Scheduler
	pipelineStartedAt     time.Time
	hostSampler           *metrics.HostSampler // configured before the diagnostics owner starts
	cgroupSampler         *metrics.CgroupSampler
	machine               *machineDashboardMonitor
	machineGeneration     uint64
	machineRetired        bool
	networkDetails        bool
	localPeers            bool
	networkRetired        bool
	prewarmGeneration     uint64
	prewarmHandover       *prewarmHandoverMonitor
	interfaces            *interfaceTrafficMonitor
	gossipClient          *gossip.Client
	gossipGeneration      uint64
	tpuSocketGeneration   uint64
	tpuSocket             *tpuSocketOwner
	votorSocketGeneration uint64
	votorSocket           *votorSocketOwner
	snapshotAttempt       uint64
	snapshotRoot          *progress.DualProgress
	snapshotManifests     *nodeSnapshotManifests
	snapshotArchive       *nodeSnapshotArchive
}

// configureHostResources is startup-only; Sample is owned by diagnostics.
func (d *nodeDashboard) configureHostResources(enabled bool) {
	if d != nil {
		d.hostSampler = metrics.NewHostSampler(enabled)
		d.mu.Lock()
		d.interfaces.stop()
		d.interfaces = nil
		if enabled && d.networkDetails {
			d.interfaces = &interfaceTrafficMonitor{sampler: metrics.NewInterfaceTrafficSampler(true)}
		}
		d.mu.Unlock()
	}
}

func dashboardFeedConfig(enabled bool, socket, group, goos string) (string, string, error) {
	if !enabled {
		return socket, group, nil
	}
	if goos != "linux" {
		return "", "", fmt.Errorf("dashboard.enabled requires the provisioned Linux service")
	}
	if socket != "" || group != "" {
		return "", "", fmt.Errorf("dashboard.enabled cannot be combined with dashboard.socket or dashboard.socket_group")
	}
	return "/run/mithril-dashboard-feed/dashboard.sock", "mithril-dashboard-feed", nil
}

func validateDashboardHostResources(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return fmt.Errorf("dashboard.host_resources requires a dashboard feed")
	}
	return nil
}

func (d *nodeDashboard) configureMachineDetails(enabled bool) {
	if d == nil {
		return
	}
	d.mu.Lock()
	previous := d.machine
	d.machine = nil
	if !d.machineRetired {
		d.machine = newMachineDashboardMonitor(enabled)
	}
	d.mu.Unlock()
	previous.stop()
}

func (d *nodeDashboard) sampleMachineDetail(_ time.Time) (dashboard.MachineDetailCapture, error) {
	if d == nil {
		return (*machineDashboardMonitor)(nil).sample()
	}
	d.mu.Lock()
	m := d.machine
	d.mu.Unlock()
	return m.sample()
}

func (d *nodeDashboard) resolvedMachineWorker(name string, count int) {
	if d == nil {
		return
	}
	d.mu.Lock()
	m := d.machine
	d.mu.Unlock()
	m.resolvedWorker(name, count)
}

func validateDashboardMachineDetails(enabled bool, socket string, hostResources bool) error {
	if enabled && (strings.TrimSpace(socket) == "" || !hostResources) {
		return fmt.Errorf("dashboard.machine_details requires dashboard.socket and dashboard.host_resources")
	}
	return nil
}

func (d *nodeDashboard) storagePaths(accounts, ledger string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.accountsPath, d.ledgerPath = accounts, ledger
	d.mu.Unlock()
}

// observeRuntime runs in the existing diagnostics goroutine, not replay or the
// feed handler. All published objects are immutable after this assignment.
func (d *nodeDashboard) observeRuntime(now time.Time, mem runtime.MemStats, pressure *metrics.RuntimeWindow) {
	if d == nil {
		return
	}
	now = now.UTC()
	d.mu.Lock()
	accounts, ledger := d.accountsPath, d.ledgerPath
	traffic := d.interfaces
	machine := d.machine
	var machineBinding metrics.RSSHistoryBinding
	var machineGeneration uint64
	var machineGenesis string
	if machine != nil && !d.machineRetired {
		machineBinding = metrics.RSSHistoryBinding{RunID: d.state.RunID, StartedAt: d.runStartedAt}
		if d.state.GenesisHash != nil {
			machineGenesis = *d.state.GenesisHash
		}
		if d.machineGeneration < ^uint64(0) {
			d.machineGeneration++
		}
		machineGeneration = d.machineGeneration
	}
	trafficBinding := dashboard.NodeNetworkBinding{}
	if traffic != nil {
		trafficBinding = dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
		if d.state.GenesisHash != nil {
			value := *d.state.GenesisHash
			trafficBinding.GenesisHash = &value
		}
	}
	var previous *dashboard.RuntimePressure
	if d.state.Runtime != nil {
		previous = d.state.Runtime.Pressure
	}
	d.mu.Unlock()
	if machineGenesis != "" {
		if genesis, err := solana.HashFromBase58(machineGenesis); err == nil {
			machineBinding.GenesisHash = [32]byte(genesis)
		}
	}
	r := &dashboard.Runtime{
		SampledAt: now, HeapBytes: mem.HeapAlloc, GoBytes: mem.Sys - mem.HeapReleased,
		NextGCBytes: mem.NextGC, GCCount: uint64(mem.NumGC), MemoryLimitBytes: debug.SetMemoryLimit(-1),
		Goroutines: runtime.NumGoroutine(), Pressure: previous,
	}
	if pressure != nil {
		r.Pressure = &dashboard.RuntimePressure{
			SampledAt: now, WindowSeconds: pressure.Seconds, SchedulerP99Seconds: pressure.SchedLatencyP99.Seconds(),
			SchedulerMaxSeconds: pressure.SchedLatencyMax.Seconds(), GCCPUFraction: pressure.GCCPUFraction,
			GCPauseMaxSeconds: pressure.GCPauseMax.Seconds(), AllocBytesPerSecond: pressure.AllocBytesPerSec,
			MutexWaitSecondsPerSecond: pressure.MutexWaitPerSec,
		}
	}
	resources := &dashboard.Resources{SampledAt: now, Root: filesystemSample("/"), Accounts: filesystemSample(accounts), Ledger: filesystemSample(ledger)}
	var rssAt time.Time
	if machine != nil {
		rssAt = time.Now()
	}
	rss := replay.ProcessRSSBytes()
	if rss != 0 {
		resources.RSSBytes = &rss
	}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		seconds := float64(usage.Utime.Sec) + float64(usage.Stime.Sec) + (float64(usage.Utime.Usec)+float64(usage.Stime.Usec))/1e6
		resources.CPUSeconds = &seconds
	}
	if observation, supported := d.hostSampler.Sample(); supported {
		resources.Host = hostResourcesMeasurement(observation)
	}
	resources.Cgroup = sampleCgroupResources(d.cgroupSampler)
	d.mu.Lock()
	d.state.Runtime, d.state.Resources = r, resources
	d.mu.Unlock()
	if machine != nil {
		machine.observe(machineBinding, machineGeneration, metrics.RSSHistoryPoint{At: rssAt, Bytes: rss, Known: rss != 0}, uint32(runtime.GOMAXPROCS(0)))
	}
	traffic.observe(trafficBinding)
	d.observeTPUSocket()
	d.observeVotorSocket()
	d.cache.observe()
}

func hostResourcesMeasurement(o metrics.HostObservation) *dashboard.HostResources {
	if o.StartedAt.IsZero() || o.SampledAt.IsZero() {
		return nil
	}
	seconds := o.SampledAt.Sub(o.StartedAt).Seconds()
	if seconds < 0 {
		return nil
	}
	h := &dashboard.HostResources{SampledAt: o.SampledAt.UTC(), CaptureStartedAt: o.StartedAt.UTC(), CaptureDurationSeconds: seconds}
	if seconds > 1 {
		return h
	}
	if o.HasCPU {
		c := o.CPU
		h.CPU = &dashboard.HostCPU{ObservedAt: c.ObservedAt.UTC(), OnlineCPUs: c.OnlineCPUs}
		if c.HasRate {
			w := c.Rate
			h.CPU.Window = &dashboard.HostCPUWindow{WindowStart: w.StartedAt.UTC(), WindowEnd: w.EndedAt.UTC(),
				DurationSeconds: w.Seconds, Percent: w.Percent, BusyPercent: w.BusyPercent}
		}
	}
	if o.HasLoad {
		l := o.Load
		h.Load = &dashboard.HostLoad{One: l.One, Five: l.Five, Fifteen: l.Fifteen, Runnable: l.Runnable, Tasks: l.Tasks}
	}
	if o.HasMemory {
		m := o.Memory
		h.Memory = &dashboard.HostMemory{TotalBytes: m.TotalBytes, FreeBytes: m.FreeBytes}
		for _, field := range []struct {
			known  bool
			value  uint64
			target **uint64
		}{
			{m.HasAvailable, m.AvailableBytes, &h.Memory.AvailableBytes}, {m.HasBuffers, m.BuffersBytes, &h.Memory.BuffersBytes},
			{m.HasCached, m.CachedBytes, &h.Memory.CachedBytes}, {m.HasReclaimable, m.ReclaimableBytes, &h.Memory.ReclaimableBytes},
			{m.HasSwapTotal, m.SwapTotalBytes, &h.Memory.SwapTotalBytes}, {m.HasSwapFree, m.SwapFreeBytes, &h.Memory.SwapFreeBytes},
			{m.HasDirty, m.DirtyBytes, &h.Memory.DirtyBytes}, {m.HasWriteback, m.WritebackBytes, &h.Memory.WritebackBytes},
		} {
			if field.known {
				value := field.value
				*field.target = &value
			}
		}
	}
	return h
}

func filesystemSample(path string) *dashboard.Filesystem {
	if path == "" {
		return nil
	}
	var stat syscall.Statfs_t
	if syscall.Statfs(path, &stat) != nil || stat.Bsize <= 0 {
		return nil
	}
	return filesystemFromStat(stat)
}

func filesystemFromStat(stat syscall.Statfs_t) *dashboard.Filesystem {
	unit := filesystemBlockSize(stat)
	if unit == 0 || stat.Blocks > ^uint64(0)/unit || stat.Bavail > ^uint64(0)/unit {
		return nil
	}
	return &dashboard.Filesystem{TotalBytes: stat.Blocks * unit, AvailableBytes: stat.Bavail * unit, Inodes: stat.Files, FreeInodes: stat.Ffree}
}

func startNodeDashboard(ctx context.Context, startedAt time.Time, socket, group, runID, network, mode string, alpenglow, rpcConfigured, hostResources, networkDetails, machineDetails, localPeers, replayDetails, cacheDetails bool) *nodeDashboard {
	if socket == "" {
		return nil
	}
	if err := validateDashboardLocalPeers(localPeers, socket, networkDetails); err != nil {
		mlog.Log.Warnf("dashboard local peers unavailable: %v", err)
		return nil
	}
	d := &nodeDashboard{runStartedAt: startedAt, networkDetails: networkDetails, localPeers: localPeers, state: dashboard.Snapshot{
		RunID: runID, StartedAt: startedAt.UTC(), Version: version.Semver(), Commit: version.Commit(),
		Network: network, Mode: mode, Alpenglow: alpenglow, RPCConfigured: rpcConfigured, Phase: "starting",
	}}
	d.configureHostResources(hostResources)
	d.configureMachineDetails(machineDetails)
	var feed *dashboard.Feed
	var err error
	var details dashboard.DetailSamplers
	if cacheDetails {
		d.cache = &cacheDashboardMonitor{}
		details.Cache = d.sampleCacheDetail
	}
	if replayDetails {
		if err := d.progress.EnableReplayDiagnostics(runID); err != nil {
			mlog.Log.Warnf("dashboard replay diagnostics unavailable: %v", err)
			return nil
		}
		details.Replay = d.sampleReplayDetail
		details.SelectedWindow = d.sampleSelectedWindow
		details.RootedHistory = d.sampleRootedHistory
		details.HeldSchedules = d.sampleHeldSchedules
	}
	if machineDetails {
		details.Machine = d.sampleMachineDetail
	}
	if networkDetails {
		d.progress.EnableNetworkDetails()
		d.progress.SetGossipObserver(d.observeGossipClient)
		details.NodeNetwork = d.sampleNodeNetwork
	}
	if localPeers {
		details.LocalPeers = d.sampleLocalPeers
	}
	if networkDetails || machineDetails || replayDetails || cacheDetails {
		feed, err = dashboard.ListenFeedWithDetails(ctx, socket, group, d.snapshot, details)
	} else if group == "" {
		feed, err = dashboard.ListenFeed(ctx, socket, d.snapshot)
	} else {
		feed, err = dashboard.ListenFeedGroup(ctx, socket, group, d.snapshot)
	}
	if err != nil {
		mlog.Log.Warnf("dashboard feed unavailable: %v", err)
		return nil
	}
	d.feed = feed
	return d
}

func (d *nodeDashboard) snapshot() dashboard.Snapshot {
	d.mu.Lock()
	s := d.state
	s.SnapshotArchive = nil
	s.IncrementalArchive = nil
	s.SnapshotManifests = nil
	if m := d.snapshotManifests; m != nil && m.root == d.snapshotRoot && m.runID == s.RunID &&
		m.nodeStarted.Equal(s.StartedAt) && s.GenesisHash != nil && *s.GenesisHash == m.genesis {
		s.SnapshotManifests = &m.value
	}
	if a := d.snapshotArchive; a != nil && a.attempt == d.snapshotAttempt && a.runID == s.RunID &&
		a.nodeStarted.Equal(s.StartedAt) && s.GenesisHash != nil && *s.GenesisHash == a.genesis {
		if a.incrementalSlot == nil {
			s.SnapshotArchive = a.value
		} else {
			s.IncrementalArchive = &dashboard.IncrementalArchive{SnapshotArchive: *a.value, IncrementalSlot: *a.incrementalSlot}
		}
	}
	holder := d.identity
	engine := d.consensus
	rpc := d.rpc
	tpu, scheduler, pipelineStartedAt := d.tpu, d.scheduler, d.pipelineStartedAt
	d.mu.Unlock()
	s.ElapsedNanoseconds = sampledRunElapsed(d.runStartedAt, time.Now())
	if holder != nil {
		key := holder.PublicKey().String()
		s.IdentityKey = &key
	}
	if s.Phase == "replaying" {
		// Replay publishes this atomic frontier after successful execution or
		// a known skip. Zero before replay initialization is unavailable here.
		if slot := global.ReplayFrontier(); slot != 0 {
			s.ReplayFrontier = &slot
		}
	}
	if p, ok := d.progress.Snapshot(); ok {
		s.Replay = replayMeasurement(p)
		if a := s.Replay.NextKnownAssignment; a != nil && (s.IdentityKey == nil || a.Identity != *s.IdentityKey) {
			s.Replay.NextKnownAssignment = nil
		}
	}
	if engine != nil {
		s.Consensus = consensusMeasurement(engine.MonitoringSnapshot(), time.Now().UTC())
	}
	if n, ok := d.progress.NetworkSnapshot(); ok {
		s.BlockSource = blockSourceMeasurement(n)
	}
	if rpc != nil {
		s.RPC = rpcMeasurement(rpc.MonitoringSnapshot(), time.Now().UTC())
	}
	if tpu != nil && scheduler != nil {
		stats, scheduled, queued := tpu.Stats(), scheduler.Stats(), scheduler.Buffered()
		captured := time.Now()
		s.Pipeline = pipelineMeasurement(captured, pipelineStartedAt, stats, scheduled, queued)
	}
	return s
}

func rpcMeasurement(r rpcserver.MonitoringSnapshot, now time.Time) *dashboard.RPC {
	m := &dashboard.RPC{SampledAt: now.UTC(), MonitoringStartedAt: r.MonitoringStartedAt.UTC(),
		Serving: r.Serving, HistoryRecoveryPending: r.HistoryRecoveryPending}
	if r.HasPublishedSlot {
		at := r.PublishedAt.UTC()
		m.PublishedSlot, m.PublishedAt = &r.PublishedSlot, &at
	}
	for _, method := range r.Methods {
		row := dashboard.RPCMethod{Name: method.Name, Completed: method.Completed, Errors: method.Errors, Active: method.Active}
		for i, count := range method.DurationBuckets {
			row.DurationBuckets[i] = strconv.FormatUint(count, 10)
		}
		m.Methods = append(m.Methods, row)
	}
	return m
}

func replayMeasurement(p replay.ProgressSnapshot) *dashboard.Replay {
	r := &dashboard.Replay{
		Attempt: p.Attempt, StartedAt: p.StartedAt, SampledAt: p.UpdatedAt, Phase: p.Phase, StartSlot: p.StartSlot,
		ExecutedBlocks: p.ExecutedBlocks, SkippedSlots: p.SkippedSlots, Rewinds: p.Rewinds,
		ExecutedTransactions: p.ExecutedTransactions, ExecutedComputeUnits: p.ExecutedComputeUnits,
		WorkCountersSaturated: p.WorkCountersSaturated,
	}
	if p.HasDurableSlot {
		r.DurableSlot = &p.DurableSlot
	}
	if c := p.CertificateOutcomes; !c.SampledAt.IsZero() && !c.VoteAccount.IsZero() {
		r.CertificateOutcomes = &dashboard.CertificateOutcomes{
			SampledAt: c.SampledAt, Provenance: c.Provenance, VoteAccount: c.VoteAccount.String(),
			CoverageIncomplete: c.CoverageIncomplete, HasWindow: c.HasWindow, FirstCarrier: c.FirstCarrier, LastCarrier: c.LastCarrier,
			Carriers: c.Carriers, Gaps: c.Gaps, SlotsWithoutCarrier: c.SlotsWithoutCarrier, Pending: c.Pending, Dropped: c.Dropped,
			RewardIncluded: c.RewardIncluded, RewardExcluded: c.RewardExcluded, RewardUnknown: c.RewardUnknown, RewardIneligible: c.RewardIneligible,
			FinalFast: c.FinalFast, FinalSlow: c.FinalSlow, FastMember: c.FastMember, FastNonmember: c.FastNonmember,
			FastMembershipUnknown: c.FastMembershipUnknown, FinalDuplicates: c.FinalDuplicates, FinalVariants: c.FinalVariants, FinalConflict: c.FinalConflict,
		}
	}
	if l := p.LeaderOutcomes; !l.SampledAt.IsZero() {
		r.LeaderOutcomes = &dashboard.LeaderOutcomes{SampledAt: l.SampledAt, Provenance: l.Provenance, Identity: l.Identity.String(),
			HasWindow: l.HasWindow, WindowStart: l.WindowStart, WindowEnd: l.WindowEnd,
			HasCoveredRange: l.HasCoveredRange, CoveredStart: l.CoveredStart, CoveredEnd: l.CoveredEnd,
			Produced: l.Produced, SelectedForkSkipped: l.SelectedForkSkipped, ScheduledUnknown: l.ScheduledUnknown,
			UnknownScheduleSlots: l.UnknownScheduleSlots, UnknownAncestrySlots: l.UnknownAncestrySlots, CoverageIncomplete: l.CoverageIncomplete}
	}
	if h := p.SlotHistory; !h.SampledAt.IsZero() {
		r.SlotHistory = &dashboard.SlotHistory{SampledAt: h.SampledAt, WindowStart: h.WindowStart, WindowEnd: h.WindowEnd,
			Outcomes: h.Outcomes, CoverageIncomplete: h.CoverageIncomplete}
	}
	if a := p.ChainActivity; !a.SampledAt.IsZero() {
		r.ChainActivity = &dashboard.ChainActivity{SampledAt: a.SampledAt, Available: a.Available, AnchorSlot: a.AnchorSlot, LastSlot: a.LastSlot,
			StartTime: a.StartTime, EndTime: a.EndTime, Blocks: a.Blocks, Transactions: a.Transactions.Total, NonVoteTransactions: a.Transactions.NonVote,
			FailedTransactions: a.Transactions.Failed, FailedNonVote: a.Transactions.NonVoteFailed, ComputeUnits: a.Transactions.ComputeUnits}
	}
	if e := p.EpochOutcomes; !e.SampledAt.IsZero() && e.Count > 0 && e.Count <= len(e.Epochs) {
		r.EpochOutcomes = &dashboard.EpochOutcomes{SampledAt: e.SampledAt, Identity: e.Identity.String(), VoteAccount: e.VoteAccount.String(), Through: e.Through, Epochs: make([]dashboard.EpochOutcome, e.Count)}
		for i := 0; i < e.Count; i++ {
			row := e.Epochs[i]
			out := &r.EpochOutcomes.Epochs[i]
			out.Epoch, out.FirstSlot, out.LastSlot = row.Epoch, row.FirstSlot, row.LastSlot
			if row.HasLeaders {
				out.Leaders = &dashboard.EpochLeaderWindow{First: row.LeaderFirst, Last: row.LeaderLast, Produced: row.Produced, Skipped: row.Skipped, ScheduledUnknown: row.ScheduledUnknown, UnknownSchedule: row.UnknownSchedule, UnknownAncestry: row.UnknownAncestry, CompleteEpoch: row.LeaderEpochComplete}
			}
			if row.HasRewards {
				out.Rewards = &dashboard.EpochRewardWindow{First: row.RewardFirst, Last: row.RewardLast, Included: row.Included, Excluded: row.Excluded, Ineligible: row.Ineligible, Unknown: row.Unknown, Unobserved: row.Unobserved, CompleteEpoch: row.RewardEpochComplete}
			}
		}
	}
	if a := p.NextKnownAssignment; !a.SampledAt.IsZero() {
		r.NextKnownAssignment = &dashboard.NextKnownAssignment{SampledAt: a.SampledAt, Identity: a.Identity.String(),
			ReferenceSlot: a.ReferenceSlot, SearchedThrough: a.SearchedThrough}
		if a.HasNextKnownSlot {
			slot := a.NextKnownSlot
			r.NextKnownAssignment.NextKnownSlot = &slot
		}
	}
	if !p.ReadyAt.IsZero() {
		r.ReadyAt = &p.ReadyAt
	}
	if h := p.RewardHealth; !h.SampledAt.IsZero() {
		r.RewardHealth = &dashboard.RewardHealth{SampledAt: h.SampledAt.UTC(), Identity: h.Identity.String(), VoteAccount: h.VoteAccount.String(),
			Through: h.Through, Included: h.Included, Excluded: h.Excluded, Ineligible: h.Ineligible, Unknown: h.Unknown,
			Unobserved: h.Unobserved, CoverageComplete: h.CoverageComplete, Health: string(h.Health)}
		if h.HasWindow {
			first, last := h.WindowFirst, h.WindowLast
			r.RewardHealth.WindowFirst, r.RewardHealth.WindowLast = &first, &last
		}
		if h.HasLatestResolved {
			r.RewardHealth.LatestResolved = &dashboard.RewardTarget{Slot: h.LatestResolved.TargetSlot, Epoch: h.LatestResolved.TargetEpoch, Outcome: string(h.LatestResolved.Outcome)}
		}
		if h.HasLatestIncluded {
			r.RewardHealth.LatestIncluded = &dashboard.RewardTarget{Slot: h.LatestIncluded.TargetSlot, Epoch: h.LatestIncluded.TargetEpoch, Outcome: string(h.LatestIncluded.Outcome)}
		}
	}
	if p.HasResumeParent {
		r.ResumeParent = &p.ResumeParent
	}
	if p.HasCompleted {
		b := p.Completed
		r.Completed = &dashboard.CompletedBlock{
			At: b.At, Slot: b.Slot, Bankhash: b.Bankhash.String(), BlockHeight: b.BlockHeight,
			Epoch: b.Epoch, SlotIndex: b.SlotIndex, SlotsInEpoch: b.SlotsInEpoch, Transactions: b.Transactions,
			ComputeUnits: b.ComputeUnits, ExecutionSeconds: b.Execution.Seconds(), WaitSeconds: b.Wait.Seconds(),
		}
	}
	if p.HasAccountFacts {
		r.AccountFacts = accountFactsMeasurement(p.AccountFacts)
	}
	return r
}

func accountFactsMeasurement(f replay.AccountFacts) *dashboard.AccountFacts {
	if f.SampledAt.IsZero() || f.Identity.IsZero() {
		return nil
	}
	m := &dashboard.AccountFacts{SampledAt: f.SampledAt.UTC(), Slot: f.Slot, Bankhash: f.Bankhash.String(), Epoch: f.Epoch, Identity: f.Identity.String()}
	if f.HasIdentityBalance {
		m.IdentityBalanceLamports = &f.IdentityLamports
	}
	if !f.VoteAccount.IsZero() {
		vote := f.VoteAccount.String()
		m.VoteAccount = &vote
		m.StoredCommissions = &dashboard.StoredCommissions{}
		if f.HasInflationRewardsCommission {
			m.StoredCommissions.InflationRewardsBPS = &f.InflationRewardsCommissionBPS
		}
		if f.HasBlockRevenueCommission {
			m.StoredCommissions.BlockRevenueBPS = &f.BlockRevenueCommissionBPS
		}
		if f.HasVoteBalance {
			m.VoteBalanceLamports = &f.VoteLamports
		}
		m.StoredVoteHeader = &dashboard.StoredVoteHeader{}
		if f.HasStoredNodeIdentity {
			key := f.StoredNodeIdentity.String()
			m.StoredVoteHeader.NodeIdentity = &key
		}
		if f.HasPendingDelegatorRewards {
			m.StoredVoteHeader.PendingDelegatorRewards = &f.PendingDelegatorRewards
		}
		if f.HasStoredBLSRegistration {
			m.StoredVoteHeader.BLSRegistrationPresent = &f.StoredBLSRegistrationPresent
		}
		if r := f.AdmissionFunding; r.HasRules {
			m.FundingRules = &dashboard.FundingRules{ReferenceSlot: f.Slot, VATFilteringActive: r.VATFilteringActive,
				VATChargeGateActive: r.VATChargeGateActive, RentMinimumLamports: r.RentMinimumLamports,
				BurnLamports: r.BurnLamports, FloorLamports: r.FloorLamports}
			if r.HasFundingComparison && f.HasVoteBalance {
				var shortfall, surplus uint64
				if f.VoteLamports >= r.FloorLamports {
					surplus = f.VoteLamports - r.FloorLamports
				} else {
					shortfall = r.FloorLamports - f.VoteLamports
				}
				m.FundingRules.MeetsFloor, m.FundingRules.ShortfallLamports, m.FundingRules.SurplusLamports = &r.MeetsFloor, &shortfall, &surplus
			}
		}
		if e := f.AdmissionEpochs; e.HasEpochs {
			m.EpochStakeSets = &dashboard.EpochStakeSets{Provenance: dashboard.EpochStakeSetsProvenance, Current: epochStakeMeasurement(e.Current)}
			if e.HasNext {
				next := epochStakeMeasurement(e.Next)
				m.EpochStakeSets.Next = &next
			}
			if e.HasPayerEpoch {
				m.EpochStakeSets.Payer = &dashboard.EpochPayerReference{ReferenceSlot: e.PayerReferenceSlot, Epoch: e.PayerEpoch}
				if e.HasPayerMembership {
					m.EpochStakeSets.Payer.Member = &e.PayerMember
				}
			}
		}
	}
	return m
}

func epochStakeMeasurement(f replay.EpochStakeFacts) dashboard.EpochStakeSet {
	m := dashboard.EpochStakeSet{Epoch: f.Epoch, Generation: f.Generation, MaterialKnown: f.HasMaterial}
	if f.HasMaterial {
		m.Member, m.SelectedStakeLamports, m.TotalStakeLamports = &f.Member, &f.SelectedStake, &f.TotalStake
		if f.HasMetadata {
			key := f.MetadataNodeIdentity.String()
			m.MetadataNodeIdentity, m.NodeMatchesIdentity, m.SelectionBalanceLamports = &key, &f.NodeMatchesIdentity, &f.SelectionLamports
		}
	}
	return m
}

func blockSourceMeasurement(s blockstream.NetworkSnapshot) *dashboard.BlockSource {
	m := &dashboard.BlockSource{SampledAt: s.SampledAt.UTC(), StartedAt: s.SourceStartedAt.UTC(),
		ConfiguredSource: s.ConfiguredSource, NextSlot: s.NextSlot, LiveConnected: s.LiveConnected,
		LiveActive: s.LiveActive, RPCBlockFetchAllowed: s.RPCBlockFetchAllowed, RepairCatchupPending: s.RepairCatchupPending}
	if s.RepairCatchupFrom != 0 {
		from, until := s.RepairCatchupFrom, s.RepairCatchupUntil
		m.RepairCatchupFrom, m.RepairCatchupUntil = &from, &until
	}
	if t := s.Turbine; t != nil {
		m.Turbine = &dashboard.Turbine{SampledAt: t.SampledAt.UTC(), StartedAt: t.StartedAt.UTC(), Session: t.Session,
			Packets: t.Packets, DataShreds: t.DataShreds, CodingShreds: t.CodingShreds,
			ParseErrors: t.ParseErrors, SignatureErrors: t.SignatureErrors,
			UsefulRepairShreds: t.UsefulRepairShreds, SpooledRepairShreds: t.SpooledRepairShreds}
		m.Turbine.Diagnostics = &dashboard.TurbineDiagnostics{BlockEmissionAttempts: t.BlocksEmitted,
			RecoveredDataShreds: t.RecoveredData, AssemblyErrors: t.AssemblyErrors, ActiveSlots: t.ActiveSlots,
			EvictedSlots: t.EvictedSlots, IgnoredOldShreds: t.IgnoredOldShreds,
			SignatureVerifications: t.SigVerifies, SignatureCacheHits: t.SigVerifyCached,
			RepairOutstanding: t.RepairOutstanding, RepairPeers: t.RepairPeers, RepairRespondingPeers: t.RepairResponding}
		m.Turbine.Diagnostics.Admission = &dashboard.TurbineAdmission{MissingLeaders: t.MissingLeaders, ShredVersionMismatch: t.ShredVersionMismatch}
		if t.RetransmitEnabled {
			bytes := t.PeerPayloadBytes
			m.Turbine.Diagnostics.PeerPayloadBytes = &bytes
		}
		if t.Frontiers.HasObserved {
			slot := t.Frontiers.ObservedSlot
			m.Turbine.Diagnostics.ObservedHeaderSlot = &slot
		}
		if t.Frontiers.HasReconstructed {
			slot := t.Frontiers.ReconstructedSlot
			m.Turbine.Diagnostics.ReconstructedSlot = &slot
		}
		if !t.LastPacketAt.IsZero() {
			at := t.LastPacketAt.UTC()
			m.Turbine.LastPacketAt = &at
		}
		if t.RepairEnabled {
			m.Turbine.Repair = &dashboard.Repair{Requests: t.RepairRequests, Responses: t.RepairResponses,
				LateResponses: t.RepairLateResponses, Timeouts: t.RepairTimeouts, Errors: t.RepairErrors}
		}
	}
	if r := s.Reception; r != nil {
		p := r.Progress
		m.Reception = &dashboard.Reception{SampledAt: p.SampledAt.UTC(), StartedAt: r.StartedAt.UTC(), Session: r.Session,
			WindowMilliseconds: r.WindowMilliseconds, CoverageLimited: p.CoverageLimited, RetainedSlots: p.RetainedSlots}
		if p.HasSlot {
			slot, at := p.Slot, p.ObservedAt.UTC()
			m.Reception.Slot, m.Reception.ObservedAt = &slot, &at
		}
	}
	if g := s.Gossip; g != nil {
		m.Gossip = &dashboard.Gossip{SampledAt: g.SampledAt.UTC(), StartedAt: g.StartedAt.UTC(),
			ReceivedPackets: g.Stats.RxPackets, SentPackets: g.Stats.TxPackets,
			DecodeErrors: g.Stats.RxDecodeErrors, SendErrors: g.Stats.TxErrors, AcceptedContacts: g.Stats.AcceptedContacts,
			KnownPeers: g.Stats.Peers, RepairPeers: g.Stats.RepairPeers, AlpenglowPeers: g.Stats.AlpenglowPeers}
		if g.Stats.LastRxUnix > 0 {
			at := time.Unix(g.Stats.LastRxUnix, 0).UTC()
			m.Gossip.LastReceivedAt = &at
		}
		if g.Stats.LastTxUnix > 0 {
			at := time.Unix(g.Stats.LastTxUnix, 0).UTC()
			m.Gossip.LastSentAt = &at
		}
		for i := 0; i < g.PeerCount && i < len(g.Peers); i++ {
			p := g.Peers[i]
			// Agave v4 packs prerelease tags in minor and their number in patch.
			// The dashboard contract exposes only the semantic base version.
			minor, patch := p.Version.Minor&0x3fff, p.Version.Patch
			if p.Version.Minor>>14 != 0 {
				patch = 0
			}
			m.Gossip.RecentPeers = append(m.Gossip.RecentPeers, dashboard.Peer{
				Identity: solana.PublicKey(p.Pubkey).String(),
				Version:  strconv.Itoa(int(p.Version.Major)) + "." + strconv.Itoa(int(minor)) + "." + strconv.Itoa(int(patch)),
				Client:   p.Version.Client, ShredVersion: p.ShredVersion, ReceivedAt: p.ReceivedAt.UTC(),
				Repair: p.Repair, TVU: p.TVU, Alpenglow: p.Alpenglow,
			})
		}
	}
	return m
}

func (d *nodeDashboard) pipelineStarted(service *tpu.TPU, scheduler *scheduler.Scheduler) {
	if d == nil {
		return
	}
	d.mu.Lock()
	if service != d.tpu {
		d.tpuSocket = nil
		if d.networkDetails && !d.networkRetired && service != nil && d.tpuSocketGeneration != ^uint64(0) {
			d.tpuSocketGeneration++
			d.tpuSocket = &tpuSocketOwner{service: service, generation: d.tpuSocketGeneration}
		}
	}
	d.tpu, d.scheduler, d.pipelineStartedAt = service, scheduler, time.Now().UTC()
	d.mu.Unlock()
}

func (d *nodeDashboard) pipelineStopped() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.tpuSocket = nil
	d.tpu, d.scheduler, d.pipelineStartedAt = nil, nil, time.Time{}
	d.mu.Unlock()
}

func (d *nodeDashboard) selectedRecovery(s *state.MithrilState) {
	if d == nil || s == nil {
		return
	}
	r := &dashboard.Recovery{SelectedAt: time.Now().UTC(), SnapshotSlot: s.SnapshotSlot}
	if s.LastBankhash != "" {
		slot, hash := s.LastSlot, s.LastBankhash
		r.StoredSlot, r.StoredBankhash = &slot, &hash
	}
	if s.LastRootedBankhash != "" {
		slot, hash := s.LastRootedSlot, s.LastRootedBankhash
		r.RootedSlot, r.RootedBankhash = &slot, &hash
	}
	d.mu.Lock()
	d.state.Recovery = r
	d.mu.Unlock()
}

func (d *nodeDashboard) rpcStarted(rpc *rpcserver.RpcServer) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.rpc = rpc
	d.mu.Unlock()
}

func (d *nodeDashboard) consensusStarted(engine *consensus.AlpenglowObserverEngine) {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.consensus != engine {
		d.votorSocket = nil
		if d.networkDetails && !d.networkRetired && d.state.Phase != "stopping" && engine != nil && d.votorSocketGeneration != ^uint64(0) {
			d.votorSocketGeneration++
			binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
			if d.state.GenesisHash != nil {
				genesis := *d.state.GenesisHash
				binding.GenesisHash = &genesis
			}
			d.votorSocket = &votorSocketOwner{engine: engine, binding: binding, generation: d.votorSocketGeneration}
		}
	}
	d.consensus = engine
	d.mu.Unlock()
}

func consensusMeasurement(s consensus.MonitoringSnapshot, now time.Time) *dashboard.Consensus {
	m := &dashboard.Consensus{SampledAt: now.UTC(), ObservedBlocks: s.ObservedBlocks, ReplayedSlots: s.ReplayedSlots,
		SafetyFault: &s.SafetyFault, CertificatesAccepted: s.Chain.CertificatesAccepted, VotingEnabled: s.VotingEnabled}
	if s.HasNotarizedSlot {
		slot := s.NotarizedSlot
		m.NotarizedSlot = &slot
	}
	if b := s.Chain.LatestDirectFinalizedBlock; b.HasHash() {
		m.FinalizedSlot = &b.Slot
		id := b.Hash.String()
		m.FinalizedBlockID = &id
	}
	if r := s.Receiver; r != nil {
		m.Receiver = &dashboard.Receiver{DatagramsReceived: r.DatagramsReceived, MessagesDecoded: r.MessagesDecoded,
			DecodeErrors: r.DecodeErrors, ReadErrors: r.ReadErrors, ConnectionsRejected: r.ConnectionsRejected,
			RateLimitedDatagrams: r.RateLimitedDatagrams}
		if !r.LastMessageAt.IsZero() {
			at := r.LastMessageAt.UTC()
			m.Receiver.LastMessageAt = &at
		}
	}
	if v := s.Voting; v != nil {
		joined := v.JoinedLive
		m.Voting = &dashboard.Voting{
			SampledAt: v.At.UTC(), Identity: v.Identity.String(), VoteAccount: v.VoteAccount.String(), JoinedLive: &joined,
			VotesCast: v.VotesCast, NetworkLanded: v.NetworkLanded, MessagesQueued: v.MessagesQueued,
			MessagesDropped: v.MessagesDropped, PeerSendErrors: v.PeerSendErrors, PeerQueueDrops: v.PeerQueueDrops,
			SendTimeouts: v.SendTimeouts, DesiredPeers: v.DesiredPeers, Connections: v.Connections,
		}
		if v.HasEpochSet {
			root, epoch, eligible, total := v.RootSlot, v.Epoch, v.Eligible, v.TotalStake
			m.Voting.RootSlot, m.Voting.Epoch, m.Voting.KeyEligible, m.Voting.TotalAdmittedStake = &root, &epoch, &eligible, &total
			if v.Eligible {
				rank, stake := v.Rank, v.Stake
				m.Voting.Rank, m.Voting.AdmittedStake = &rank, &stake
			}
		}
		if !v.LastNetworkLandedAt.IsZero() {
			slot, at := v.LastNetworkLandedSlot, v.LastNetworkLandedAt.UTC()
			m.Voting.LastNetworkLandedSlot, m.Voting.LastNetworkLandedAt = &slot, &at
		}
	}
	return m
}

func (d *nodeDashboard) bootstrap(genesis string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.state.Phase, d.state.GenesisHash = "bootstrap", &genesis
	d.mu.Unlock()
}

func (d *nodeDashboard) replaying(holder *identity.Holder) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.identity, d.state.Phase = holder, "replaying"
	d.mu.Unlock()
}

func (d *nodeDashboard) stopping() {
	if d == nil {
		return
	}
	d.cache.stop()
	d.mu.Lock()
	machine := d.machine
	d.machineRetired = true
	d.state.Phase = "stopping"
	d.networkRetired, d.gossipClient = true, nil
	d.tpuSocket, d.votorSocket = nil, nil
	d.interfaces.stop()
	d.consensus = nil
	d.rpc = nil
	d.tpu, d.scheduler, d.pipelineStartedAt = nil, nil, time.Time{}
	d.mu.Unlock()
	machine.stop()
}

func (d *nodeDashboard) close() {
	if d != nil {
		d.mu.Lock()
		machine := d.machine
		d.machineRetired = true
		d.networkRetired, d.gossipClient = true, nil
		d.tpuSocket, d.votorSocket = nil, nil
		d.interfaces.stop()
		d.mu.Unlock()
		machine.stop()
		d.cache.stop()
		d.feed.Close()
	}
}
