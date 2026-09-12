package blockstream

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sort"
	"sync"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	gossipclient "github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// TurbinePrewarm collects live turbine blocks BEFORE replay exists. The
// repair protocol serves data shreds only, one per round trip — a slot that
// airs before the receiver joins must be fetched shred by shred with zero
// FEC leverage, while a slot received live completes from roughly half of
// each FEC set for free. Starting collection the moment current-epoch
// stakes are known (the incremental snapshot manifest parse, a minute
// before the AccountsDB finishes building) turns the expensive pre-join
// repair hole into spooled blocks handed to the BlockSource at replay
// start.
type TurbinePrewarm struct {
	cancel   context.CancelFunc
	done     chan struct{}
	receiver *turbine.UDPReceiver

	mu        sync.Mutex
	spool     map[uint64]*b.Block
	floor     uint64
	probeNext uint64 // next frontier slot the rolling probe will pin
	capacity  int
	dropped   int
	stopped   bool
}

// prewarmProbeSlots: the replay frontier slots the prewarm actively repairs
// during the AccountsDB build. These are the slots replay needs FIRST and
// the ones guaranteed to have aired before the receiver joined (zero FEC
// leverage, one data shred per round trip) — every one completed during the
// build is head-of-line stall time deleted from catchup. The probe rides
// the normal repair client (priority pins -> HighestWindowIndex discovery ->
// metered followups -> assembler/spool), so everything it fetches is kept,
// and its response statistics double as the boot-time peer-quality
// measurement behind the rate recommendation logged at handover. The
// window ROLLS: each completed probe slot pins the next frontier slot, so
// a long snapshot build keeps the repair budget on the frontier instead of
// idling after a fixed prefix completes.
const prewarmProbeSlots = 64

// TurbinePrewarmConfig mirrors the BlockSource's turbine wiring; the
// prewarm receiver is torn down (freeing the bind port) before the
// BlockSource constructs its own.
type TurbinePrewarmConfig struct {
	BindAddr         string
	GossipEntrypoint string
	GossipBindAddr   string
	AdvertisedIP     string
	ShredVersion     uint16
	AlpenglowAddr    string
	Identity         ed25519.PrivateKey
	LeaderForSlot    turbine.LeaderForSlotFunc
	StakesForSlot    func(slot uint64) map[solana.PublicKey]uint64
	EpochForSlot     func(slot uint64) uint64
	RootSlot         func() uint64
	UseChaCha8       bool
	DedupAddrs       bool
	FloorSlot        uint64 // resume frontier: spool floor and assembler retention floor
	MaxSpoolBlocks   int
	// ShredSpoolDir: shared on-disk shred spool. Everything the prewarm
	// hears goes to disk too, so even blocks beyond the in-RAM spool cap
	// hydrate later instead of re-repairing.
	ShredSpoolDir string
	// RepairMaxRequestsPerSecond mirrors the BlockSource's repair rate
	// ceiling override (0 = default).
	RepairMaxRequestsPerSecond int
}

// StartTurbinePrewarm joins gossip, starts a shred receiver with the
// retention floor pinned at the resume frontier, and spools completed
// blocks. Fail-open by design: any error means "no prewarm", never a
// blocked boot.
func StartTurbinePrewarm(cfg TurbinePrewarmConfig) (*TurbinePrewarm, error) {
	if cfg.BindAddr == "" || cfg.GossipEntrypoint == "" {
		return nil, fmt.Errorf("turbine prewarm needs a bind address and gossip entrypoint")
	}
	if cfg.MaxSpoolBlocks <= 0 {
		// Below the staging buffer's 256-slot cap so the handover injection
		// never evicts.
		cfg.MaxSpoolBlocks = 192
	}

	ctx, cancel := context.WithCancel(context.Background())

	gossipCfg := gossipclient.Config{
		Entrypoint:    cfg.GossipEntrypoint,
		BindAddr:      cfg.GossipBindAddr,
		TVUAddr:       cfg.BindAddr,
		AlpenglowAddr: cfg.AlpenglowAddr,
		AdvertisedIP:  cfg.AdvertisedIP,
		ShredVersion:  cfg.ShredVersion,
		Identity:      cfg.Identity,
		Name:          gossipclient.ClientName,
	}
	client, err := gossipclient.NewClient(gossipCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("prewarm gossip client: %w", err)
	}

	receiver := turbine.NewUDPReceiver(cfg.BindAddr)
	receiver.SetShredVersion(cfg.ShredVersion)
	if cfg.LeaderForSlot != nil {
		receiver.SetLeaderForSlot(cfg.LeaderForSlot)
	}
	if cfg.StakesForSlot != nil {
		if err := receiver.SetRetransmit(turbine.RetransmitConfig{
			Identity:     client.Identity(),
			Peers:        client,
			Stakes:       cfg.StakesForSlot,
			EpochForSlot: cfg.EpochForSlot,
			RootSlot:     cfg.RootSlot,
			UseChaCha8:   cfg.UseChaCha8,
			DedupAddrs:   cfg.DedupAddrs,
		}); err != nil {
			cancel()
			return nil, fmt.Errorf("prewarm retransmit setup: %w", err)
		}
	}
	if err := receiver.SetRepairPeerSource(client.Identity(), client.RepairPeers); err != nil {
		cancel()
		return nil, fmt.Errorf("prewarm repair setup: %w", err)
	}
	receiver.SetRepairRequestRate(cfg.RepairMaxRequestsPerSecond)
	if cfg.FloorSlot > 0 {
		receiver.SetRetentionFloor(cfg.FloorSlot)
	}
	if cfg.ShredSpoolDir != "" {
		if spool, serr := turbine.OpenShredSpool(cfg.ShredSpoolDir, shredSpoolMaxBytes); serr != nil {
			mlog.Log.FileOnlyf("prewarm shred spool disabled: %v", serr)
		} else {
			receiver.SetShredSpool(spool)
			// Assemble the resume-adjacent window in RAM; spool the far
			// edge to disk only.
			receiver.SetHydrationWindow(cfg.FloorSlot, cfg.FloorSlot+repairCatchupLiveDeliverWindow)
		}
	}

	if cfg.FloorSlot > 0 {
		receiver.PrioritizeRepairRange(cfg.FloorSlot, cfg.FloorSlot+prewarmProbeSlots-1)
	}

	pw := &TurbinePrewarm{
		cancel:    cancel,
		done:      make(chan struct{}),
		receiver:  receiver,
		spool:     make(map[uint64]*b.Block),
		floor:     cfg.FloorSlot,
		probeNext: cfg.FloorSlot + prewarmProbeSlots,
		capacity:  cfg.MaxSpoolBlocks,
	}

	go func() {
		if err := client.Run(ctx); err != nil && ctx.Err() == nil {
			mlog.Log.FileOnlyf("turbine prewarm gossip exited: %v", err)
		}
	}()
	receiverDone := make(chan error, 1)
	go func() {
		receiverDone <- receiver.Run(ctx)
	}()

	select {
	case err := <-receiverDone:
		cancel()
		close(pw.done)
		return nil, fmt.Errorf("prewarm receiver failed to start: %v", err)
	case rerr := <-receiver.Ready():
		if rerr != nil {
			cancel()
			close(pw.done)
			return nil, fmt.Errorf("prewarm receiver not ready: %v", rerr)
		}
	case <-time.After(15 * time.Second):
		cancel()
		close(pw.done)
		return nil, fmt.Errorf("prewarm receiver not ready within 15s")
	}

	go pw.drain(receiver)
	return pw, nil
}

// drain spools completed blocks (lowest slots win: they are the ones repair
// cannot fetch cheaply later) and discards receiver errors quietly.
func (pw *TurbinePrewarm) drain(receiver *turbine.UDPReceiver) {
	defer close(pw.done)
	blocks := receiver.Blocks()
	errs := receiver.Errors()
	for {
		select {
		case blk, ok := <-blocks:
			if !ok {
				return
			}
			pw.add(blk)
		case _, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
		}
	}
}

func (pw *TurbinePrewarm) add(blk *b.Block) {
	if blk == nil {
		return
	}
	pw.mu.Lock()
	defer pw.mu.Unlock()
	if pw.stopped {
		return
	}
	if pw.floor > 0 && blk.Slot < pw.floor {
		return
	}
	if _, exists := pw.spool[blk.Slot]; exists {
		return
	}
	pw.spool[blk.Slot] = blk
	// Roll the probe frontier: a completed slot inside the probe window
	// frees repair budget — pin the next frontier slot so a long build
	// keeps filling forward instead of idling once the prefix completes.
	if pw.floor > 0 && blk.Slot < pw.probeNext && pw.receiver != nil {
		pw.receiver.PrioritizeRepairSlot(pw.probeNext)
		pw.probeNext++
	}
	// Over capacity: evict the HIGHEST slot. The low end borders the resume
	// frontier and is what emission needs first — and what repair would
	// otherwise fetch one shred at a time; the high end is re-fetchable
	// cheaply near the tip later.
	for len(pw.spool) > pw.capacity {
		var victim uint64
		first := true
		for slot := range pw.spool {
			if first || slot > victim {
				victim = slot
				first = false
			}
		}
		delete(pw.spool, victim)
		pw.dropped++
	}
}

// Handover stops collection (freeing the turbine bind port for the
// BlockSource) and returns the spooled blocks in ascending slot order,
// along with how many overflowed the spool.
func (pw *TurbinePrewarm) Handover() ([]*b.Block, int) {
	pw.logProbeOutcome() // before Stop: receiver stats die with the receiver
	pw.Stop()
	pw.mu.Lock()
	defer pw.mu.Unlock()
	blocks := make([]*b.Block, 0, len(pw.spool))
	for _, blk := range pw.spool {
		blocks = append(blocks, blk)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Slot < blocks[j].Slot })
	pw.spool = nil
	return blocks, pw.dropped
}

// logProbeOutcome reports the boot-time frontier probe: how many of the
// replay-frontier slots were fully repaired before replay starts, and what
// the response statistics say about the peer set's headroom for the
// configurable rate ceiling. One console line; the per-peer table goes to
// the repair-peers file log.
func (pw *TurbinePrewarm) logProbeOutcome() {
	pw.mu.Lock()
	receiver, floor, stopped := pw.receiver, pw.floor, pw.stopped
	pw.mu.Unlock()
	if receiver == nil || stopped {
		return
	}
	completed := 0
	if floor > 0 {
		for slot := floor; slot < floor+prewarmProbeSlots; slot++ {
			if receiver.SlotCompleted(slot) {
				completed++
			}
		}
	}
	r := receiver.Stats().Repair
	outcomes := r.Responses + r.LateResponses + r.Timeouts
	if r.Requests == 0 || outcomes == 0 {
		mlog.Log.FileOnlyf("prewarm repair probe: no repair traffic during boot (requests %d) — no rate assessment", r.Requests)
		return
	}
	timeoutPct := 100 * r.Timeouts / outcomes
	var verdict string
	switch {
	case timeoutPct <= 10 && r.RespondingPeers >= 30:
		verdict = "healthy with headroom — the auto rate controller will ramp toward its ceiling on its own; pin block.repair_max_requests_per_second only if you need to cap it lower"
	case timeoutPct <= 25:
		verdict = "healthy at the current rate ceiling"
	case timeoutPct <= 50:
		verdict = "elevated timeouts — keep the current rate"
	default:
		verdict = "poor peer response quality — do not raise the rate"
	}
	mlog.Log.Infof("prewarm repair probe: %d/%d frontier slots fully repaired before replay | responses %d (late %d), timeouts %d (%d%%), avg %dms, peers responding %d/%d — %s",
		completed, prewarmProbeSlots, r.Responses, r.LateResponses, r.Timeouts, timeoutPct,
		r.AvgResponseMillis, r.RespondingPeers, r.Peers, verdict)
	logRepairPeerTable(receiver, "boot probe")
}

// Stop tears the prewarm receiver and gossip session down and waits for the
// drainer to exit. Idempotent.
func (pw *TurbinePrewarm) Stop() {
	pw.mu.Lock()
	if pw.stopped {
		pw.mu.Unlock()
		<-pw.done
		return
	}
	pw.stopped = true
	pw.mu.Unlock()
	pw.cancel()
	<-pw.done
}
