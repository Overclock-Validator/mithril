package turbine

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

type LeaderForSlotFunc func(slot uint64) (solana.PublicKey, bool)

type UDPReceiver struct {
	Addr string

	assembler *SlotAssembler
	// A reset must be atomic with respect to both network ingestion and spool
	// hydration. Otherwise a hydrator can read a soon-to-be-discarded file,
	// lose the race with ResetSlot, and feed its stale packets straight back
	// into the newly-cleared assembler state.
	slotResetMu    sync.RWMutex
	completionPool *slotCompletionPool
	leaderForSlot  LeaderForSlotFunc
	repairClient   *repairClient
	retransmitCfg  *RetransmitConfig
	retransmitter  *Retransmitter
	sigCache       shredSigCache
	shredVersion   uint16
	blocks         chan *block.Block
	// Count queued deliveries rather than storing a boolean. A rejected
	// Alpenglow variant can reset a slot and assemble its replacement while
	// the first delivery is still crossing Blocks(); acknowledging the first
	// must not make the replacement look lost to the catchup watchdog.
	pendingBlocksMu sync.Mutex
	pendingBlocks   map[uint64]int // slot -> deliveries waiting in Blocks()
	errs            chan error
	ready           chan error
	once            sync.Once
	readyOnce       sync.Once

	spool       *ShredSpool
	hydrLo      atomic.Uint64
	hydrHi      atomic.Uint64
	hydrateKick chan struct{}
	// Hydration outcomes: whether spooled slots complete purely from disk
	// (their own data + spooled coding via recovery) or hand holes to network
	// repair — the number that says if the freshness-repair lead time is
	// sufficient or the priority window needs widening.
	hydratedSlots    atomic.Uint64
	hydratedFromDisk atomic.Uint64
	// Repair shreds written straight to the spool during deep catchup (slot
	// beyond the hydration window): they never reach in-RAM assembly here, so
	// the assembler's useful-repair counter — which increments in AddShredFrom
	// — misses them, and hydration later re-adds them as fromRepair=false. This
	// counter keeps them in the useful-repair throughput picture; without it,
	// USEFUL shreds/s reads ~0 exactly when disk-bound catchup repair is doing
	// all the work.
	spooledRepairShreds   atomic.Uint64
	repairSocketPackets   atomic.Uint64
	repairSocketUnmatched atomic.Uint64

	packets         atomic.Uint64
	dataShreds      atomic.Uint64
	codingShreds    atomic.Uint64
	parseErrors     atomic.Uint64
	signatureErrors atomic.Uint64
	missingLeaders  atomic.Uint64
	versionMismatch atomic.Uint64
	assemblyErrors  atomic.Uint64
	blocksEmitted   atomic.Uint64
	lastPacketUnix  atomic.Int64
	lastDataSlot    atomic.Uint64
	lastBlockSlot   atomic.Uint64

	firstShredMu    sync.Mutex
	firstShredSink  func(slot uint64)
	firstShredSeen  map[uint64]struct{}
	firstShredOrder []uint64
}

const maxFirstShredNotifications = 8192

type ReceiverStats struct {
	Packets               uint64
	DataShreds            uint64
	CodingShreds          uint64
	ParseErrors           uint64
	SignatureErrors       uint64
	MissingLeaders        uint64
	ShredVersionMismatch  uint64
	AssemblyErrors        uint64
	BlocksEmitted         uint64
	RecoveredData         uint64
	UsefulRepairShreds    uint64 // distinct repair data shreds accepted into in-RAM assembly
	SpooledRepairShreds   uint64 // repair data shreds written to the catchup spool (not yet assembled)
	RepairSocketPackets   uint64 // all datagrams read from the dedicated repair socket
	RepairSocketUnmatched uint64 // parsed shreds on that socket which no longer match an outstanding request
	EvictedSlots          uint64
	IgnoredOldShreds      uint64
	PriorityRepairSlots   int
	NonCanonicalBlockIDs  uint64
	LastNonCanonicalSlot  uint64
	LastNonCanonicalGot   solana.Hash
	LastNonCanonicalWant  solana.Hash
	Repair                RepairStats
	LastPacketUnix        int64
	LastDataSlot          uint64
	LastBlockSlot         uint64
	ActiveSlots           int
	// Spool hydration outcomes: slots fed from disk, and how many of those
	// completed with zero network repair (spooled coding healed the holes).
	HydratedSlots    uint64
	HydratedFromDisk uint64
	// Signature verification: ed25519 ops actually run vs shreds authenticated
	// off the per-FEC-set merkle root cache (one signed root covers ~64 shreds).
	SigVerifies     uint64
	SigVerifyCached uint64
	Retransmit      RetransmitStats
}

// Repair catchup is packet-processing heavy (wire decode, Merkle proof,
// occasional Ed25519, spool classification, FEC/block assembly). Multiple
// readers on the dedicated repair socket keep the kernel queue draining while
// one reader finishes a large block. net.UDPConn supports concurrent methods;
// assembler, signature cache, repair client, and spool provide their own
// synchronization.
const turbineRepairReadWorkers = 4

func NewUDPReceiver(addr string) *UDPReceiver {
	return &UDPReceiver{
		Addr:           addr,
		assembler:      NewSlotAssembler(),
		blocks:         make(chan *block.Block, 1024),
		pendingBlocks:  make(map[uint64]int),
		errs:           make(chan error, 16),
		ready:          make(chan error, 1),
		hydrateKick:    make(chan struct{}, 1),
		firstShredSeen: make(map[uint64]struct{}),
	}
}

// SetShredVersion rejects packets for another cluster before they can be
// assembled or amplified by retransmit. A zero version leaves filtering off.
func (r *UDPReceiver) SetShredVersion(version uint16) {
	r.shredVersion = version
}

// SetRetransmit configures the receiver as a Turbine relay. It must be called
// before Run and requires a leader lookup so the receiver can authenticate the
// leader before the retransmitter validates the upstream hop.
func (r *UDPReceiver) SetRetransmit(cfg RetransmitConfig) error {
	if r.leaderForSlot == nil {
		return errors.New("turbine retransmit requires a leader-for-slot source")
	}
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return fmt.Errorf("turbine retransmit identity has invalid size %d", len(cfg.Identity))
	}
	cp := cfg
	cp.Identity = append(ed25519.PrivateKey(nil), cfg.Identity...)
	r.retransmitCfg = &cp
	return nil
}

func (r *UDPReceiver) SetLeaderForSlot(fn LeaderForSlotFunc) {
	r.leaderForSlot = fn
}

// SetFirstShredSink registers the Votor early-crashed-leader signal. The sink
// is called once per slot, after leader-signature verification and before full
// block assembly.
func (r *UDPReceiver) SetFirstShredSink(fn func(slot uint64)) {
	r.firstShredMu.Lock()
	r.firstShredSink = fn
	r.firstShredMu.Unlock()
}

func (r *UDPReceiver) noteFirstShred(slot uint64) {
	r.firstShredMu.Lock()
	if _, exists := r.firstShredSeen[slot]; exists {
		r.firstShredMu.Unlock()
		return
	}
	r.firstShredSeen[slot] = struct{}{}
	r.firstShredOrder = append(r.firstShredOrder, slot)
	for len(r.firstShredOrder) > maxFirstShredNotifications {
		old := r.firstShredOrder[0]
		r.firstShredOrder = r.firstShredOrder[1:]
		delete(r.firstShredSeen, old)
	}
	sink := r.firstShredSink
	r.firstShredMu.Unlock()
	if sink != nil {
		sink(slot)
	}
}

func (r *UDPReceiver) SetRepairPeerSource(identity ed25519.PrivateKey, source func() []gossip.RepairPeer) error {
	client, err := newRepairClient(identity, source)
	if err != nil {
		return err
	}
	r.repairClient = client
	return nil
}

// SetRepairRequestRate overrides the global repair request-rate ceiling
// (requests per second; <=0 keeps the default). Call after
// SetRepairPeerSource.
func (r *UDPReceiver) SetRepairRequestRate(perSecond int) {
	if r.repairClient != nil && perSecond > 0 {
		r.repairClient.setRateLimit(perSecond)
	}
}

// RepairPeerReport returns the busiest repair peers' service records (sent,
// timely/late/timeout, latency, score) for file-log peer tables.
func (r *UDPReceiver) RepairPeerReport(maxPeers int) []RepairPeerReport {
	if r.repairClient == nil {
		return nil
	}
	return r.repairClient.peerReport(maxPeers)
}

// RepairPeerQuality returns the aggregate peer-quality picture; ok is false
// when no repair client is attached.
func (r *UDPReceiver) RepairPeerQuality() (RepairPeerAggregate, bool) {
	if r.repairClient == nil {
		return RepairPeerAggregate{}, false
	}
	return r.repairClient.peerAggregate(), true
}

// RepairOutstandingForSlot counts in-flight repair requests for slot.
func (r *UDPReceiver) RepairOutstandingForSlot(slot uint64) int {
	if r.repairClient == nil {
		return 0
	}
	return r.repairClient.outstandingForSlot(slot)
}

func (r *UDPReceiver) SetKnownAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.SetKnownAlpenglowBlockID(slot, blockID)
}

func (r *UDPReceiver) ResetSlot(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.slotResetMu.Lock()
	defer r.slotResetMu.Unlock()
	if r.repairClient != nil {
		r.repairClient.cancelSlotRequests(slot)
	}
	r.assembler.ResetSlot(slot)
}

// ResetSlotAndDiscardSpool clears the in-memory assembly state and the
// persisted packets as one operation relative to ingestion/hydration. Use it
// when the slot is known to be poisoned or belongs to a rejected Alpenglow
// branch; ordinary eviction resets deliberately keep the spool for cheap
// rehydration.
func (r *UDPReceiver) ResetSlotAndDiscardSpool(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.slotResetMu.Lock()
	defer r.slotResetMu.Unlock()
	if r.repairClient != nil {
		r.repairClient.cancelSlotRequests(slot)
	}
	r.assembler.ResetSlot(slot)
	if r.spool != nil {
		r.spool.DiscardSlot(slot)
	}
}

// RejectAlpenglowBlockIDAndDiscardSlot atomically rejects one objective-invalid
// block identity, unpins it as an assembler hint, and drops its current memory
// and spool packets. A different sibling identity remains eligible.
func (r *UDPReceiver) RejectAlpenglowBlockIDAndDiscardSlot(slot uint64, blockID solana.Hash) {
	if r == nil || r.assembler == nil {
		return
	}
	r.slotResetMu.Lock()
	defer r.slotResetMu.Unlock()
	if r.repairClient != nil {
		r.repairClient.cancelSlotRequests(slot)
	}
	r.assembler.RejectAlpenglowBlockID(slot, blockID)
	r.assembler.ResetSlot(slot)
	if r.spool != nil {
		r.spool.DiscardSlot(slot)
	}
}

// ShredEdges reports the monotonic shred frontier from the assembler: highest
// slot with any accepted shred, and highest slot that became full.
func (r *UDPReceiver) ShredEdges() (latestShredSlot, highestFullSlot uint64) {
	return r.assembler.ShredEdges()
}

// ShredObservation reports partial shred arrivals for a slot that never
// became full (skip observability).
// SlotAssemblyErrors reports assembly-failure count and latest failure text
// for a slot still being assembled.
func (r *UDPReceiver) SlotAssemblyErrors(slot uint64) (int, string) {
	return r.assembler.SlotAssemblyErrors(slot)
}

// SlotCompleted reports whether the assembler holds a completed-slot marker —
// a completed slot silently ignores all further shreds and repair requests.
func (r *UDPReceiver) SlotCompleted(slot uint64) bool {
	return r.assembler.SlotCompleted(slot)
}

// HeadShredDetail reports the completion picture for a slot still being
// assembled (see SlotAssembler.HeadShredDetail).
func (r *UDPReceiver) HeadShredDetail(slot uint64) (HeadShredDetail, bool) {
	return r.assembler.HeadShredDetail(slot)
}

func (r *UDPReceiver) ShredObservation(slot uint64) (PartialShredObservation, bool) {
	return r.assembler.ShredObservation(slot)
}

// SetRetentionFloor pins the assembler's age cutoff for repair catchup: slots
// >= floor stay accepted however far they trail the live edge. 0 restores the
// normal lag-based retention.
// SetShredSpool attaches the on-disk shred spool. Must be called before Run.
func (r *UDPReceiver) SetShredSpool(spool *ShredSpool) {
	r.spool = spool
	// Journal completeness the moment a slot fully assembles — including
	// during hydration of adopted files, which self-heals missing markers.
	r.assembler.SetOnComplete(spool.MarkComplete)
}

// SetHydrationWindow bounds in-RAM assembly during catchup: verified shreds
// for slots beyond hi (and outside the live edge / priority pins) go to the
// on-disk spool only, and the hydrator streams slots in [lo, hi] back into
// the assembler AHEAD of replay — prefetch, not just-in-time. hi == 0
// disables the policy (normal near-tip operation).
func (r *UDPReceiver) SetHydrationWindow(lo, hi uint64) {
	r.hydrLo.Store(lo)
	r.hydrHi.Store(hi)
	if r.spool != nil && lo > 8 {
		r.spool.SetFloor(lo - 8)
	}
	// Keep the freshness-repair scan aligned with the RAM policy: with the
	// window ON, only the last spoolLiveAssemblyLag slots assemble in RAM,
	// so scanning further back would emit repair requests whose responses
	// cannot assemble. Window OFF restores the full near-tip scan.
	if hi != 0 {
		r.assembler.SetEdgeRepairLag(spoolLiveAssemblyLag)
	} else {
		r.assembler.SetEdgeRepairLag(0)
	}
	select {
	case r.hydrateKick <- struct{}{}:
	default:
	}
}

func (r *UDPReceiver) SetRetentionFloor(slot uint64) {
	r.assembler.SetRetentionFloor(slot)
}

func (r *UDPReceiver) PrioritizeRepairSlot(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.PrioritizeRepairSlot(slot)
}

func (r *UDPReceiver) PrioritizeRepairRange(start, end uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.PrioritizeRepairRange(start, end)
}

func (r *UDPReceiver) Blocks() <-chan *block.Block {
	return r.blocks
}

// BlockPendingDelivery reports whether an assembled block is still queued
// between the receiver and its consumer. A completed assembler marker is not
// "lost" while this is true.
func (r *UDPReceiver) BlockPendingDelivery(slot uint64) bool {
	r.pendingBlocksMu.Lock()
	defer r.pendingBlocksMu.Unlock()
	return r.pendingBlocks[slot] > 0
}

// AcknowledgeBlockDelivery transfers ownership of a block received from
// Blocks() to the consumer.
func (r *UDPReceiver) AcknowledgeBlockDelivery(slot uint64) {
	r.finishPendingBlock(slot)
}

func (r *UDPReceiver) startPendingBlock(slot uint64) {
	r.pendingBlocksMu.Lock()
	r.pendingBlocks[slot]++
	r.pendingBlocksMu.Unlock()
}

func (r *UDPReceiver) finishPendingBlock(slot uint64) {
	r.pendingBlocksMu.Lock()
	if r.pendingBlocks[slot] <= 1 {
		delete(r.pendingBlocks, slot)
	} else {
		r.pendingBlocks[slot]--
	}
	r.pendingBlocksMu.Unlock()
}

func (r *UDPReceiver) Errors() <-chan error {
	return r.errs
}

func (r *UDPReceiver) Ready() <-chan error {
	return r.ready
}

func (r *UDPReceiver) Stats() ReceiverStats {
	nonCanonicalCount, nonCanonicalSlot, nonCanonicalGot, nonCanonicalWant := r.assembler.NonCanonicalBlockIDStats()
	sigHits, sigVerifies := r.sigCache.stats()
	return ReceiverStats{
		Packets:               r.packets.Load(),
		DataShreds:            r.dataShreds.Load(),
		CodingShreds:          r.codingShreds.Load(),
		ParseErrors:           r.parseErrors.Load(),
		SignatureErrors:       r.signatureErrors.Load(),
		MissingLeaders:        r.missingLeaders.Load(),
		ShredVersionMismatch:  r.versionMismatch.Load(),
		AssemblyErrors:        r.assemblyErrors.Load(),
		BlocksEmitted:         r.blocksEmitted.Load(),
		RecoveredData:         r.assembler.RecoveredDataShreds(),
		UsefulRepairShreds:    r.assembler.UsefulRepairShreds(),
		SpooledRepairShreds:   r.spooledRepairShreds.Load(),
		RepairSocketPackets:   r.repairSocketPackets.Load(),
		RepairSocketUnmatched: r.repairSocketUnmatched.Load(),
		EvictedSlots:          r.assembler.EvictedSlots(),
		IgnoredOldShreds:      r.assembler.IgnoredOldShreds(),
		PriorityRepairSlots:   r.assembler.PriorityRepairSlots(),
		NonCanonicalBlockIDs:  nonCanonicalCount,
		LastNonCanonicalSlot:  nonCanonicalSlot,
		LastNonCanonicalGot:   nonCanonicalGot,
		LastNonCanonicalWant:  nonCanonicalWant,
		Repair:                r.repairStats(),
		LastPacketUnix:        r.lastPacketUnix.Load(),
		LastDataSlot:          r.lastDataSlot.Load(),
		LastBlockSlot:         r.lastBlockSlot.Load(),
		ActiveSlots:           r.assembler.ActiveSlots(),
		HydratedSlots:         r.hydratedSlots.Load(),
		HydratedFromDisk:      r.hydratedFromDisk.Load(),
		SigVerifies:           sigVerifies,
		SigVerifyCached:       sigHits,
		Retransmit:            r.retransmitStats(),
	}
}

func (r *UDPReceiver) retransmitStats() RetransmitStats {
	if r.retransmitter == nil {
		return RetransmitStats{}
	}
	return r.retransmitter.Stats()
}

func (r *UDPReceiver) repairStats() RepairStats {
	if r.repairClient == nil {
		return RepairStats{}
	}
	return r.repairClient.stats()
}

func (r *UDPReceiver) signalReady(err error) {
	r.readyOnce.Do(func() {
		r.ready <- err
		close(r.ready)
	})
}

func (r *UDPReceiver) Run(ctx context.Context) error {
	defer r.once.Do(func() {
		close(r.blocks)
		close(r.errs)
	})
	// Everything Run spawns is bound to runCtx and dies with Run — on the
	// cancel path AND the socket-error path (where the caller's ctx is
	// still alive). The hydrator is additionally JOINED before the deferred
	// channel close above runs (LIFO): an unjoined hydrator emitting into a
	// just-closed blocks channel was a coin-flip panic on every stream
	// restart or prewarm handover (found by review).
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	udpAddr, err := net.ResolveUDPAddr("udp", r.Addr)
	if err != nil {
		r.signalReady(err)
		return err
	}
	liveConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		r.signalReady(err)
		return err
	}
	defer liveConn.Close()
	gossip.BoostUDPReceiveBuffer(liveConn, gossip.TurbineUDPReceiveBufferBytes, "turbine receiver")

	// Serve-repair replies to the request's UDP source port. Give repair its
	// own socket so the one or two shreds gating replay never sit behind a
	// multi-megabyte FIFO of unrelated live-edge broadcast. The advertised TVU
	// socket remains unchanged; this ephemeral socket is outbound repair only.
	var repairConn *net.UDPConn
	if r.repairClient != nil {
		repairBind := &net.UDPAddr{IP: udpAddr.IP, Port: 0, Zone: udpAddr.Zone}
		repairConn, err = net.ListenUDP("udp", repairBind)
		if err != nil {
			r.signalReady(fmt.Errorf("listen for turbine repair responses: %w", err))
			return fmt.Errorf("listen for turbine repair responses: %w", err)
		}
		defer repairConn.Close()
		gossip.BoostUDPReceiveBuffer(repairConn, gossip.TurbineUDPReceiveBufferBytes, "turbine repair receiver")
	}

	var retransmitDone chan struct{}
	if r.retransmitCfg != nil {
		r.retransmitter, err = NewRetransmitter(*r.retransmitCfg)
		if err != nil {
			r.signalReady(fmt.Errorf("start turbine retransmit: %w", err))
			return fmt.Errorf("start turbine retransmit: %w", err)
		}
		retransmitDone = make(chan struct{})
		go func() {
			defer close(retransmitDone)
			r.retransmitter.Run(runCtx)
		}()
	}
	r.signalReady(nil)
	completionPool := newSlotCompletionPool(r.assembler, &r.slotResetMu, r.startPendingBlock, defaultSlotCompletionWorkers(), slotCompletionQueueDepth)
	r.completionPool = completionPool
	completionDone := make(chan struct{})
	go func() {
		defer close(completionDone)
		r.consumeCompletionResults(runCtx, completionPool.results)
	}()

	go func() {
		<-runCtx.Done()
		_ = liveConn.Close()
		if repairConn != nil {
			_ = repairConn.Close()
		}
	}()
	if repairConn != nil {
		go r.repairClient.run(runCtx, repairConn, r.assembler)
	}
	var hydratorDone chan struct{}
	if r.spool != nil {
		// Flush buffered slot-file tails and close the completeness journal
		// when this receiver dies: the next receiver over the SAME spool
		// directory (block source after a prewarm handoff, or a stream
		// restart) can only see what reached disk — and it truncate-rewrites
		// the journal on open, so ours must be closed first.
		defer r.spool.Close()
		hydratorDone = make(chan struct{})
		go func() {
			defer close(hydratorDone)
			r.hydrateLoop(runCtx)
		}()
	}

	readers := 1
	readErr := make(chan error, 1+turbineRepairReadWorkers)
	go func() { readErr <- r.readPackets(runCtx, liveConn, false) }()
	if repairConn != nil {
		readers += turbineRepairReadWorkers
		for i := 0; i < turbineRepairReadWorkers; i++ {
			go func() { readErr <- r.readPackets(runCtx, repairConn, true) }()
		}
	}
	firstErr := <-readErr
	runCancel()
	_ = liveConn.Close()
	if repairConn != nil {
		_ = repairConn.Close()
	}
	for i := 1; i < readers; i++ {
		<-readErr
	}
	if hydratorDone != nil {
		<-hydratorDone
	}
	if retransmitDone != nil {
		<-retransmitDone
	}
	// No submitters remain. Drain queued work while the spool hook and result
	// consumer are alive, then close results and join it before channel close.
	completionPool.closeAndWait()
	<-completionDone
	r.completionPool = nil

	if firstErr != nil && ctx.Err() == nil {
		return firstErr
	}
	return nil
}

func (r *UDPReceiver) readPackets(ctx context.Context, conn *net.UDPConn, onRepairSocket bool) error {
	buf := make([]byte, packetDataSize)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !r.processPacket(ctx, conn, buf[:n], addr, onRepairSocket) {
			return nil
		}
	}
}

func (r *UDPReceiver) processPacket(ctx context.Context, conn *net.UDPConn, packet []byte, addr *net.UDPAddr, onRepairSocket bool) bool {
	r.packets.Add(1)
	if onRepairSocket {
		r.repairSocketPackets.Add(1)
	}
	r.lastPacketUnix.Store(time.Now().Unix())
	if r.repairClient != nil && r.repairClient.handleRepairPing(conn, packet, addr) {
		return true
	}
	shred, err := ParseShred(packet)
	if err != nil {
		if errors.Is(err, ErrCodingShredIgnored) {
			r.codingShreds.Add(1)
			return true
		}
		r.parseErrors.Add(1)
		select {
		case r.errs <- err:
		default:
		}
		return true
	}
	if r.shredVersion != 0 && shred.Version != r.shredVersion {
		r.versionMismatch.Add(1)
		select {
		case r.errs <- fmt.Errorf("turbine shred version mismatch: slot %d shred %d got %d want %d", shred.Slot, shred.Index, shred.Version, r.shredVersion):
		default:
		}
		return true
	}
	switch shred.Type {
	case ShredTypeData:
		r.dataShreds.Add(1)
		// Monotonic max: out-of-order packets must not move the reported
		// latest-shred edge backward.
		for {
			cur := r.lastDataSlot.Load()
			if shred.Slot <= cur || r.lastDataSlot.CompareAndSwap(cur, shred.Slot) {
				break
			}
		}
	case ShredTypeCode:
		r.codingShreds.Add(1)
	}
	var leader solana.PublicKey
	if r.leaderForSlot != nil {
		var ok bool
		leader, ok = r.leaderForSlot(shred.Slot)
		if !ok {
			r.missingLeaders.Add(1)
			select {
			case r.errs <- fmt.Errorf("missing leader for turbine shred slot %d", shred.Slot):
			default:
			}
			return true
		}
		if err := r.sigCache.verifyShred(shred, leader); err != nil {
			r.signatureErrors.Add(1)
			select {
			case r.errs <- err:
			default:
			}
			return true
		}
	}
	matchedRepair := false
	if onRepairSocket && r.repairClient != nil {
		matchedRepair = r.repairClient.observeShredResponse(conn, packet, addr, shred)
	}
	if onRepairSocket && !matchedRepair {
		r.repairSocketUnmatched.Add(1)
	}
	if r.retransmitter != nil {
		// Socket origin, rather than mutable nonce-accounting state, decides
		// whether a packet may be retransmitted. Duplicate or late repair
		// responses remain repair-path packets even after their nonce entry was
		// consumed or expired.
		if err := r.retransmitter.SubmitFrom(packet, shred, leader, onRepairSocket, addr); err != nil {
			r.signatureErrors.Add(1)
			select {
			case r.errs <- err:
			default:
			}
			return true
		}
	}
	if r.repairClient != nil {
		// A leader-valid broadcast with a forged upstream signature is not an
		// accepted shred and must not retire repair for the real packet.
		r.repairClient.satisfyDataShred(shred)
	}
	r.noteFirstShred(shred.Slot)
	r.slotResetMu.RLock()
	if r.spool != nil {
		// Write-through of every VERIFIED shred. AppendShred consumes packet
		// synchronously and retains no reference, so the receive buffer can be
		// reused immediately without an intermediate copy. Repaired shreds are
		// included — a later slot reset re-hydrates from disk instead of
		// re-fetching over the wire.
		spooled := r.spool.AppendShred(shred, packet)
		if r.skipAssemblyForSpool(shred.Slot) {
			r.slotResetMu.RUnlock()
			// Far-future slot during catchup: on disk is enough. The
			// hydrator streams it into RAM when replay approaches. A repair
			// delivery lands on disk and bypasses the assembler's useful
			// counter, so count it as useful repair throughput here — else
			// disk-bound catchup, where most repair goes straight to the
			// spool, would read as ~zero useful/s while repair is in fact
			// carrying the whole catchup.
			if matchedRepair && spooled {
				r.spooledRepairShreds.Add(1)
			}
			return true
		}
	}
	work, err := r.assembler.addShredFrom(shred, matchedRepair)
	r.slotResetMu.RUnlock()
	if err != nil {
		if errors.Is(err, ErrDuplicateShred) {
			return true
		}
		r.assemblyErrors.Add(1)
		select {
		case r.errs <- err:
		default:
		}
		return true
	}
	return r.submitCompletion(ctx, work, false)
}

func (r *UDPReceiver) submitCompletion(ctx context.Context, work *slotCompletionWork, hydrated bool) bool {
	if work == nil {
		return true
	}
	if pool := r.completionPool; pool != nil {
		return pool.enqueue(ctx, work, hydrated)
	}

	// Direct processPacket callers outside Run retain synchronous behavior.
	processed := r.assembler.processCompletion(ctx, work)
	if processed.canceled {
		r.assembler.abortCompletion(work)
		return false
	}
	r.slotResetMu.RLock()
	blk, err := r.assembler.finalizeCompletion(work, processed)
	pending := false
	if blk != nil && err == nil {
		r.startPendingBlock(blk.Slot)
		pending = true
	}
	r.slotResetMu.RUnlock()
	return r.handleCompletionResult(ctx, slotCompletionResult{
		block:    blk,
		err:      err,
		hydrated: hydrated,
		pending:  pending,
	})
}

func (r *UDPReceiver) consumeCompletionResults(ctx context.Context, results <-chan slotCompletionResult) {
	for result := range results {
		if ctx.Err() != nil {
			if result.pending && result.block != nil {
				r.finishPendingBlock(result.block.Slot)
			}
			continue
		}
		r.handleCompletionResult(ctx, result)
	}
}

func (r *UDPReceiver) handleCompletionResult(ctx context.Context, result slotCompletionResult) bool {
	if result.err != nil {
		r.assemblyErrors.Add(1)
		select {
		case r.errs <- result.err:
		default:
		}
		return true
	}
	if result.block == nil {
		return true
	}
	if r.repairClient != nil {
		r.repairClient.cancelSlotRequests(result.block.Slot)
	}
	if result.hydrated {
		r.hydratedFromDisk.Add(1)
	}
	if result.pending {
		return r.emitPendingAssembled(ctx, result.block)
	}
	return r.emitAssembled(ctx, result.block)
}

// skipAssemblyForSpool implements the catchup RAM policy: with a hydration
// window set, only the window itself, the live edge, and priority-pinned
// slots assemble in RAM; everything else lives on disk until hydrated.
func (r *UDPReceiver) skipAssemblyForSpool(slot uint64) bool {
	hi := r.hydrHi.Load()
	if hi == 0 || slot <= hi {
		return false
	}
	if latest, _ := r.assembler.ShredEdges(); slot+spoolLiveAssemblyLag >= latest {
		return false // live edge stays in RAM: broadcast + FEC completes it free
	}
	return !r.assembler.IsPrioritySlot(slot)
}

func (r *UDPReceiver) emitAssembled(ctx context.Context, blk *block.Block) bool {
	r.startPendingBlock(blk.Slot)
	return r.emitPendingAssembled(ctx, blk)
}

func (r *UDPReceiver) emitPendingAssembled(ctx context.Context, blk *block.Block) bool {
	r.blocksEmitted.Add(1)
	r.lastBlockSlot.Store(blk.Slot)
	select {
	case r.blocks <- blk:
		return true
	case <-ctx.Done():
		r.finishPendingBlock(blk.Slot)
		return false
	}
}

// hydrateLoop streams spooled slots inside the hydration window into the
// assembler AHEAD of replay — batched reads a window at a time, so the
// emitter never waits on a disk load for the slot it needs next.
func (r *UDPReceiver) hydrateLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	hydrated := make(map[uint64]bool)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.hydrateKick:
		case <-ticker.C:
		}
		lo := r.hydrLo.Load()
		hi := r.hydrHi.Load()
		if hi == 0 || lo > hi {
			continue
		}
		for slot := range hydrated {
			if slot < lo {
				delete(hydrated, slot)
			}
		}
		for _, slot := range r.spool.SlotsInRange(lo, hi) {
			if r.assembler.SlotCompleted(slot) {
				hydrated[slot] = true
				continue
			}
			if hydrated[slot] {
				// Re-hydrate only if the assembler lost its state (reset).
				if _, live := r.assembler.HeadShredDetail(slot); live {
					continue
				}
			}
			r.slotResetMu.RLock()
			packets, err := r.spool.ReadSlot(slot)
			if err != nil {
				r.slotResetMu.RUnlock()
				continue
			}
			hydrated[slot] = true
			var work *slotCompletionWork
			for _, pkt := range packets {
				shred, perr := ParseShred(pkt)
				if perr != nil {
					continue
				}
				if r.repairClient != nil {
					r.repairClient.satisfyDataShred(shred)
				}
				candidate, aerr := r.assembler.addShredFrom(shred, false)
				if aerr != nil || candidate == nil {
					continue
				}
				work = candidate
				break
			}
			r.slotResetMu.RUnlock()
			r.hydratedSlots.Add(1)
			if !r.submitCompletion(ctx, work, true) {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}
