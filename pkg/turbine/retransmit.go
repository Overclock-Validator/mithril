package turbine

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/net/ipv4"
)

const (
	defaultRetransmitQueueDepth = 8192
	maxRetransmitWorkers        = 8
	retransmitNodesCacheTTL     = 5 * time.Second
	retransmitNodesCacheCap     = 5
	retransmitDedupResetCycle   = 5 * time.Minute
	retransmitDedupCapacity     = 1 << 18
	retransmitParentSigCacheCap = 4096
	maxDuplicateShreds          = 2
	commonShredHeaderSize       = shredFECSetIndexOffset + 4
	maxRetransmitSendAttempts   = 3
	retransmitDiagnosticPeriod  = 10 * time.Second
)

var ErrInvalidRetransmitterSignature = errors.New("invalid turbine retransmitter signature")

const (
	parentSourceDirectLeader = "direct_leader"
	parentSourceRepairSocket = "repair_socket"
	parentSourceUnexplained  = "unexplained"
)

type retransmitRepairPeerSource interface {
	RepairPeers() []gossip.RepairPeer
}

// RetransmitConfig supplies the live gossip and epoch-stake view needed to
// derive this validator's position in Agave's deterministic Turbine tree.
type RetransmitConfig struct {
	Identity     ed25519.PrivateKey
	Peers        TVUPeerSource
	Stakes       func(slot uint64) map[solana.PublicKey]uint64
	EpochForSlot func(slot uint64) uint64
	RootSlot     func() uint64
	UseChaCha8   bool
	DedupAddrs   bool
	Workers      int
	QueueDepth   int
}

type retransmitWork struct {
	packet []byte
	shred  ShredID
	leader solana.PublicKey
}

type cachedRetransmitNodes struct {
	asof  time.Time
	nodes *ClusterNodes
}

type retransmitHeader [commonShredHeaderSize]byte

type retransmitDeduper struct {
	mu      sync.Mutex
	resetAt time.Time
	headers map[retransmitHeader]struct{}
	counts  map[ShredID]uint8
}

type retransmitParentSigKey struct {
	parent    solana.PublicKey
	root      solana.Hash
	signature solana.Signature
}

type retransmitParentSigCache struct {
	mu   sync.Mutex
	cur  map[retransmitParentSigKey]bool
	prev map[retransmitParentSigKey]bool
}

type packetBatchSender interface {
	Send(packet []byte, peers []*net.UDPAddr) (int, error)
	Close() error
}

type udpBatchSender struct {
	conn     *net.UDPConn
	batch    *ipv4.PacketConn
	messages []ipv4.Message
	buffers  [][]byte
}

type retransmitSendIssue struct {
	attempt     int
	requested   int
	sent        int
	firstUnsent string
	err         error
}

// Retransmitter validates the previous Turbine hop, re-signs resigned Merkle
// shreds, and asynchronously fans packets out to this node's downstream peers.
type Retransmitter struct {
	cfg  RetransmitConfig
	self solana.PublicKey
	// Minimum effective SO_SNDBUF across the worker sockets. It is fixed at
	// construction and exposed in periodic telemetry so a capped host is
	// immediately visible alongside short-send counters.
	sendBufferBytes int

	queue   chan retransmitWork
	senders []packetBatchSender

	cacheMu        sync.Mutex
	cache          map[uint64]cachedRetransmitNodes
	deduper        retransmitDeduper
	parentSigCache retransmitParentSigCache
	diagnosticMu   sync.Mutex
	lastSendDiag   string
	lastParentDiag string
	nextSendDiag   atomic.Int64
	nextParentDiag atomic.Int64

	submitted                atomic.Uint64
	sentShreds               atomic.Uint64
	sentPackets              atomic.Uint64
	targetPackets            atomic.Uint64
	packetAttempts           atomic.Uint64
	unsentPackets            atomic.Uint64
	duplicateShreds          atomic.Uint64
	repairShreds             atomic.Uint64
	oldShreds                atomic.Uint64
	queueDrops               atomic.Uint64
	loopbacks                atomic.Uint64
	noPeers                  atomic.Uint64
	invalidParentSignatures  atomic.Uint64
	parentSignaturesVerified atomic.Uint64
	parentSignaturesSkipped  atomic.Uint64
	parentSignatureCacheHits atomic.Uint64
	resignedShreds           atomic.Uint64
	sendErrors               atomic.Uint64
	peerSelectionErrors      atomic.Uint64
	sendSyscallErrors        atomic.Uint64
	shortSendBatches         atomic.Uint64
	retryBatches             atomic.Uint64
	retryPackets             atomic.Uint64
	retrySentPackets         atomic.Uint64
	exhaustedSendBatches     atomic.Uint64
	sendDiagnosticSamples    atomic.Uint64
	parentDiagnosticSamples  atomic.Uint64
	parentSignerFound        atomic.Uint64
	parentSignerNotFound     atomic.Uint64
	parentSourceDirectLeader atomic.Uint64
	parentSourceRepairSocket atomic.Uint64
	parentSourceUnexplained  atomic.Uint64
	rootDistance             [maxTurbineHops]atomic.Uint64
}

type RetransmitStats struct {
	Submitted                uint64
	SentShreds               uint64
	SentPackets              uint64 // datagrams accepted by the local kernel
	TargetPackets            uint64 // original downstream destinations
	PacketAttempts           uint64 // target packets plus retry attempts
	UnsentPackets            uint64 // destinations left after bounded retries
	DuplicateShreds          uint64
	RepairShreds             uint64
	OldShreds                uint64
	QueueDrops               uint64
	Loopbacks                uint64
	NoPeers                  uint64
	InvalidParentSignatures  uint64
	ParentSignaturesVerified uint64
	ParentSignaturesSkipped  uint64
	ParentSignatureCacheHits uint64
	ResignedShreds           uint64
	SendErrors               uint64 // shreds with any peer-selection/send issue
	PeerSelectionErrors      uint64
	SendSyscallErrors        uint64 // WriteBatch calls returning a non-nil error
	ShortSendBatches         uint64 // WriteBatch calls accepting fewer than requested
	RetryBatches             uint64
	RetryPackets             uint64
	RetrySentPackets         uint64
	ExhaustedSendBatches     uint64
	SendDiagnosticSamples    uint64
	ParentDiagnosticSamples  uint64
	ParentSignerFound        uint64
	ParentSignerNotFound     uint64
	ParentSourceDirectLeader uint64
	ParentSourceRepairSocket uint64
	ParentSourceUnexplained  uint64
	SendBufferBytes          int
	LastSendDiagnostic       string
	LastParentDiagnostic     string
	RootDistance             [maxTurbineHops]uint64
}

func NewRetransmitter(cfg RetransmitConfig) (*Retransmitter, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = min(max(runtime.GOMAXPROCS(0)/2, 2), maxRetransmitWorkers)
	}
	senders := make([]packetBatchSender, 0, workers)
	minSendBufferBytes := 0
	for worker := range workers {
		conn, err := net.ListenUDP("udp4", nil)
		if err != nil {
			for _, sender := range senders {
				_ = sender.Close()
			}
			return nil, fmt.Errorf("open turbine retransmit socket: %w", err)
		}
		got := gossip.BoostUDPTransmitBuffer(conn, gossip.TurbineUDPTransmitBufferBytes,
			fmt.Sprintf("turbine retransmit socket %d/%d", worker+1, workers))
		if got > 0 && (minSendBufferBytes == 0 || got < minSendBufferBytes) {
			minSendBufferBytes = got
		}
		senders = append(senders, &udpBatchSender{conn: conn, batch: ipv4.NewPacketConn(conn)})
	}
	retransmitter, err := newRetransmitterWithSenders(cfg, senders)
	if err != nil {
		for _, sender := range senders {
			_ = sender.Close()
		}
		return nil, err
	}
	retransmitter.sendBufferBytes = minSendBufferBytes
	return retransmitter, nil
}

func newRetransmitterWithSenders(cfg RetransmitConfig, senders []packetBatchSender) (*Retransmitter, error) {
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("turbine retransmit identity has invalid size %d", len(cfg.Identity))
	}
	if cfg.Peers == nil {
		return nil, errors.New("turbine retransmit peer source is required")
	}
	if cfg.Stakes == nil {
		return nil, errors.New("turbine retransmit stake source is required")
	}
	if len(senders) == 0 {
		return nil, errors.New("turbine retransmit needs at least one sender")
	}
	queueDepth := cfg.QueueDepth
	if queueDepth <= 0 {
		queueDepth = defaultRetransmitQueueDepth
	}
	pubkey := cfg.Identity.Public().(ed25519.PublicKey)
	var self solana.PublicKey
	copy(self[:], pubkey)
	return &Retransmitter{
		cfg:     cfg,
		self:    self,
		queue:   make(chan retransmitWork, queueDepth),
		senders: senders,
		cache:   make(map[uint64]cachedRetransmitNodes),
		deduper: retransmitDeduper{
			resetAt: time.Now().Add(retransmitDedupResetCycle),
			headers: make(map[retransmitHeader]struct{}),
			counts:  make(map[ShredID]uint8),
		},
	}, nil
}

// Run drains submitted shreds until ctx is canceled. Submit is deliberately
// non-blocking so retransmit congestion cannot starve the receive/repair path.
func (r *Retransmitter) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(len(r.senders))
	for _, sender := range r.senders {
		sender := sender
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case work := <-r.queue:
					r.send(work, sender)
				}
			}
		}()
	}
	<-ctx.Done()
	workers.Wait()
	for _, sender := range r.senders {
		_ = sender.Close()
	}
}

// Submit authenticates and queues one broadcast shred. It returns an error
// only when the packet must be discarded; duplicates, loopbacks, old shreds,
// and local queue pressure merely suppress forwarding.
func (r *Retransmitter) Submit(packet []byte, shred *Shred, leader solana.PublicKey, fromRepair bool) error {
	return r.SubmitFrom(packet, shred, leader, fromRepair, nil)
}

// SubmitFrom is Submit with the UDP source retained for sampled invalid-hop
// diagnostics. The source is observational only and never affects topology.
func (r *Retransmitter) SubmitFrom(packet []byte, shred *Shred, leader solana.PublicKey, fromRepair bool, source *net.UDPAddr) error {
	if r == nil || shred == nil {
		return nil
	}
	if fromRepair {
		r.repairShreds.Add(1)
		return nil
	}
	if r.cfg.RootSlot != nil && shred.Slot < r.cfg.RootSlot() {
		r.oldShreds.Add(1)
		return nil
	}
	if leader == r.self {
		r.loopbacks.Add(1)
		return nil
	}

	id := ShredID{Slot: shred.Slot, Index: shred.Index, Type: shred.Type}
	root, err := r.verifyParentSignature(shred, leader, id, source)
	if err != nil {
		return err
	}
	if !r.deduper.accept(packet, id, time.Now()) {
		r.duplicateShreds.Add(1)
		return nil
	}

	packetSize := len(shred.Payload)
	if packetSize == 0 || packetSize > len(packet) {
		return fmt.Errorf("turbine retransmit: invalid canonical packet size %d/%d", packetSize, len(packet))
	}
	out := append([]byte(nil), packet[:packetSize]...)
	if root != nil {
		offset, err := shred.retransmitterSignatureOffset()
		if err != nil {
			return fmt.Errorf("turbine retransmit: locate retransmitter signature: %w", err)
		}
		if offset+ed25519.SignatureSize > len(out) {
			return fmt.Errorf("turbine retransmit: retransmitter signature slice %d:%d exceeds packet size %d", offset, offset+ed25519.SignatureSize, len(out))
		}
		signature := ed25519.Sign(r.cfg.Identity, root[:])
		copy(out[offset:offset+ed25519.SignatureSize], signature)
		r.resignedShreds.Add(1)
	}

	r.submitted.Add(1)
	select {
	case r.queue <- retransmitWork{packet: out, shred: id, leader: leader}:
	default:
		r.queueDrops.Add(1)
	}
	return nil
}

// verifyParentSignature returns a Merkle root when the packet must be signed
// for the next hop, and nil for ordinary non-resigned variants.
func (r *Retransmitter) verifyParentSignature(shred *Shred, leader solana.PublicKey, id ShredID, source *net.UDPAddr) (*solana.Hash, error) {
	_, _, resigned, ok := merkleVariantInfo(shred.Variant)
	if !ok || !resigned {
		return nil, nil
	}
	root, err := shred.MerkleRoot()
	if err != nil {
		return nil, err
	}
	nodes := r.clusterNodesForSlot(shred.Slot)
	parent, hasParent, err := nodes.RetransmitParent(leader, id, dataPlaneFanout)
	if errors.Is(err, ErrRetransmitLoopback) {
		r.loopbacks.Add(1)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !hasParent {
		r.parentSignaturesSkipped.Add(1)
		return &root, nil
	}
	signature, err := shred.RetransmitterSignature()
	if err != nil {
		r.invalidParentSignatures.Add(1)
		return nil, fmt.Errorf("%w: slot %d shred %d parent %s: %v", ErrInvalidRetransmitterSignature, shred.Slot, shred.Index, parent, err)
	}
	valid, cached := r.parentSigCache.verify(parent, root, signature)
	if cached {
		r.parentSignatureCacheHits.Add(1)
	}
	if !valid {
		r.invalidParentSignatures.Add(1)
		sourceRole, sourceIdentity := r.classifyInvalidParentSource(nodes, leader, source)
		switch sourceRole {
		case parentSourceDirectLeader:
			r.parentSourceDirectLeader.Add(1)
		case parentSourceRepairSocket:
			r.parentSourceRepairSocket.Add(1)
		default:
			r.parentSourceUnexplained.Add(1)
		}
		diagnostic := r.sampleInvalidParentSignature(nodes, id, parent, root, signature, source, sourceRole, sourceIdentity)
		if diagnostic != "" {
			return nil, fmt.Errorf("%w: %s", ErrInvalidRetransmitterSignature, diagnostic)
		}
		return nil, fmt.Errorf("%w: slot %d shred %d parent %s", ErrInvalidRetransmitterSignature, shred.Slot, shred.Index, parent)
	}
	r.parentSignaturesVerified.Add(1)
	return &root, nil
}

func (r *Retransmitter) send(work retransmitWork, sender packetBatchSender) {
	nodes := r.clusterNodesForSlot(work.shred.Slot)
	distance, peers, err := nodes.RetransmitPeers(work.leader, work.shred, dataPlaneFanout)
	if errors.Is(err, ErrRetransmitLoopback) {
		r.loopbacks.Add(1)
		return
	}
	if err != nil {
		r.peerSelectionErrors.Add(1)
		r.sendErrors.Add(1)
		return
	}
	if int(distance) < len(r.rootDistance) {
		r.rootDistance[distance].Add(1)
	}
	if len(peers) == 0 {
		r.noPeers.Add(1)
		return
	}
	r.sendToPeers(work.packet, peers, sender)
}

func (r *Retransmitter) sendToPeers(packet []byte, peers []*net.UDPAddr, sender packetBatchSender) {
	if len(peers) == 0 {
		return
	}
	r.targetPackets.Add(uint64(len(peers)))
	remaining := peers
	totalSent := 0
	hadIssue := false
	var diagnostic *retransmitSendIssue
	for attempt := 1; attempt <= maxRetransmitSendAttempts && len(remaining) > 0; attempt++ {
		requested := len(remaining)
		r.packetAttempts.Add(uint64(requested))
		if attempt > 1 {
			r.retryBatches.Add(1)
			r.retryPackets.Add(uint64(requested))
		}
		sent, err := sender.Send(packet, remaining)
		if sent < 0 || sent > requested {
			err = fmt.Errorf("invalid batch sender result: sent %d of %d: %w", sent, requested, err)
			sent = min(max(sent, 0), requested)
		}
		if sent > 0 {
			totalSent += sent
			r.sentPackets.Add(uint64(sent))
			if attempt > 1 {
				r.retrySentPackets.Add(uint64(sent))
			}
		}
		short := sent != requested
		if err != nil {
			r.sendSyscallErrors.Add(1)
		}
		if short {
			r.shortSendBatches.Add(1)
		}
		if err != nil || short {
			hadIssue = true
			firstUnsent := "none"
			if sent < len(remaining) && remaining[sent] != nil {
				firstUnsent = remaining[sent].String()
			}
			// A short send can initially hide the errno for the first unsent
			// destination. Prefer a later retry which surfaces the syscall error.
			if diagnostic == nil || (diagnostic.err == nil && err != nil) {
				diagnostic = &retransmitSendIssue{
					attempt: attempt, requested: requested, sent: sent,
					firstUnsent: firstUnsent, err: err,
				}
			}
		}
		remaining = remaining[sent:]
		if !short || (err == nil && len(remaining) == 0) {
			break
		}
	}
	if hadIssue {
		r.sendErrors.Add(1)
		r.sampleSendDiagnostic(diagnostic)
	}
	if len(remaining) > 0 {
		r.unsentPackets.Add(uint64(len(remaining)))
		r.exhaustedSendBatches.Add(1)
	}
	if totalSent > 0 {
		r.sentShreds.Add(1)
	}
}

func (r *Retransmitter) clusterNodesForSlot(slot uint64) *ClusterNodes {
	key := slot
	if r.cfg.EpochForSlot != nil {
		key = r.cfg.EpochForSlot(slot)
	}
	now := time.Now()
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	if entry, ok := r.cache[key]; ok && now.Sub(entry.asof) < retransmitNodesCacheTTL {
		return entry.nodes
	}
	stakes := r.cfg.Stakes(slot)
	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{
		Self:         r.self,
		SelfTVU:      r.cfg.Peers.SelfTVUAddr(),
		TVUPeers:     r.cfg.Peers.TVUPeers(),
		Stakes:       stakes,
		DedupTVUAddr: r.cfg.DedupAddrs,
		UseChaCha8:   r.cfg.UseChaCha8,
	})
	if len(r.cache) >= retransmitNodesCacheCap {
		var oldestKey uint64
		var oldest time.Time
		for cacheKey, entry := range r.cache {
			if oldest.IsZero() || entry.asof.Before(oldest) {
				oldestKey, oldest = cacheKey, entry.asof
			}
		}
		delete(r.cache, oldestKey)
	}
	r.cache[key] = cachedRetransmitNodes{asof: now, nodes: nodes}
	return nodes
}

func (r *Retransmitter) sampleSendDiagnostic(issue *retransmitSendIssue) {
	if issue == nil {
		return
	}
	if !claimDiagnosticSample(&r.nextSendDiag, time.Now()) {
		return
	}
	errno := "none"
	if issue.err != nil {
		var sysErr syscall.Errno
		if errors.As(issue.err, &sysErr) {
			errno = fmt.Sprintf("%s(%d)", sysErr.Error(), int(sysErr))
		} else {
			errno = "unclassified"
		}
	}
	diagnostic := fmt.Sprintf("attempt=%d/%d requested=%d sent=%d unsent=%d first_unsent=%s errno=%s error=%q",
		issue.attempt, maxRetransmitSendAttempts, issue.requested, issue.sent, issue.requested-issue.sent,
		issue.firstUnsent, errno, errorString(issue.err))
	r.diagnosticMu.Lock()
	r.lastSendDiag = diagnostic
	r.diagnosticMu.Unlock()
	r.sendDiagnosticSamples.Add(1)
}

func (r *Retransmitter) sampleInvalidParentSignature(
	nodes *ClusterNodes,
	shred ShredID,
	expected solana.PublicKey,
	root solana.Hash,
	signature solana.Signature,
	source *net.UDPAddr,
	sourceRole string,
	sourceIdentity solana.PublicKey,
) string {
	if !claimDiagnosticSample(&r.nextParentDiag, time.Now()) {
		return ""
	}
	actual := solana.PublicKey{}
	for _, node := range nodes.nodes {
		if node.pubkey == expected {
			continue
		}
		if narya.VerifyStrict(node.pubkey[:], root[:], signature[:]) {
			actual = node.pubkey
			break
		}
	}
	actualSigner := "unknown"
	if actual != (solana.PublicKey{}) {
		actualSigner = actual.String()
		r.parentSignerFound.Add(1)
	} else {
		r.parentSignerNotFound.Add(1)
	}
	sourceAddr := "unknown"
	if source != nil {
		sourceAddr = source.String()
	}
	sourceNode := "unknown"
	if sourceIdentity != (solana.PublicKey{}) {
		sourceNode = sourceIdentity.String()
	}
	diagnostic := fmt.Sprintf("slot=%d shred=%d type=%s source=%s source_identity=%s source_role=%s expected_parent=%s actual_signer=%s candidates=%d",
		shred.Slot, shred.Index, retransmitShredTypeName(shred.Type), sourceAddr, sourceNode, sourceRole, expected, actualSigner, len(nodes.nodes))
	r.diagnosticMu.Lock()
	r.lastParentDiag = diagnostic
	r.diagnosticMu.Unlock()
	r.parentDiagnosticSamples.Add(1)
	return diagnostic
}

// classifyInvalidParentSource separates two valid non-tree ingress paths from
// genuine topology disagreements. Leaders send a redundant copy directly to
// the next leader, and serve-repair replies arrive from a peer's advertised
// repair socket; neither packet is evidence that Mithril signed a bad hop.
func (r *Retransmitter) classifyInvalidParentSource(nodes *ClusterNodes, leader solana.PublicKey, source *net.UDPAddr) (string, solana.PublicKey) {
	if source == nil {
		return parentSourceUnexplained, solana.PublicKey{}
	}
	if repairPeers, ok := r.cfg.Peers.(retransmitRepairPeerSource); ok {
		for _, peer := range repairPeers.RepairPeers() {
			if sameUDPAddr(source, peer.Addr) {
				return parentSourceRepairSocket, solana.PublicKey(peer.Pubkey)
			}
		}
	}

	var (
		sourceIdentity solana.PublicKey
		ambiguous      bool
		exact          bool
	)
	for _, node := range nodes.nodes {
		if node.tvuAddr == nil || !source.IP.Equal(node.tvuAddr.IP) {
			continue
		}
		if node.pubkey == leader {
			return parentSourceDirectLeader, leader
		}
		if sameUDPAddr(source, node.tvuAddr) {
			sourceIdentity = node.pubkey
			ambiguous = false
			exact = true
			continue
		}
		if exact {
			continue
		}
		if sourceIdentity == (solana.PublicKey{}) {
			sourceIdentity = node.pubkey
		} else if sourceIdentity != node.pubkey {
			ambiguous = true
		}
	}
	if ambiguous {
		sourceIdentity = solana.PublicKey{}
	}
	return parentSourceUnexplained, sourceIdentity
}

func sameUDPAddr(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}

func retransmitShredTypeName(shredType ShredType) string {
	if shredType == ShredTypeCode {
		return "code"
	}
	return "data"
}

func claimDiagnosticSample(next *atomic.Int64, now time.Time) bool {
	deadline := next.Load()
	if now.UnixNano() < deadline {
		return false
	}
	return next.CompareAndSwap(deadline, now.Add(retransmitDiagnosticPeriod).UnixNano())
}

func errorString(err error) string {
	if err == nil {
		return "none"
	}
	return err.Error()
}

func (r *Retransmitter) Stats() RetransmitStats {
	if r == nil {
		return RetransmitStats{}
	}
	stats := RetransmitStats{
		Submitted:                r.submitted.Load(),
		SentShreds:               r.sentShreds.Load(),
		SentPackets:              r.sentPackets.Load(),
		TargetPackets:            r.targetPackets.Load(),
		PacketAttempts:           r.packetAttempts.Load(),
		UnsentPackets:            r.unsentPackets.Load(),
		DuplicateShreds:          r.duplicateShreds.Load(),
		RepairShreds:             r.repairShreds.Load(),
		OldShreds:                r.oldShreds.Load(),
		QueueDrops:               r.queueDrops.Load(),
		Loopbacks:                r.loopbacks.Load(),
		NoPeers:                  r.noPeers.Load(),
		InvalidParentSignatures:  r.invalidParentSignatures.Load(),
		ParentSignaturesVerified: r.parentSignaturesVerified.Load(),
		ParentSignaturesSkipped:  r.parentSignaturesSkipped.Load(),
		ParentSignatureCacheHits: r.parentSignatureCacheHits.Load(),
		ResignedShreds:           r.resignedShreds.Load(),
		SendErrors:               r.sendErrors.Load(),
		PeerSelectionErrors:      r.peerSelectionErrors.Load(),
		SendSyscallErrors:        r.sendSyscallErrors.Load(),
		ShortSendBatches:         r.shortSendBatches.Load(),
		RetryBatches:             r.retryBatches.Load(),
		RetryPackets:             r.retryPackets.Load(),
		RetrySentPackets:         r.retrySentPackets.Load(),
		ExhaustedSendBatches:     r.exhaustedSendBatches.Load(),
		SendDiagnosticSamples:    r.sendDiagnosticSamples.Load(),
		ParentDiagnosticSamples:  r.parentDiagnosticSamples.Load(),
		ParentSignerFound:        r.parentSignerFound.Load(),
		ParentSignerNotFound:     r.parentSignerNotFound.Load(),
		ParentSourceDirectLeader: r.parentSourceDirectLeader.Load(),
		ParentSourceRepairSocket: r.parentSourceRepairSocket.Load(),
		ParentSourceUnexplained:  r.parentSourceUnexplained.Load(),
		SendBufferBytes:          r.sendBufferBytes,
	}
	r.diagnosticMu.Lock()
	stats.LastSendDiagnostic = r.lastSendDiag
	stats.LastParentDiagnostic = r.lastParentDiag
	r.diagnosticMu.Unlock()
	for i := range stats.RootDistance {
		stats.RootDistance[i] = r.rootDistance[i].Load()
	}
	return stats
}

func (c *retransmitParentSigCache) verify(parent solana.PublicKey, root solana.Hash, signature solana.Signature) (valid, cached bool) {
	key := retransmitParentSigKey{parent: parent, root: root, signature: signature}
	c.mu.Lock()
	if valid, ok := c.cur[key]; ok {
		c.mu.Unlock()
		return valid, true
	}
	if valid, ok := c.prev[key]; ok {
		c.addLocked(key, valid)
		c.mu.Unlock()
		return valid, true
	}
	c.mu.Unlock()

	valid = narya.VerifyStrict(parent[:], root[:], signature[:])
	c.mu.Lock()
	c.addLocked(key, valid)
	c.mu.Unlock()
	return valid, false
}

func (c *retransmitParentSigCache) addLocked(key retransmitParentSigKey, valid bool) {
	if c.cur == nil {
		c.cur = make(map[retransmitParentSigKey]bool, retransmitParentSigCacheCap)
	}
	if len(c.cur) >= retransmitParentSigCacheCap {
		c.prev = c.cur
		c.cur = make(map[retransmitParentSigKey]bool, retransmitParentSigCacheCap)
	}
	c.cur[key] = valid
}

func (d *retransmitDeduper) accept(packet []byte, id ShredID, now time.Time) bool {
	if len(packet) < commonShredHeaderSize {
		return false
	}
	var header retransmitHeader
	copy(header[:], packet[:commonShredHeaderSize])
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.headers == nil {
		d.headers = make(map[retransmitHeader]struct{})
		d.counts = make(map[ShredID]uint8)
		d.resetAt = now.Add(retransmitDedupResetCycle)
	}
	if now.After(d.resetAt) || len(d.headers) >= retransmitDedupCapacity {
		clear(d.headers)
		clear(d.counts)
		d.resetAt = now.Add(retransmitDedupResetCycle)
	}
	if _, duplicate := d.headers[header]; duplicate {
		return false
	}
	if d.counts[id] >= maxDuplicateShreds {
		return false
	}
	d.headers[header] = struct{}{}
	d.counts[id]++
	return true
}

func (s *udpBatchSender) Send(packet []byte, peers []*net.UDPAddr) (int, error) {
	if cap(s.messages) < len(peers) {
		s.messages = make([]ipv4.Message, len(peers))
		s.buffers = make([][]byte, len(peers))
	} else {
		s.messages = s.messages[:len(peers)]
		s.buffers = s.buffers[:len(peers)]
	}
	for i, peer := range peers {
		s.buffers[i] = packet
		s.messages[i] = ipv4.Message{Buffers: s.buffers[i : i+1], Addr: peer}
	}
	sent, err := s.batch.WriteBatch(s.messages, 0)
	for i := range peers {
		s.messages[i] = ipv4.Message{}
		s.buffers[i] = nil
	}
	return sent, err
}

func (s *udpBatchSender) Close() error {
	return s.conn.Close()
}

// Compile-time check that gossip.Client remains a complete retransmit source.
var _ TVUPeerSource = (*gossip.Client)(nil)
