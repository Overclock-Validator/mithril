package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
)

const (
	serveRepairMaxRequestsPerSecond = 100
	serveRepairRateLimitWindow      = time.Second
	serveRepairTimestampTolerance   = 10 * time.Minute
	serveRepairRateLimitCleanup     = time.Minute
	serveRepairRateLimitIdle        = 30 * time.Second
	serveRepairMaxTrackedPeers      = 64 << 10
	serveRepairQueueDepth           = 4096
	serveRepairMaxWorkers           = 4
	serveRepairErrorLogInterval     = 10 * time.Second
)

// ServeRepairStats is the cumulative Solana serve-repair service picture.
type ServeRepairStats struct {
	Requests           uint64
	WindowIndex        uint64
	HighestWindowIndex uint64
	Served             uint64
	Pings              uint64
	Pongs              uint64
	RateLimited        uint64
	QueueDrops         uint64
	DropMalformed      uint64
	DropHeaderInvalid  uint64
	DropSignature      uint64
	DropNotFound       uint64
	DropStoreError     uint64
	DropOversized      uint64
	SendErrors         uint64
}

func (s ServeRepairStats) Dropped() uint64 {
	return s.QueueDrops + s.DropMalformed + s.DropHeaderInvalid + s.DropSignature +
		s.DropNotFound + s.DropStoreError + s.DropOversized
}

type serveRepairCounters struct {
	requests           atomic.Uint64
	windowIndex        atomic.Uint64
	highestWindowIndex atomic.Uint64
	served             atomic.Uint64
	pings              atomic.Uint64
	pongs              atomic.Uint64
	rateLimited        atomic.Uint64
	queueDrops         atomic.Uint64
	dropMalformed      atomic.Uint64
	dropHeaderInvalid  atomic.Uint64
	dropSignature      atomic.Uint64
	dropNotFound       atomic.Uint64
	dropStoreError     atomic.Uint64
	dropOversized      atomic.Uint64
	sendErrors         atomic.Uint64
}

type serveRepairPeerLimit struct {
	count       uint32
	windowStart time.Time
	lastSeen    time.Time
}

func (l *serveRepairPeerLimit) allow(now time.Time) bool {
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= serveRepairRateLimitWindow || now.Before(l.windowStart) {
		l.count = 0
		l.windowStart = now
	}
	l.lastSeen = now
	if l.count >= serveRepairMaxRequestsPerSecond {
		return false
	}
	l.count++
	return true
}

type serveRepairWork struct {
	packet   [repairproto.RequestPacketSize]byte
	signable [repairproto.RequestSignableSize]byte
	request  repairproto.Request
	addr     netip.AddrPort
}

type ServeRepairConfig struct {
	Addr     string
	Identity ed25519.PrivateKey
	Store    *ShredSpool
}

// ServeRepairServer answers modern signed Solana WindowIndex and
// HighestWindowIndex requests from Mithril's verified shred store.
type ServeRepairServer struct {
	conn     *net.UDPConn
	identity ed25519.PrivateKey
	self     gossip.Pubkey
	store    *ShredSpool

	limits      map[netip.AddrPort]*serveRepairPeerLimit
	lastCleanup time.Time
	now         func() time.Time
	counters    serveRepairCounters

	closeOnce         sync.Once
	nextStoreErrorLog atomic.Int64
}

func NewServeRepairServer(cfg ServeRepairConfig) (*ServeRepairServer, error) {
	if cfg.Addr == "" {
		return nil, errors.New("serve repair bind address is required")
	}
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("serve repair identity has invalid size %d", len(cfg.Identity))
	}
	if cfg.Store == nil {
		return nil, errors.New("serve repair shred store is required")
	}
	addr, err := net.ResolveUDPAddr("udp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("resolve serve repair bind %q: %w", cfg.Addr, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen serve repair udp %q: %w", cfg.Addr, err)
	}
	_ = conn.SetReadBuffer(gossip.GossipUDPReceiveBufferBytes)
	_ = conn.SetWriteBuffer(gossip.TurbineUDPTransmitBufferBytes)

	identity := append(ed25519.PrivateKey(nil), cfg.Identity...)
	pub := identity.Public().(ed25519.PublicKey)
	var self gossip.Pubkey
	copy(self[:], pub)
	now := time.Now()
	return &ServeRepairServer{
		conn:        conn,
		identity:    identity,
		self:        self,
		store:       cfg.Store,
		limits:      make(map[netip.AddrPort]*serveRepairPeerLimit),
		lastCleanup: now,
		now:         time.Now,
	}, nil
}

func (s *ServeRepairServer) Addr() *net.UDPAddr {
	if s == nil || s.conn == nil {
		return nil
	}
	addr, _ := s.conn.LocalAddr().(*net.UDPAddr)
	if addr == nil {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port, Zone: addr.Zone}
}

func (s *ServeRepairServer) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() { err = s.conn.Close() })
	return err
}

func (s *ServeRepairServer) Run(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return errors.New("serve repair server is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stopClose := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stopClose()

	workers := runtime.GOMAXPROCS(0) / 4
	if workers < 1 {
		workers = 1
	}
	if workers > serveRepairMaxWorkers {
		workers = serveRepairMaxWorkers
	}
	work := make(chan serveRepairWork, serveRepairQueueDepth)
	var workerWG sync.WaitGroup
	workerWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer workerWG.Done()
			s.runWorker(ctx, work, workers)
		}()
	}
	defer func() {
		close(work)
		workerWG.Wait()
	}()

	buf := make([]byte, packetDataSize)
	for {
		n, addr, err := s.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("read serve repair udp: %w", err)
		}
		s.handlePacket(buf[:n], addr, work)
	}
}

func (s *ServeRepairServer) handlePacket(packet []byte, addr netip.AddrPort, work chan<- serveRepairWork) {
	now := s.now()
	s.cleanupRateLimits(now)
	limit := s.limits[addr]
	if limit == nil {
		if len(s.limits) >= serveRepairMaxTrackedPeers {
			s.counters.rateLimited.Add(1)
			return
		}
		limit = &serveRepairPeerLimit{}
		s.limits[addr] = limit
	}
	if !limit.allow(now) {
		s.counters.rateLimited.Add(1)
		return
	}

	if ping, ok := repairproto.DecodePing(packet); ok {
		s.counters.pings.Add(1)
		pong, err := repairproto.BuildPong(s.identity, ping)
		if err != nil {
			s.counters.sendErrors.Add(1)
			return
		}
		if _, err := s.conn.WriteToUDPAddrPort(pong, addr); err != nil {
			s.counters.sendErrors.Add(1)
			return
		}
		s.counters.pongs.Add(1)
		return
	}
	if repairproto.IsPong(packet) {
		s.counters.pongs.Add(1)
		return
	}

	request, ok := repairproto.DecodeRequest(packet)
	if !ok {
		s.counters.dropMalformed.Add(1)
		return
	}
	s.counters.requests.Add(1)
	switch request.Kind {
	case repairproto.RequestWindowIndex:
		s.counters.windowIndex.Add(1)
	case repairproto.RequestHighestWindowIndex:
		s.counters.highestWindowIndex.Add(1)
	}
	if !s.validHeader(request, now) {
		s.counters.dropHeaderInvalid.Add(1)
		return
	}

	var item serveRepairWork
	copy(item.packet[:], packet)
	if !repairproto.CopyRequestSignable(item.signable[:], item.packet[:]) {
		s.counters.dropMalformed.Add(1)
		return
	}
	item.request = request
	item.addr = addr
	select {
	case work <- item:
	default:
		s.counters.queueDrops.Add(1)
	}
}

func (s *ServeRepairServer) validHeader(request repairproto.Request, now time.Time) bool {
	if request.Sender == s.self || request.Recipient != s.self {
		return false
	}
	nowMillis := now.UnixMilli()
	if nowMillis < 0 {
		return false
	}
	current := uint64(nowMillis)
	if request.Timestamp > current {
		return request.Timestamp-current <= uint64(serveRepairTimestampTolerance/time.Millisecond)
	}
	return current-request.Timestamp <= uint64(serveRepairTimestampTolerance/time.Millisecond)
}

func (s *ServeRepairServer) cleanupRateLimits(now time.Time) {
	if now.Sub(s.lastCleanup) < serveRepairRateLimitCleanup && !now.Before(s.lastCleanup) {
		return
	}
	for addr, limit := range s.limits {
		if now.Sub(limit.lastSeen) >= serveRepairRateLimitIdle || now.Before(limit.lastSeen) {
			delete(s.limits, addr)
		}
	}
	s.lastCleanup = now
}

func (s *ServeRepairServer) runWorker(ctx context.Context, work <-chan serveRepairWork, workers int) {
	var (
		group    []serveRepairWork
		verifier sigverify.Batch
		response [packetDataSize]byte
	)
	for first := range work {
		if ctx.Err() != nil {
			return
		}
		group = sigverify.Drain(group, first, work,
			sigverify.FairShare(len(work), workers, sigverify.MaxDrain))
		verifier.Reset()
		for i := range group {
			signature, _ := repairproto.RequestSignature(group[i].packet[:])
			verifier.Add((*[32]byte)(&group[i].request.Sender), group[i].signable[:], signature)
		}
		verifier.Verify()
		for i := range group {
			if !verifier.OK(i) {
				s.counters.dropSignature.Add(1)
				continue
			}
			s.serve(group[i], response[:])
		}
		clear(group)
	}
}

func (s *ServeRepairServer) serve(work serveRepairWork, response []byte) {
	var (
		shred []byte
		ok    bool
		err   error
	)
	switch work.request.Kind {
	case repairproto.RequestWindowIndex:
		shred, ok, err = s.store.GetDataShred(work.request.Slot, work.request.ShredIndex)
	case repairproto.RequestHighestWindowIndex:
		shred, ok, err = s.store.GetHighestDataShredFrom(work.request.Slot, work.request.ShredIndex)
	default:
		s.counters.dropMalformed.Add(1)
		return
	}
	if err != nil {
		s.counters.dropStoreError.Add(1)
		s.sampleStoreError(err)
		return
	}
	if !ok {
		s.counters.dropNotFound.Add(1)
		return
	}
	if len(shred)+4 > len(response) {
		s.counters.dropOversized.Add(1)
		return
	}
	n := copy(response, shred)
	binary.LittleEndian.PutUint32(response[n:n+4], work.request.Nonce)
	if written, err := s.conn.WriteToUDPAddrPort(response[:n+4], work.addr); err != nil || written != n+4 {
		s.counters.sendErrors.Add(1)
		return
	}
	s.counters.served.Add(1)
}

func (s *ServeRepairServer) sampleStoreError(err error) {
	now := time.Now().UnixNano()
	deadline := s.nextStoreErrorLog.Load()
	if now < deadline || !s.nextStoreErrorLog.CompareAndSwap(deadline, now+serveRepairErrorLogInterval.Nanoseconds()) {
		return
	}
	mlog.Log.FileOnlyf("serve repair shred-store lookup failed: %v", err)
}

func (s *ServeRepairServer) Stats() ServeRepairStats {
	if s == nil {
		return ServeRepairStats{}
	}
	return ServeRepairStats{
		Requests:           s.counters.requests.Load(),
		WindowIndex:        s.counters.windowIndex.Load(),
		HighestWindowIndex: s.counters.highestWindowIndex.Load(),
		Served:             s.counters.served.Load(),
		Pings:              s.counters.pings.Load(),
		Pongs:              s.counters.pongs.Load(),
		RateLimited:        s.counters.rateLimited.Load(),
		QueueDrops:         s.counters.queueDrops.Load(),
		DropMalformed:      s.counters.dropMalformed.Load(),
		DropHeaderInvalid:  s.counters.dropHeaderInvalid.Load(),
		DropSignature:      s.counters.dropSignature.Load(),
		DropNotFound:       s.counters.dropNotFound.Load(),
		DropStoreError:     s.counters.dropStoreError.Load(),
		DropOversized:      s.counters.dropOversized.Load(),
		SendErrors:         s.counters.sendErrors.Load(),
	}
}
