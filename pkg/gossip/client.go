package gossip

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const (
	DefaultBindAddr      = "0.0.0.0:0"
	defaultPushInterval  = 2 * time.Second
	defaultPingInterval  = 10 * time.Second
	defaultEchoTimeout   = 5 * time.Second
	maxKnownGossipPeers  = 128
	peerExpirationWindow = 15 * time.Minute
)

var ErrIdentityInUse = errors.New("validator identity is already active in gossip")

type Config struct {
	Entrypoint        string
	BindAddr          string
	TVUAddr           string
	AlpenglowAddr     string
	AdvertisedIP      string
	ShredVersion      uint16
	Identity          *identity.Holder // signs this node's gossip; nil runs with an ephemeral identity
	PushInterval      time.Duration
	PingInterval      time.Duration
	EntrypointTimeout time.Duration
	Name              string
}

type Client struct {
	cfg Config

	entrypoint *net.UDPAddr
	bindAddr   *net.UDPAddr
	tvuAddr    *net.UDPAddr
	alpenglow  *net.UDPAddr
	identity   *identity.Holder

	contactMu sync.RWMutex
	// contact is replaced on every update and never modified once published,
	// so readers may use it after releasing contactMu.
	contact *ContactInfo
	// pendingContact holds updates made before Run initializes contact.
	pendingContact []func(*ContactInfo) error
	contactChanged chan struct{}
	relay          contactRelay

	identityConflictMu        sync.Mutex
	identityConflictGossip    contactEndpoint
	identityConflictTVU       contactEndpoint
	identityConflictAlpenglow contactEndpoint
	identityConflictWallclock uint64
	identityConflictCh        chan error

	peerMu sync.Mutex
	peers  map[udpAddrKey]knownPeer

	repairPeerMu sync.Mutex
	repairPeers  map[Pubkey]RepairPeer

	tvuPeerMu sync.Mutex
	tvuPeers  map[Pubkey]TVUPeer

	alpenglowPeerMu sync.Mutex
	alpenglowPeers  map[Pubkey]AlpenglowPeer

	rxPackets        atomic.Uint64
	txPackets        atomic.Uint64
	rxDecodeErrors   atomic.Uint64
	txErrors         atomic.Uint64
	rxPings          atomic.Uint64
	rxPongs          atomic.Uint64
	rxContacts       atomic.Uint64
	acceptedContacts atomic.Uint64
	rxPullRequests   atomic.Uint64
	txPushMessages   atomic.Uint64
	txPingMessages   atomic.Uint64
	txPongMessages   atomic.Uint64
	txPullResponses  atomic.Uint64
	lastRxUnix       atomic.Int64
	lastTxUnix       atomic.Int64
}

type knownPeer struct {
	addr     *net.UDPAddr
	lastSeen time.Time
}

type udpAddrKey struct {
	ip   [16]byte
	port int
}

type RepairPeer struct {
	Pubkey   Pubkey
	Addr     *net.UDPAddr
	LastSeen time.Time
}

// TVUPeer is a gossip contact with a TVU UDP socket for turbine broadcast.
type TVUPeer struct {
	Pubkey   Pubkey
	TVUAddr  *net.UDPAddr
	LastSeen time.Time
	version  contactVersion
}

// AlpenglowPeer is a gossip contact's Votor QUIC endpoint. Voting code applies
// the epoch validator-set filter before using these contacts.
type AlpenglowPeer struct {
	Pubkey        Pubkey
	AlpenglowAddr *net.UDPAddr
	LastSeen      time.Time
}

type Stats struct {
	RxPackets        uint64
	TxPackets        uint64
	RxDecodeErrors   uint64
	TxErrors         uint64
	RxPings          uint64
	RxPongs          uint64
	RxContacts       uint64
	AcceptedContacts uint64
	RxPullRequests   uint64
	TxPushMessages   uint64
	TxPingMessages   uint64
	TxPongMessages   uint64
	TxPullResponses  uint64
	LastRxUnix       int64
	LastTxUnix       int64
	Peers            int
	RepairPeers      int
	AlpenglowPeers   int
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.Name == "" {
		cfg.Name = ClientName
	}
	if cfg.Entrypoint == "" {
		return nil, fmt.Errorf("gossip entrypoint is required")
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = DefaultBindAddr
	}
	if cfg.PushInterval == 0 {
		cfg.PushInterval = defaultPushInterval
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = defaultPingInterval
	}
	if cfg.EntrypointTimeout == 0 {
		cfg.EntrypointTimeout = defaultEchoTimeout
	}
	entrypoint, err := net.ResolveUDPAddr("udp", cfg.Entrypoint)
	if err != nil {
		return nil, fmt.Errorf("resolve gossip entrypoint: %w", err)
	}
	bindAddr, err := net.ResolveUDPAddr("udp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve gossip bind address: %w", err)
	}
	tvuAddr, err := net.ResolveUDPAddr("udp", cfg.TVUAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve TVU address: %w", err)
	}
	var alpenglowAddr *net.UDPAddr
	if cfg.AlpenglowAddr != "" {
		alpenglowAddr, err = net.ResolveUDPAddr("udp", cfg.AlpenglowAddr)
		if err != nil {
			return nil, fmt.Errorf("resolve Alpenglow address: %w", err)
		}
	}
	holder, err := identity.HolderOrEphemeral(cfg.Identity)
	if err != nil {
		return nil, fmt.Errorf("gossip identity: %w", err)
	}

	return &Client{
		cfg:                cfg,
		entrypoint:         entrypoint,
		bindAddr:           bindAddr,
		tvuAddr:            tvuAddr,
		alpenglow:          alpenglowAddr,
		identity:           holder,
		identityConflictCh: make(chan error, 1),
		contactChanged:     make(chan struct{}, 1),
		peers:              make(map[udpAddrKey]knownPeer),
		repairPeers:        make(map[Pubkey]RepairPeer),
		tvuPeers:           make(map[Pubkey]TVUPeer),
		alpenglowPeers:     make(map[Pubkey]AlpenglowPeer),
	}, nil
}

func (c *Client) Pubkey() Pubkey {
	return Pubkey(c.identity.PublicKey())
}

// Identity is the holder this client signs with. Components that share the
// client's gossip identity, such as turbine retransmit and repair, read it.
func (c *Client) Identity() *identity.Holder {
	return c.identity
}

// adoptIdentity restarts this client's contact under the new identity, as
// Agave's ClusterInfo::set_keypair does. A fresh outset and wallclock make
// CRDS values the previous owner of the identity published before the swap
// stale, so they cannot trip the identity-conflict check.
func (c *Client) adoptIdentity(prev, next *identity.Keypair) {
	c.contactMu.Lock()
	if c.contact != nil {
		contact := c.contact.CloneWithWallclock(wallclockMillis())
		contact.Pubkey = Pubkey(next.PublicKey())
		contact.Outset = uint64(time.Now().UnixMicro())
		c.contact = contact
	}
	c.contactMu.Unlock()

	c.identityConflictMu.Lock()
	c.identityConflictGossip = contactEndpoint{}
	c.identityConflictTVU = contactEndpoint{}
	c.identityConflictAlpenglow = contactEndpoint{}
	c.identityConflictWallclock = 0
	c.identityConflictMu.Unlock()

	mlog.Log.Infof("gossip identity changed from %s to %s; advertising new contact info", prev.PublicKey(), next.PublicKey())
	c.notifyContactChanged()
}

func (c *Client) ShredVersion() uint16 {
	c.contactMu.RLock()
	defer c.contactMu.RUnlock()
	if c.contact == nil {
		return c.cfg.ShredVersion
	}
	return c.contact.ShredVer
}

// AdvertisedGossipAddr returns the socket in this client's current signed
// ContactInfo. It is nil until Run has initialized the contact.
func (c *Client) AdvertisedGossipAddr() *net.UDPAddr {
	c.contactMu.RLock()
	defer c.contactMu.RUnlock()
	if c.contact == nil {
		return nil
	}
	return cloneUDPAddr(c.contact.GossipAddr)
}

func (c *Client) Run(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", c.bindAddr)
	if err != nil {
		return fmt.Errorf("bind gossip socket %s: %w", c.bindAddr, err)
	}
	defer conn.Close()
	BoostUDPReceiveBuffer(conn, GossipUDPReceiveBufferBytes, "gossip client")

	if err := c.initializeContact(conn.LocalAddr().(*net.UDPAddr)); err != nil {
		return err
	}
	defer c.identity.OnSwap(c.adoptIdentity)()
	c.contactMu.RLock()
	contact := c.contact
	c.contactMu.RUnlock()
	if contact != nil {
		alpenglowAddr := "disabled"
		tpuVoteAddr := "disabled"
		tpuVoteQuicAddr := "disabled"
		if contact.AlpenglowAddr != nil {
			alpenglowAddr = contact.AlpenglowAddr.String()
		}
		if contact.TPUVoteAddr != nil {
			tpuVoteAddr = contact.TPUVoteAddr.String()
		}
		if contact.TPUVoteQuicAddr != nil {
			tpuVoteQuicAddr = contact.TPUVoteQuicAddr.String()
		}
		mlog.Log.Infof("gossip joined: advertised tvu=%s alpenglow=%s | shred_version=%d",
			contact.TVUAddr.String(), alpenglowAddr, contact.ShredVer)
		mlog.Log.FileOnlyf("gossip client listening: local=%s advertised_gossip=%s advertised_tvu=%s advertised_alpenglow=%s advertised_tpu_vote=%s advertised_tpu_vote_quic=%s shred_version=%d client=%s",
			conn.LocalAddr().String(), contact.GossipAddr.String(), contact.TVUAddr.String(), alpenglowAddr, tpuVoteAddr, tpuVoteQuicAddr, contact.ShredVer, c.cfg.Name)
	}
	c.recordPeer(c.entrypoint)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.recvLoop(ctx, conn)
	}()

	if err := c.pushContact(conn); err != nil {
		return err
	}
	_ = c.sendPing(conn, c.entrypoint)

	pushTicker := time.NewTicker(c.cfg.PushInterval)
	defer pushTicker.Stop()
	pingTicker := time.NewTicker(c.cfg.PingInterval)
	defer pingTicker.Stop()
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err == nil || ctx.Err() != nil {
				return nil
			}
			return err
		case err := <-c.identityConflictCh:
			return err
		case <-pushTicker.C:
			if err := c.pushContact(conn); err != nil {
				return err
			}
		case <-c.contactChanged:
			if err := c.pushContact(conn); err != nil {
				return err
			}
		case <-pingTicker.C:
			_ = c.sendPing(conn, c.entrypoint)
			// Pull peers' CRDS tables so discovery isn't purely passive (fixes
			// peers=1). Peers ping us first if unponged; we answer, so this converges.
			_ = c.sendPullRequests(conn)
		case <-statsTicker.C:
			stats := c.Stats()
			mlog.Log.FileOnlyf("gossip client stats: rx=%d tx=%d peers=%d repair_peers=%d decode_errors=%d tx_errors=%d pings=%d/%d pongs=%d/%d contacts_rx=%d contacts_accepted=%d pull_requests=%d pull_responses=%d last_rx=%s last_tx=%s",
				stats.RxPackets, stats.TxPackets, stats.Peers, stats.RepairPeers, stats.RxDecodeErrors, stats.TxErrors,
				stats.RxPings, stats.TxPingMessages, stats.RxPongs, stats.TxPongMessages,
				stats.RxContacts, stats.AcceptedContacts, stats.RxPullRequests, stats.TxPullResponses,
				formatAge(stats.LastRxUnix), formatAge(stats.LastTxUnix))
		}
	}
}

func (c *Client) Stats() Stats {
	c.peerMu.Lock()
	peers := len(c.peers)
	c.peerMu.Unlock()
	c.repairPeerMu.Lock()
	repairPeers := len(c.repairPeers)
	c.repairPeerMu.Unlock()
	c.alpenglowPeerMu.Lock()
	alpenglowPeers := len(c.alpenglowPeers)
	c.alpenglowPeerMu.Unlock()
	return Stats{
		RxPackets:        c.rxPackets.Load(),
		TxPackets:        c.txPackets.Load(),
		RxDecodeErrors:   c.rxDecodeErrors.Load(),
		TxErrors:         c.txErrors.Load(),
		RxPings:          c.rxPings.Load(),
		RxPongs:          c.rxPongs.Load(),
		RxContacts:       c.rxContacts.Load(),
		AcceptedContacts: c.acceptedContacts.Load(),
		RxPullRequests:   c.rxPullRequests.Load(),
		TxPushMessages:   c.txPushMessages.Load(),
		TxPingMessages:   c.txPingMessages.Load(),
		TxPongMessages:   c.txPongMessages.Load(),
		TxPullResponses:  c.txPullResponses.Load(),
		LastRxUnix:       c.lastRxUnix.Load(),
		LastTxUnix:       c.lastTxUnix.Load(),
		Peers:            peers,
		RepairPeers:      repairPeers,
		AlpenglowPeers:   alpenglowPeers,
	}
}

func (c *Client) RepairPeers() []RepairPeer {
	now := time.Now()
	c.repairPeerMu.Lock()
	defer c.repairPeerMu.Unlock()
	for key, peer := range c.repairPeers {
		if now.Sub(peer.LastSeen) > peerExpirationWindow {
			delete(c.repairPeers, key)
		}
	}
	peers := make([]RepairPeer, 0, len(c.repairPeers))
	for _, peer := range c.repairPeers {
		peers = append(peers, peer)
	}
	return peers
}

// SetTPUQUIC updates the TPU QUIC endpoints published in this client's CRDS contact.
// It may be called before Run has initialized the contact record.
func (c *Client) SetTPUQUIC(addr *net.UDPAddr) error {
	addr = cloneUDPAddr(addr)
	return c.UpdateContact(func(contact *ContactInfo) error {
		return contact.SetTPUQUIC(addr)
	})
}

// SetTPUSockets publishes exactly the given TPU endpoints; see
// ContactInfo.SetTPUSockets.
func (c *Client) SetTPUSockets(tpu TPUSockets) error {
	tpu = TPUSockets{
		UDP:          cloneUDPAddr(tpu.UDP),
		QUIC:         cloneUDPAddr(tpu.QUIC),
		ForwardsUDP:  cloneUDPAddr(tpu.ForwardsUDP),
		ForwardsQUIC: cloneUDPAddr(tpu.ForwardsQUIC),
	}
	return c.UpdateContact(func(contact *ContactInfo) error {
		return contact.SetTPUSockets(tpu)
	})
}

// SetClientID sets the client ID advertised in this client's contact.
func (c *Client) SetClientID(id uint16) error {
	return c.UpdateContact(func(contact *ContactInfo) error {
		contact.SetClientID(id)
		return nil
	})
}

// UpdateContact applies fn to a copy of this client's contact and publishes
// the copy, so a concurrent push never sees a partial update, then pushes it
// without waiting for the next push interval. Nothing changes if fn fails or
// leaves the contact in a form peers reject. Updates made before Run
// initializes the contact are applied in order when it does. fn runs with the
// contact lock held and must not call back into the client.
func (c *Client) UpdateContact(fn func(*ContactInfo) error) error {
	c.contactMu.Lock()
	defer c.contactMu.Unlock()
	if c.contact == nil {
		c.pendingContact = append(c.pendingContact, fn)
		return nil
	}
	next, err := updatedContact(c.contact, fn)
	if err != nil {
		return err
	}
	c.contact = next
	c.notifyContactChanged()
	return nil
}

func (c *Client) notifyContactChanged() {
	select {
	case c.contactChanged <- struct{}{}:
	default:
	}
}

func updatedContact(current *ContactInfo, fn func(*ContactInfo) error) (*ContactInfo, error) {
	next := current.CloneWithWallclock(current.Wallclock)
	if err := fn(next); err != nil {
		return nil, err
	}
	if err := next.validateEntries(); err != nil {
		return nil, err
	}
	return next, nil
}

func (c *Client) initializeContact(localGossipAddr *net.UDPAddr) error {
	shredVersion := c.cfg.ShredVersion
	var advertisedIP net.IP
	if c.cfg.AdvertisedIP != "" {
		advertisedIP = net.ParseIP(c.cfg.AdvertisedIP)
		if advertisedIP == nil {
			return fmt.Errorf("invalid advertised IP %q", c.cfg.AdvertisedIP)
		}
	}

	if shredVersion == 0 || advertisedIP == nil {
		echo, err := QueryEntrypoint(c.entrypoint, c.cfg.EntrypointTimeout)
		if err != nil {
			if shredVersion == 0 {
				return fmt.Errorf("query gossip entrypoint %s: %w", c.entrypoint.String(), err)
			}
		} else {
			if shredVersion == 0 {
				shredVersion = echo.ShredVersion
			}
			if advertisedIP == nil {
				advertisedIP = echo.Address
			}
		}
	}
	if advertisedIP == nil {
		advertisedIP = localGossipAddr.IP
	}
	advertisedIP = normalizedIP(advertisedIP)
	if advertisedIP == nil || advertisedIP.IsUnspecified() {
		return fmt.Errorf("could not determine advertised gossip IP")
	}
	if shredVersion == 0 {
		return fmt.Errorf("could not determine shred version")
	}

	gossipAddr := &net.UDPAddr{IP: advertisedIP, Port: localGossipAddr.Port}
	tvuAddr := &net.UDPAddr{IP: advertisedIP, Port: c.tvuAddr.Port}
	if tvuIP := normalizedIP(c.tvuAddr.IP); tvuIP != nil && !tvuIP.IsUnspecified() {
		tvuAddr.IP = tvuIP
	}

	contact, err := NewContactInfo(Pubkey(c.identity.PublicKey()), shredVersion, gossipAddr, tvuAddr)
	if err != nil {
		return err
	}
	if c.alpenglow != nil {
		alpenglowAddr := &net.UDPAddr{IP: advertisedIP, Port: c.alpenglow.Port}
		if alpenglowIP := normalizedIP(c.alpenglow.IP); alpenglowIP != nil && !alpenglowIP.IsUnspecified() {
			alpenglowAddr.IP = alpenglowIP
		}
		if err := contact.SetAlpenglowAddr(alpenglowAddr); err != nil {
			return fmt.Errorf("set Alpenglow gossip socket: %w", err)
		}
	}
	c.contactMu.Lock()
	defer c.contactMu.Unlock()
	for _, fn := range c.pendingContact {
		next, err := updatedContact(contact, fn)
		if err != nil {
			return fmt.Errorf("apply pending gossip contact update: %w", err)
		}
		contact = next
	}
	c.pendingContact = nil
	c.contact = contact
	return nil
}

// currentContactValue signs the contact with the current identity. The pubkey
// is taken from the same keypair that signs, so a swap racing this call can
// never publish one identity's contact under another's signature.
func (c *Client) currentContactValue() (CrdsValue, *identity.Keypair, error) {
	c.contactMu.RLock()
	contact := c.contact
	c.contactMu.RUnlock()
	if contact == nil {
		return CrdsValue{}, nil, fmt.Errorf("gossip contact info is not initialized")
	}
	kp := c.identity.Current()
	value := contact.CloneWithWallclock(wallclockMillis())
	value.Pubkey = Pubkey(kp.PublicKey())
	signed, err := signCrdsContactInfo(value, kp.PrivateKey())
	return signed, kp, err
}

func (c *Client) pushContact(conn *net.UDPConn) error {
	// Standalone gossip clients may have no consumer calling TVUPeers.
	c.tvuPeerMu.Lock()
	c.pruneTVUPeersLocked(time.Now())
	c.tvuPeerMu.Unlock()
	value, kp, err := c.currentContactValue()
	if err != nil {
		return err
	}
	packet, err := encodePushMessage(Pubkey(kp.PublicKey()), []CrdsValue{value})
	if err != nil {
		return err
	}
	packets := [][]byte{packet}
	if values := c.relay.values(wallclockMillis()); len(values) != 0 {
		packet, err := encodePushMessage(Pubkey(kp.PublicKey()), values)
		if err != nil {
			return err
		}
		packets = append(packets, packet)
	}
	for _, peer := range c.currentPeers() {
		for _, packet := range packets {
			if err := sendUDP(conn, packet, peer); err != nil {
				c.txErrors.Add(1)
				break
			}
			c.recordTx()
			c.txPushMessages.Add(1)
		}
	}
	return nil
}

func (c *Client) sendPullResponse(conn *net.UDPConn, addr *net.UDPAddr) error {
	value, kp, err := c.currentContactValue()
	if err != nil {
		return err
	}
	packet, err := encodePullResponse(Pubkey(kp.PublicKey()), []CrdsValue{value})
	if err != nil {
		return err
	}
	if err := sendUDP(conn, packet, addr); err != nil {
		c.txErrors.Add(1)
		return err
	}
	c.recordTx()
	c.txPullResponses.Add(1)
	return nil
}

// sendPullRequests asks every known peer to dump its CRDS table (match-everything
// filter), bootstrapping our peer/repair set. Peers ping us first if we're not yet
// ponged; we answer pings, so repeated ticks converge. Best-effort per peer.
func (c *Client) sendPullRequests(conn *net.UDPConn) error {
	value, _, err := c.currentContactValue()
	if err != nil {
		return err
	}
	var seedBytes [8]byte
	if _, err := rand.Read(seedBytes[:]); err != nil {
		return err
	}
	packet, err := encodePullRequest(value, binary.LittleEndian.Uint64(seedBytes[:]))
	if err != nil {
		return err
	}
	// Pull from a bounded window each tick rather than flooding every peer (avoids
	// peers rate-limiting our gossip, which the shared turbine path relies on). Start
	// offset is derived from the random seed so the window rotates over time.
	peers := c.currentPeers()
	if len(peers) == 0 {
		return nil
	}
	start := int(binary.LittleEndian.Uint64(seedBytes[:]) % uint64(len(peers)))
	n := maxPullRequestsPerTick
	if n > len(peers) {
		n = len(peers)
	}
	for i := 0; i < n; i++ {
		if err := sendUDP(conn, packet, peers[(start+i)%len(peers)]); err != nil {
			c.txErrors.Add(1)
			continue
		}
		c.recordTx()
	}
	return nil
}

// maxPullRequestsPerTick bounds outbound CRDS pull requests per ping tick so a
// large peer table doesn't turn into a gossip flood.
const maxPullRequestsPerTick = 8

func (c *Client) sendPing(conn *net.UDPConn, addr *net.UDPAddr) error {
	ping, err := newPing(c.identity.Current().PrivateKey())
	if err != nil {
		return err
	}
	if err := sendUDP(conn, encodePingMessage(ping), addr); err != nil {
		c.txErrors.Add(1)
		return err
	}
	c.recordTx()
	c.txPingMessages.Add(1)
	return nil
}

func (c *Client) sendPong(conn *net.UDPConn, ping Ping, addr *net.UDPAddr) error {
	if !ping.Verify() {
		return nil
	}
	pong, err := newPong(ping, c.identity.Current().PrivateKey())
	if err != nil {
		return err
	}
	if err := sendUDP(conn, encodePongMessage(pong), addr); err != nil {
		c.txErrors.Add(1)
		return err
	}
	c.recordTx()
	c.txPongMessages.Add(1)
	return nil
}

func (c *Client) recvLoop(ctx context.Context, conn *net.UDPConn) error {
	buf := make([]byte, packetDataSize)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		c.rxPackets.Add(1)
		c.lastRxUnix.Store(time.Now().Unix())
		if err := c.handlePacket(conn, buf[:n], addr); err != nil {
			c.rxDecodeErrors.Add(1)
			continue
		}
	}
}

func (c *Client) handlePacket(conn *net.UDPConn, packet []byte, from *net.UDPAddr) error {
	shredVersion := c.ShredVersion()
	decoded, err := decodePacketWithContactHandler(packet, func(record contactRecord) {
		c.handleContactRecord(record, shredVersion)
	})
	if err != nil {
		return err
	}
	switch decoded.Kind {
	case packetPing:
		c.rxPings.Add(1)
		return c.sendPong(conn, decoded.Ping, from)
	case packetPong:
		c.rxPongs.Add(1)
		if decoded.Pong.Verify() {
			c.recordPeer(from)
		}
	case packetContacts:
		c.rxContacts.Add(uint64(decoded.ContactCount))
	case packetPullRequest:
		c.rxPullRequests.Add(1)
		return c.sendPullResponse(conn, from)
	}
	return nil
}

func (c *Client) handleContactRecord(record contactRecord, shredVersion uint16) {
	if record.ShredVer != shredVersion || !record.GossipAddr.ok {
		return
	}
	addr := record.GossipAddr.UDPAddr()
	if addr == nil || addr.Port == 0 || addr.IP.IsUnspecified() || addr.IP.IsMulticast() {
		return
	}
	// CRDS is keyed by validator identity. If another process publishes that
	// same identity with different sockets, its newer ContactInfo replaces ours
	// cluster-wide: turbine and Votor traffic then move to the other process in
	// one instant while our gossip-derived tip keeps advancing. Treat echoed
	// copies of our own contact as harmless, but confirm two increasing foreign
	// wallclocks before stopping so a stale record from a previous run cannot
	// trip a fresh process.
	if record.Pubkey == c.Pubkey() {
		c.observeOwnContactRecord(record)
		return
	}
	// Route ordering outlives the bounded relay byte cache. Keep a nil TVU
	// entry for withdrawals until the existing peer lifetime expires.
	c.tvuPeerMu.Lock()
	defer c.tvuPeerMu.Unlock()
	version := record.version()
	if previous, ok := c.tvuPeers[record.Pubkey]; ok && !version.newerThan(previous.version) {
		return
	}
	// The decoder verified the original signed bytes. Retain those bytes for relay.
	if !c.relay.accept(record, wallclockMillis()) {
		return
	}
	// The entrypoint is commonly a validator itself. Do not add its gossip
	// socket to the rotating pull-peer table, but retain the service endpoints
	// from its signed ContactInfo so Turbine, repair, and Votor can route to it.
	if !sameEndpointUDPAddr(record.GossipAddr, c.entrypoint) {
		c.recordPeerEndpoint(record.GossipAddr)
	}
	c.recordRepairPeerRecord(record)
	c.recordAlpenglowPeerRecord(record)
	// Expire ordering no earlier than the service endpoints it protects.
	c.tvuPeers[record.Pubkey] = TVUPeer{
		Pubkey:   record.Pubkey,
		TVUAddr:  record.TVUAddr.UDPAddr(),
		LastSeen: time.Now(),
		version:  version,
	}
	c.acceptedContacts.Add(1)
}

func (c *Client) observeOwnContactRecord(record contactRecord) {
	c.contactMu.RLock()
	var local *ContactInfo
	if c.contact != nil {
		local = c.contact.CloneWithWallclock(c.contact.Wallclock)
	}
	c.contactMu.RUnlock()
	if local == nil {
		return
	}
	alpenglow := record.Sockets[socketTagAlpenglow]
	if sameEndpointUDPAddr(record.GossipAddr, local.GossipAddr) &&
		sameEndpointUDPAddr(record.TVUAddr, local.TVUAddr) &&
		sameEndpointUDPAddr(alpenglow, local.AlpenglowAddr) {
		return
	}
	if record.Wallclock <= local.Wallclock {
		return // stale ContactInfo from before this process initialized
	}

	c.identityConflictMu.Lock()
	sameInstance := record.GossipAddr == c.identityConflictGossip &&
		record.TVUAddr == c.identityConflictTVU &&
		alpenglow == c.identityConflictAlpenglow
	previousWallclock := c.identityConflictWallclock
	if !sameInstance || previousWallclock == 0 {
		c.identityConflictGossip = record.GossipAddr
		c.identityConflictTVU = record.TVUAddr
		c.identityConflictAlpenglow = alpenglow
		c.identityConflictWallclock = record.Wallclock
		c.identityConflictMu.Unlock()
		mlog.Log.Warnf("validator identity %s also appeared in gossip at gossip=%s tvu=%s alpenglow=%s; waiting for a newer record to confirm that the other instance is live",
			c.identity.PublicKey().String(), endpointString(record.GossipAddr), endpointString(record.TVUAddr), endpointString(alpenglow))
		return
	}
	if record.Wallclock <= previousWallclock {
		c.identityConflictMu.Unlock()
		return // the same stale CRDS value echoed by another peer
	}
	c.identityConflictWallclock = record.Wallclock
	c.identityConflictMu.Unlock()

	err := fmt.Errorf("%w: %s is publishing conflicting endpoints (ours gossip=%s tvu=%s alpenglow=%s; other gossip=%s tvu=%s alpenglow=%s); stop the other validator before starting Mithril",
		ErrIdentityInUse, c.identity.PublicKey().String(), addrString(local.GossipAddr), addrString(local.TVUAddr), addrString(local.AlpenglowAddr),
		endpointString(record.GossipAddr), endpointString(record.TVUAddr), endpointString(alpenglow))
	select {
	case c.identityConflictCh <- err:
	default:
	}
}

func endpointString(endpoint contactEndpoint) string {
	if addr := endpoint.UDPAddr(); addr != nil {
		return addr.String()
	}
	return "disabled"
}

func addrString(addr *net.UDPAddr) string {
	if addr == nil {
		return "disabled"
	}
	return addr.String()
}

func (c *Client) recordRepairPeer(contact *ContactInfo) {
	if contact == nil || contact.ServeRepairAddr == nil || contact.ServeRepairAddr.Port == 0 {
		return
	}
	now := time.Now()
	key := contact.Pubkey
	c.repairPeerMu.Lock()
	if existing, ok := c.repairPeers[key]; ok && sameAddr(existing.Addr, contact.ServeRepairAddr) {
		existing.LastSeen = now
		c.repairPeers[key] = existing
		c.repairPeerMu.Unlock()
		return
	}
	ip := normalizedIP(contact.ServeRepairAddr.IP)
	if ip == nil || ip.IsUnspecified() {
		c.repairPeerMu.Unlock()
		return
	}
	addr := &net.UDPAddr{IP: ip, Port: contact.ServeRepairAddr.Port}
	c.repairPeers[key] = RepairPeer{
		Pubkey:   contact.Pubkey,
		Addr:     addr,
		LastSeen: now,
	}
	c.repairPeerMu.Unlock()
}

func (c *Client) recordRepairPeerRecord(record contactRecord) {
	c.repairPeerMu.Lock()
	defer c.repairPeerMu.Unlock()
	if !record.ServeRepairAddr.ok || record.ServeRepairAddr.port == 0 {
		delete(c.repairPeers, record.Pubkey)
		return
	}
	now := time.Now()
	key := record.Pubkey
	if existing, ok := c.repairPeers[key]; ok && sameEndpointUDPAddr(record.ServeRepairAddr, existing.Addr) {
		existing.LastSeen = now
		c.repairPeers[key] = existing
		return
	}
	c.repairPeers[key] = RepairPeer{
		Pubkey:   record.Pubkey,
		Addr:     record.ServeRepairAddr.UDPAddr(),
		LastSeen: now,
	}
}

// TVUPeers returns non-expired TVU endpoints learned from gossip.
func (c *Client) TVUPeers() []TVUPeer {
	now := time.Now()
	c.tvuPeerMu.Lock()
	defer c.tvuPeerMu.Unlock()
	c.pruneTVUPeersLocked(now)
	out := make([]TVUPeer, 0, len(c.tvuPeers))
	for _, peer := range c.tvuPeers {
		if peer.TVUAddr == nil {
			continue
		}
		peer.TVUAddr = cloneUDPAddr(peer.TVUAddr)
		out = append(out, peer)
	}
	return out
}

func (c *Client) pruneTVUPeersLocked(now time.Time) {
	for key, peer := range c.tvuPeers {
		if now.Sub(peer.LastSeen) > peerExpirationWindow {
			delete(c.tvuPeers, key)
		}
	}
}

// SelfTVUAddr returns this node's configured TVU endpoint.
func (c *Client) SelfTVUAddr() *net.UDPAddr {
	if c == nil {
		return nil
	}
	return cloneUDPAddr(c.tvuAddr)
}

// LookupTVU resolves a validator identity to its gossip-advertised TVU socket.
func (c *Client) LookupTVU(pubkey solana.PublicKey) (*net.UDPAddr, bool) {
	if c == nil {
		return nil, false
	}
	c.tvuPeerMu.Lock()
	defer c.tvuPeerMu.Unlock()
	peer, ok := c.tvuPeers[Pubkey(pubkey)]
	if !ok || peer.TVUAddr == nil || time.Since(peer.LastSeen) > peerExpirationWindow {
		return nil, false
	}
	return cloneUDPAddr(peer.TVUAddr), true
}

// AlpenglowPeers returns non-expired Votor QUIC endpoints learned from signed
// ContactInfo records.
func (c *Client) AlpenglowPeers() []AlpenglowPeer {
	if c == nil {
		return nil
	}
	now := time.Now()
	c.alpenglowPeerMu.Lock()
	defer c.alpenglowPeerMu.Unlock()
	for key, peer := range c.alpenglowPeers {
		if now.Sub(peer.LastSeen) > peerExpirationWindow {
			delete(c.alpenglowPeers, key)
		}
	}
	out := make([]AlpenglowPeer, 0, len(c.alpenglowPeers))
	for _, peer := range c.alpenglowPeers {
		peer.AlpenglowAddr = cloneUDPAddr(peer.AlpenglowAddr)
		out = append(out, peer)
	}
	return out
}

// LookupAlpenglow resolves a validator identity to its gossip-advertised
// Votor QUIC socket.
func (c *Client) LookupAlpenglow(pubkey solana.PublicKey) (*net.UDPAddr, bool) {
	if c == nil {
		return nil, false
	}
	c.alpenglowPeerMu.Lock()
	defer c.alpenglowPeerMu.Unlock()
	peer, ok := c.alpenglowPeers[Pubkey(pubkey)]
	if !ok || peer.AlpenglowAddr == nil || time.Since(peer.LastSeen) > peerExpirationWindow {
		return nil, false
	}
	return cloneUDPAddr(peer.AlpenglowAddr), true
}

func (c *Client) recordAlpenglowPeerRecord(record contactRecord) {
	addr := record.Sockets[socketTagAlpenglow].UDPAddr()
	c.alpenglowPeerMu.Lock()
	defer c.alpenglowPeerMu.Unlock()
	if addr == nil {
		delete(c.alpenglowPeers, record.Pubkey)
		return
	}
	c.alpenglowPeers[record.Pubkey] = AlpenglowPeer{
		Pubkey:        record.Pubkey,
		AlpenglowAddr: addr,
		LastSeen:      time.Now(),
	}
}

func (c *Client) recordTx() {
	c.txPackets.Add(1)
	c.lastTxUnix.Store(time.Now().Unix())
}

func (c *Client) recordPeer(addr *net.UDPAddr) {
	if addr == nil || addr.Port == 0 {
		return
	}
	ip := normalizedIP(addr.IP)
	if ip == nil || ip.IsUnspecified() {
		return
	}
	key, ok := makeUDPAddrKey(addr)
	if !ok {
		return
	}
	now := time.Now()
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	if existing, exists := c.peers[key]; exists {
		existing.lastSeen = now
		c.peers[key] = existing
		return
	}
	if len(c.peers) >= maxKnownGossipPeers {
		c.prunePeersLocked(now)
	}
	if len(c.peers) >= maxKnownGossipPeers {
		return
	}
	c.peers[key] = knownPeer{addr: cloneUDPAddr(addr), lastSeen: now}
}

func (c *Client) recordPeerEndpoint(endpoint contactEndpoint) {
	if !endpoint.ok || endpoint.port == 0 {
		return
	}
	key, ok := makeUDPAddrKeyFromEndpoint(endpoint)
	if !ok {
		return
	}
	now := time.Now()
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	if existing, exists := c.peers[key]; exists {
		existing.lastSeen = now
		c.peers[key] = existing
		return
	}
	if len(c.peers) >= maxKnownGossipPeers {
		c.prunePeersLocked(now)
	}
	if len(c.peers) >= maxKnownGossipPeers {
		return
	}
	c.peers[key] = knownPeer{addr: endpoint.UDPAddr(), lastSeen: now}
}

func (c *Client) currentPeers() []*net.UDPAddr {
	now := time.Now()
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	c.prunePeersLocked(now)
	peers := make([]*net.UDPAddr, 0, len(c.peers))
	for _, peer := range c.peers {
		peers = append(peers, cloneUDPAddr(peer.addr))
	}
	return peers
}

func (c *Client) prunePeersLocked(now time.Time) {
	for key, peer := range c.peers {
		if now.Sub(peer.lastSeen) > peerExpirationWindow {
			delete(c.peers, key)
		}
	}
}

func formatAge(unix int64) string {
	if unix == 0 {
		return "never"
	}
	return time.Since(time.Unix(unix, 0)).Round(time.Second).String()
}

func makeUDPAddrKey(addr *net.UDPAddr) (udpAddrKey, bool) {
	var key udpAddrKey
	if addr == nil || addr.Port == 0 {
		return key, false
	}
	ip := addr.IP.To16()
	if ip == nil {
		return key, false
	}
	copy(key.ip[:], ip)
	key.port = addr.Port
	return key, true
}

func makeUDPAddrKeyFromEndpoint(endpoint contactEndpoint) (udpAddrKey, bool) {
	var key udpAddrKey
	if !endpoint.ok || endpoint.port == 0 {
		return key, false
	}
	copy(key.ip[:], endpoint.ip[:])
	key.port = endpoint.port
	return key, true
}

func sameEndpointUDPAddr(endpoint contactEndpoint, addr *net.UDPAddr) bool {
	key, ok := makeUDPAddrKeyFromEndpoint(endpoint)
	if !ok {
		return addr == nil
	}
	addrKey, ok := makeUDPAddrKey(addr)
	return ok && key == addrKey
}
