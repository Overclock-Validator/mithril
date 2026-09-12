package gossip

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"net"
	"sort"
	"time"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
)

const (
	ClientName = "mithril"

	packetDataSize = 1232

	crdsDataLegacyContactInfo = 0
	crdsDataContactInfo       = 11

	protocolPullRequest  = 0
	protocolPullResponse = 1
	protocolPushMessage  = 2
	protocolPruneMessage = 3
	protocolPingMessage  = 4
	protocolPongMessage  = 5

	socketTagGossip          = 0
	socketTagServeRepairQuic = 1
	socketTagServeRepair     = 4
	socketTagTPUQUICForwards = 7
	socketTagTPUQUIC         = 8
	socketTagTPUVote         = 9
	socketTagTVU             = 10
	socketTagTPUVoteQuic     = 12
	socketTagAlpenglow       = 13

	// Agave's Version has a numeric ClientId, not a string. Unknown IDs are
	// accepted, so use a stable "MI" marker while keeping ClientName in logs.
	versionClientMithril = 0x4d49
)

type Pubkey [32]byte
type Signature [64]byte
type Hash [32]byte

type Version struct {
	Major      uint16
	Minor      uint16
	Patch      uint16
	Commit     uint32
	FeatureSet uint32
	Client     uint16
}

type SocketEntry struct {
	Key   uint8
	Index uint8
	Port  uint16
}

type ContactInfo struct {
	Pubkey          Pubkey
	Wallclock       uint64
	Outset          uint64
	ShredVer        uint16
	Version         Version
	Addrs           []net.IP
	Sockets         []SocketEntry
	GossipAddr      *net.UDPAddr
	ServeRepairAddr *net.UDPAddr
	TVUAddr         *net.UDPAddr
	TPUVoteAddr     *net.UDPAddr
	TPUVoteQuicAddr *net.UDPAddr
	AlpenglowAddr   *net.UDPAddr
	Extensions      [][]byte
}

type contactEndpoint struct {
	ip   [16]byte
	port int
	ok   bool
}

type contactRecord struct {
	Pubkey          Pubkey
	Wallclock       uint64
	ShredVer        uint16
	GossipAddr      contactEndpoint
	ServeRepairAddr contactEndpoint
	TVUAddr         contactEndpoint
	Sockets         map[uint8]contactEndpoint
	signature       Signature
	data            []byte
}

func NewContactInfo(pubkey Pubkey, shredVersion uint16, gossipAddr, tvuAddr *net.UDPAddr) (*ContactInfo, error) {
	info := &ContactInfo{
		Pubkey:    pubkey,
		Wallclock: wallclockMillis(),
		Outset:    uint64(time.Now().UnixMicro()),
		ShredVer:  shredVersion,
		Version: Version{
			Client: versionClientMithril,
		},
	}
	if gossipAddr != nil {
		if err := info.SetSocket(socketTagGossip, gossipAddr); err != nil {
			return nil, err
		}
		info.GossipAddr = cloneUDPAddr(gossipAddr)
	}
	if tvuAddr != nil {
		if err := info.SetSocket(socketTagTVU, tvuAddr); err != nil {
			return nil, err
		}
		info.TVUAddr = cloneUDPAddr(tvuAddr)
	}
	return info, nil
}

func (r contactRecord) Verify() bool {
	return narya.VerifyStrict(r.Pubkey[:], r.data, r.signature[:])
}

func (r contactRecord) ContactInfo() *ContactInfo {
	return &ContactInfo{
		Pubkey:          r.Pubkey,
		ShredVer:        r.ShredVer,
		GossipAddr:      r.GossipAddr.UDPAddr(),
		ServeRepairAddr: r.ServeRepairAddr.UDPAddr(),
		TVUAddr:         r.TVUAddr.UDPAddr(),
	}
}

func (e contactEndpoint) UDPAddr() *net.UDPAddr {
	if !e.ok || e.port == 0 {
		return nil
	}
	ip := make(net.IP, 16)
	copy(ip, e.ip[:])
	return &net.UDPAddr{IP: ip, Port: e.port}
}

func (c *ContactInfo) CloneWithWallclock(wallclock uint64) *ContactInfo {
	cp := *c
	cp.Wallclock = wallclock
	cp.Addrs = append([]net.IP(nil), c.Addrs...)
	cp.Sockets = append([]SocketEntry(nil), c.Sockets...)
	cp.Extensions = append([][]byte(nil), c.Extensions...)
	cp.GossipAddr = cloneUDPAddr(c.GossipAddr)
	cp.ServeRepairAddr = cloneUDPAddr(c.ServeRepairAddr)
	cp.TVUAddr = cloneUDPAddr(c.TVUAddr)
	cp.TPUVoteAddr = cloneUDPAddr(c.TPUVoteAddr)
	cp.TPUVoteQuicAddr = cloneUDPAddr(c.TPUVoteQuicAddr)
	cp.AlpenglowAddr = cloneUDPAddr(c.AlpenglowAddr)
	return &cp
}

func (c *ContactInfo) SetAlpenglowAddr(addr *net.UDPAddr) error {
	if err := c.SetSocket(socketTagAlpenglow, addr); err != nil {
		return err
	}
	c.AlpenglowAddr = cloneUDPAddr(addr)
	return nil
}

func (c *ContactInfo) SetTPUVoteAddr(addr *net.UDPAddr) error {
	if err := c.SetSocket(socketTagTPUVote, addr); err != nil {
		return err
	}
	c.TPUVoteAddr = cloneUDPAddr(addr)
	return nil
}

func (c *ContactInfo) SetTPUVoteQuicAddr(addr *net.UDPAddr) error {
	if err := c.SetSocket(socketTagTPUVoteQuic, addr); err != nil {
		return err
	}
	c.TPUVoteQuicAddr = cloneUDPAddr(addr)
	return nil
}

// SetTPUQUIC publishes the same endpoint for normal and forwarding TPU QUIC.
func (c *ContactInfo) SetTPUQUIC(addr *net.UDPAddr) error {
	if err := c.SetSocket(socketTagTPUQUICForwards, addr); err != nil {
		return err
	}
	return c.SetSocket(socketTagTPUQUIC, addr)
}

func (c *ContactInfo) SetSocket(key uint8, addr *net.UDPAddr) error {
	if addr == nil {
		return fmt.Errorf("nil socket for key %d", key)
	}
	if addr.Port <= 0 || addr.Port > 0xffff {
		return fmt.Errorf("invalid port %d for key %d", addr.Port, key)
	}
	ip := normalizedIP(addr.IP)
	if ip == nil || ip.IsUnspecified() {
		return fmt.Errorf("invalid IP %q for key %d", addr.IP.String(), key)
	}
	addrIndex := -1
	for idx, existing := range c.Addrs {
		if existing.Equal(ip) {
			addrIndex = idx
			break
		}
	}
	if addrIndex < 0 {
		if len(c.Addrs) >= 256 {
			return fmt.Errorf("too many contact IP addresses")
		}
		addrIndex = len(c.Addrs)
		c.Addrs = append(c.Addrs, ip)
	}

	filtered := c.Sockets[:0]
	for _, entry := range c.Sockets {
		if entry.Key != key {
			filtered = append(filtered, entry)
		}
	}
	c.Sockets = append(filtered, SocketEntry{Key: key, Index: uint8(addrIndex), Port: uint16(addr.Port)})
	sort.Slice(c.Sockets, func(i, j int) bool {
		if c.Sockets[i].Port == c.Sockets[j].Port {
			return c.Sockets[i].Key < c.Sockets[j].Key
		}
		return c.Sockets[i].Port < c.Sockets[j].Port
	})
	return nil
}

func (c *ContactInfo) encode(e *encoder) error {
	e.fixed(c.Pubkey[:])
	e.varint(c.Wallclock)
	e.u64(c.Outset)
	e.u16(c.ShredVer)
	encodeVersion(e, c.Version)

	if err := e.shortLen(len(c.Addrs)); err != nil {
		return err
	}
	for _, ip := range c.Addrs {
		if err := encodeIP(e, ip); err != nil {
			return err
		}
	}

	if err := e.shortLen(len(c.Sockets)); err != nil {
		return err
	}
	var previousPort uint16
	for _, socket := range c.Sockets {
		if socket.Port < previousPort {
			return fmt.Errorf("socket ports are not sorted")
		}
		e.u8(socket.Key)
		e.u8(socket.Index)
		e.varint(uint64(socket.Port - previousPort))
		previousPort = socket.Port
	}

	if err := e.shortLen(len(c.Extensions)); err != nil {
		return err
	}
	for _, ext := range c.Extensions {
		e.fixed(ext)
	}
	return nil
}

func decodeContactInfo(d *decoder) (*ContactInfo, error) {
	info := &ContactInfo{}
	pubkey, err := d.read(32)
	if err != nil {
		return nil, err
	}
	copy(info.Pubkey[:], pubkey)
	if info.Wallclock, err = d.varint(10); err != nil {
		return nil, err
	}
	if info.Outset, err = d.u64(); err != nil {
		return nil, err
	}
	if info.ShredVer, err = d.u16(); err != nil {
		return nil, err
	}
	if info.Version, err = decodeVersion(d); err != nil {
		return nil, err
	}

	numAddrs, err := d.shortLen()
	if err != nil {
		return nil, err
	}
	info.Addrs = make([]net.IP, numAddrs)
	for i := range info.Addrs {
		ip, err := decodeIP(d)
		if err != nil {
			return nil, err
		}
		info.Addrs[i] = normalizedIP(ip)
	}

	numSockets, err := d.shortLen()
	if err != nil {
		return nil, err
	}
	info.Sockets = make([]SocketEntry, 0, numSockets)
	var port uint16
	for i := 0; i < numSockets; i++ {
		key, err := d.u8()
		if err != nil {
			return nil, err
		}
		index, err := d.u8()
		if err != nil {
			return nil, err
		}
		offset, err := d.varint(3)
		if err != nil {
			return nil, err
		}
		if offset > 0xffff || uint64(port)+offset > 0xffff {
			return nil, fmt.Errorf("socket port offset overflow")
		}
		port += uint16(offset)
		if int(index) >= len(info.Addrs) {
			return nil, fmt.Errorf("socket IP index %d out of range", index)
		}
		entry := SocketEntry{Key: key, Index: index, Port: port}
		info.Sockets = append(info.Sockets, entry)
		addr := &net.UDPAddr{IP: info.Addrs[index], Port: int(port)}
		switch key {
		case socketTagGossip:
			info.GossipAddr = cloneUDPAddr(addr)
		case socketTagServeRepair:
			info.ServeRepairAddr = cloneUDPAddr(addr)
		case socketTagTVU:
			info.TVUAddr = cloneUDPAddr(addr)
		case socketTagTPUVote:
			info.TPUVoteAddr = cloneUDPAddr(addr)
		case socketTagTPUVoteQuic:
			info.TPUVoteQuicAddr = cloneUDPAddr(addr)
		case socketTagAlpenglow:
			info.AlpenglowAddr = cloneUDPAddr(addr)
		}
	}

	numExt, err := d.shortLen()
	if err != nil {
		return nil, err
	}
	if err := skipContactExtensions(d, numExt); err != nil {
		return nil, err
	}
	return info, nil
}

func decodeContactRecord(d *decoder) (contactRecord, error) {
	var record contactRecord
	pubkey, err := d.read(32)
	if err != nil {
		return contactRecord{}, err
	}
	copy(record.Pubkey[:], pubkey)
	if record.Wallclock, err = d.varint(10); err != nil {
		return contactRecord{}, err
	}
	if _, err := d.u64(); err != nil {
		return contactRecord{}, err
	}
	if record.ShredVer, err = d.u16(); err != nil {
		return contactRecord{}, err
	}
	if _, err = decodeVersion(d); err != nil {
		return contactRecord{}, err
	}

	numAddrs, err := d.shortLen()
	if err != nil {
		return contactRecord{}, err
	}
	const inlineAddrCount = 8
	var inlineAddrs [inlineAddrCount]contactEndpoint
	var addrs []contactEndpoint
	if numAddrs > inlineAddrCount {
		addrs = make([]contactEndpoint, numAddrs)
	}
	for i := 0; i < numAddrs; i++ {
		addr, err := decodeContactIP(d)
		if err != nil {
			return contactRecord{}, err
		}
		if addrs != nil {
			addrs[i] = addr
		} else {
			inlineAddrs[i] = addr
		}
	}
	addrAt := func(index uint8) (contactEndpoint, bool) {
		if int(index) >= numAddrs {
			return contactEndpoint{}, false
		}
		if addrs != nil {
			return addrs[index], true
		}
		return inlineAddrs[index], true
	}

	numSockets, err := d.shortLen()
	if err != nil {
		return contactRecord{}, err
	}
	var port uint16
	for i := 0; i < numSockets; i++ {
		key, err := d.u8()
		if err != nil {
			return contactRecord{}, err
		}
		index, err := d.u8()
		if err != nil {
			return contactRecord{}, err
		}
		offset, err := d.varint(3)
		if err != nil {
			return contactRecord{}, err
		}
		if offset > 0xffff || uint64(port)+offset > 0xffff {
			return contactRecord{}, fmt.Errorf("socket port offset overflow")
		}
		port += uint16(offset)
		addr, ok := addrAt(index)
		if !ok {
			return contactRecord{}, fmt.Errorf("socket IP index %d out of range", index)
		}
		addr.port = int(port)
		addr.ok = true
		if record.Sockets == nil {
			record.Sockets = make(map[uint8]contactEndpoint)
		}
		record.Sockets[key] = addr
		switch key {
		case socketTagGossip:
			record.GossipAddr = addr
		case socketTagServeRepair:
			record.ServeRepairAddr = addr
		case socketTagTVU:
			record.TVUAddr = addr
		}
	}

	numExt, err := d.shortLen()
	if err != nil {
		return contactRecord{}, err
	}
	if err := skipContactExtensions(d, numExt); err != nil {
		return contactRecord{}, err
	}
	return record, nil
}

func skipContactExtensions(d *decoder, count int) error {
	for i := 0; i < count; i++ {
		if _, err := d.u8(); err != nil {
			return err
		}
		length, err := d.shortLen()
		if err != nil {
			return err
		}
		if _, err := d.read(length); err != nil {
			return err
		}
	}
	return nil
}

func decodeContactIP(d *decoder) (contactEndpoint, error) {
	var out contactEndpoint
	variant, err := d.variant()
	if err != nil {
		return contactEndpoint{}, err
	}
	switch variant {
	case 0:
		b, err := d.read(4)
		if err != nil {
			return contactEndpoint{}, err
		}
		out.ip[10] = 0xff
		out.ip[11] = 0xff
		copy(out.ip[12:], b)
	case 1:
		b, err := d.read(16)
		if err != nil {
			return contactEndpoint{}, err
		}
		copy(out.ip[:], b)
	default:
		return contactEndpoint{}, fmt.Errorf("unknown IP variant %d", variant)
	}
	out.ok = true
	return out, nil
}

func encodeVersion(e *encoder, version Version) {
	e.varint(uint64(version.Major))
	e.varint(uint64(version.Minor))
	e.varint(uint64(version.Patch))
	e.u32(version.Commit)
	e.u32(version.FeatureSet)
	e.varint(uint64(version.Client))
}

func decodeVersion(d *decoder) (Version, error) {
	major, err := d.varint(3)
	if err != nil {
		return Version{}, err
	}
	minor, err := d.varint(3)
	if err != nil {
		return Version{}, err
	}
	patch, err := d.varint(3)
	if err != nil {
		return Version{}, err
	}
	commit, err := d.u32()
	if err != nil {
		return Version{}, err
	}
	featureSet, err := d.u32()
	if err != nil {
		return Version{}, err
	}
	client, err := d.varint(3)
	if err != nil {
		return Version{}, err
	}
	return Version{
		Major:      uint16(major),
		Minor:      uint16(minor),
		Patch:      uint16(patch),
		Commit:     commit,
		FeatureSet: featureSet,
		Client:     uint16(client),
	}, nil
}

func encodeCrdsDataContactInfo(info *ContactInfo) ([]byte, error) {
	var e encoder
	e.variant(crdsDataContactInfo)
	if err := info.encode(&e); err != nil {
		return nil, err
	}
	return e.bytes(), nil
}

func decodeCrdsDataContactInfo(d *decoder) (*ContactInfo, error) {
	variant, err := d.variant()
	if err != nil {
		return nil, err
	}
	switch variant {
	case crdsDataContactInfo:
		return decodeContactInfo(d)
	default:
		return nil, errUnsupportedCRDSValue
	}
}

func decodeCrdsDataContactRecord(d *decoder) (contactRecord, error) {
	variant, err := d.variant()
	if err != nil {
		return contactRecord{}, err
	}
	switch variant {
	case crdsDataContactInfo:
		return decodeContactRecord(d)
	default:
		return contactRecord{}, errUnsupportedCRDSValue
	}
}

func signCrdsContactInfo(info *ContactInfo, identity ed25519.PrivateKey) (CrdsValue, error) {
	data, err := encodeCrdsDataContactInfo(info)
	if err != nil {
		return CrdsValue{}, err
	}
	sig := ed25519.Sign(identity, data)
	value := CrdsValue{ContactInfo: info, Data: data}
	copy(value.Signature[:], sig)
	return value, nil
}

func decodeCrdsContactRecord(d *decoder) (contactRecord, error) {
	sig, err := d.read(64)
	if err != nil {
		return contactRecord{}, err
	}
	dataStart := d.off
	record, err := decodeCrdsDataContactRecord(d)
	if err != nil {
		return contactRecord{}, err
	}
	record.data = d.data[dataStart:d.off]
	copy(record.signature[:], sig)
	return record, nil
}

type CrdsValue struct {
	Signature   Signature
	ContactInfo *ContactInfo
	Data        []byte
}

func (v CrdsValue) encode(e *encoder) {
	e.fixed(v.Signature[:])
	e.fixed(v.Data)
}

func decodeCrdsValue(d *decoder) (CrdsValue, error) {
	sig, err := d.read(64)
	if err != nil {
		return CrdsValue{}, err
	}
	dataStart := d.off
	info, err := decodeCrdsDataContactInfo(d)
	if err != nil {
		return CrdsValue{}, err
	}
	data := d.data[dataStart:d.off]
	value := CrdsValue{ContactInfo: info, Data: data}
	copy(value.Signature[:], sig)
	return value, nil
}

func (v CrdsValue) Verify() bool {
	if v.ContactInfo == nil {
		return false
	}
	return narya.VerifyStrict(v.ContactInfo.Pubkey[:], v.Data, v.Signature[:])
}

func hashPingToken(token [32]byte) Hash {
	h := sha256.New()
	h.Write([]byte("SOLANA_PING_PONG"))
	h.Write(token[:])
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

func normalizedIP(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return net.IPv4(v4[0], v4[1], v4[2], v4[3])
	}
	if v6 := ip.To16(); v6 != nil {
		return append(net.IP(nil), v6...)
	}
	return nil
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port, Zone: addr.Zone}
}

func wallclockMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func pubkeyFromPrivateKey(key ed25519.PrivateKey) (Pubkey, error) {
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return Pubkey{}, fmt.Errorf("invalid ed25519 private key")
	}
	var out Pubkey
	copy(out[:], pub)
	return out, nil
}

func sameAddr(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Port == b.Port && bytes.Equal(normalizedIP(a.IP), normalizedIP(b.IP))
}
