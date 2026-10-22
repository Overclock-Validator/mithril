package gossip

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	localIP   = net.ParseIP("203.0.113.10")
	relayerIP = net.ParseIP("198.51.100.7")
)

func udp(ip net.IP, port int) *net.UDPAddr {
	return &net.UDPAddr{IP: ip, Port: port}
}

func newTestContact(t *testing.T) *ContactInfo {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	pubkey, err := pubkeyFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("pubkeyFromPrivateKey: %v", err)
	}
	contact, err := NewContactInfo(pubkey, 1234, udp(localIP, 8001), udp(localIP, 8002))
	if err != nil {
		t.Fatalf("NewContactInfo: %v", err)
	}
	if err := contact.SetTPUQUIC(udp(localIP, 8009)); err != nil {
		t.Fatalf("SetTPUQUIC: %v", err)
	}
	return contact
}

func socketAddr(contact *ContactInfo, key uint8) *net.UDPAddr {
	for _, entry := range contact.Sockets {
		if entry.Key == key {
			return udp(contact.Addrs[entry.Index], int(entry.Port))
		}
	}
	return nil
}

func encodeContact(t *testing.T, contact *ContactInfo) []byte {
	t.Helper()
	var e encoder
	if err := contact.encode(&e); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return e.bytes()
}

// requireAcceptedByPeers checks the contact survives a wire round trip and
// passes the entry checks peers apply before accepting it.
func requireAcceptedByPeers(t *testing.T, contact *ContactInfo) *ContactInfo {
	t.Helper()
	if err := contact.validateEntries(); err != nil {
		t.Fatalf("contact entries rejected: %v (addrs=%v sockets=%v)", err, contact.Addrs, contact.Sockets)
	}
	decoded, err := decodeContactInfo(newDecoder(encodeContact(t, contact)))
	if err != nil {
		t.Fatalf("decodeContactInfo: %v", err)
	}
	if err := decoded.validateEntries(); err != nil {
		t.Fatalf("decoded contact entries rejected: %v", err)
	}
	return decoded
}

func TestContactInfoPrunesAddressesNoSocketUses(t *testing.T) {
	contact := newTestContact(t)
	if err := contact.SetSocket(socketTagTPUQUIC, udp(relayerIP, 9000)); err != nil {
		t.Fatalf("SetSocket: %v", err)
	}
	if len(contact.Addrs) != 2 {
		t.Fatalf("addrs = %v, want local and relayer", contact.Addrs)
	}
	// Moving the only socket on the relayer address back drops that address.
	if err := contact.SetSocket(socketTagTPUQUIC, udp(localIP, 8009)); err != nil {
		t.Fatalf("SetSocket: %v", err)
	}
	if len(contact.Addrs) != 1 || !contact.Addrs[0].Equal(localIP) {
		t.Fatalf("addrs = %v, want only the local address", contact.Addrs)
	}
	requireAcceptedByPeers(t, contact)

	if err := contact.SetSocket(socketTagTPUQUIC, udp(relayerIP, 9000)); err != nil {
		t.Fatalf("SetSocket: %v", err)
	}
	contact.RemoveSocket(socketTagTPUQUIC)
	if len(contact.Addrs) != 1 || socketAddr(contact, socketTagTPUQUIC) != nil {
		t.Fatalf("addrs = %v sockets = %v, want the relayer address and socket removed", contact.Addrs, contact.Sockets)
	}
	requireAcceptedByPeers(t, contact)

	contact.RemoveSocket(socketTagTVU)
	if contact.TVUAddr != nil || socketAddr(contact, socketTagTVU) != nil {
		t.Fatalf("TVU socket still published")
	}
	requireAcceptedByPeers(t, contact)
}

func TestContactInfoTPUSocketsRelayerAndBack(t *testing.T) {
	contact := newTestContact(t)
	outset := contact.Outset
	local := TPUSockets{QUIC: udp(localIP, 8009), ForwardsQUIC: udp(localIP, 8009)}
	relayer := TPUSockets{
		UDP:          udp(relayerIP, 11222),
		QUIC:         udp(relayerIP, 11228),
		ForwardsUDP:  udp(relayerIP, 11223),
		ForwardsQUIC: udp(relayerIP, 11229),
	}
	keys := []struct {
		key  uint8
		want func(TPUSockets) *net.UDPAddr
	}{
		{socketTagTPU, func(s TPUSockets) *net.UDPAddr { return s.UDP }},
		{socketTagTPUQUIC, func(s TPUSockets) *net.UDPAddr { return s.QUIC }},
		{socketTagTPUForwards, func(s TPUSockets) *net.UDPAddr { return s.ForwardsUDP }},
		{socketTagTPUQUICForwards, func(s TPUSockets) *net.UDPAddr { return s.ForwardsQUIC }},
	}
	for step, tpu := range []TPUSockets{relayer, local, relayer, local} {
		if err := contact.SetTPUSockets(tpu); err != nil {
			t.Fatalf("step %d: SetTPUSockets: %v", step, err)
		}
		decoded := requireAcceptedByPeers(t, contact)
		for _, k := range keys {
			want := k.want(tpu)
			for _, c := range []*ContactInfo{contact, decoded} {
				got := socketAddr(c, k.key)
				if (want == nil) != (got == nil) || (want != nil && got.String() != want.String()) {
					t.Fatalf("step %d: socket %d = %v, want %v", step, k.key, got, want)
				}
			}
		}
		if got := socketAddr(contact, socketTagGossip); got.String() != "203.0.113.10:8001" {
			t.Fatalf("step %d: gossip socket = %v", step, got)
		}
	}
	if len(contact.Addrs) != 1 {
		t.Fatalf("addrs = %v, want only the local address after reverting", contact.Addrs)
	}
	if contact.Outset != outset {
		t.Fatalf("outset changed")
	}
}

func TestContactInfoSetTPUSocketsRejectsInvalidWithoutChange(t *testing.T) {
	contact := newTestContact(t)
	before := encodeContact(t, contact)
	err := contact.SetTPUSockets(TPUSockets{QUIC: udp(relayerIP, 9000), ForwardsQUIC: udp(relayerIP, 0)})
	if err == nil {
		t.Fatalf("expected an invalid port to be rejected")
	}
	if !bytes.Equal(before, encodeContact(t, contact)) {
		t.Fatalf("contact changed after a rejected update")
	}
}

func TestContactInfoValidateEntriesMatchesPeerChecks(t *testing.T) {
	valid := func() *ContactInfo {
		return &ContactInfo{
			Addrs:   []net.IP{localIP, relayerIP},
			Sockets: []SocketEntry{{Key: socketTagGossip, Index: 0, Port: 8001}, {Key: socketTagTPUQUIC, Index: 1, Port: 9000}},
		}
	}
	if err := valid().validateEntries(); err != nil {
		t.Fatalf("valid entries rejected: %v", err)
	}
	cases := map[string]func(*ContactInfo){
		"duplicate address": func(c *ContactInfo) { c.Addrs[1] = localIP },
		"IPv6 address":      func(c *ContactInfo) { c.Addrs[1] = net.ParseIP("2001:db8::1") },
		"duplicate key":     func(c *ContactInfo) { c.Sockets[1].Key = socketTagGossip },
		"index past end":    func(c *ContactInfo) { c.Sockets[1].Index = 2 },
		"unused address":    func(c *ContactInfo) { c.Sockets[1].Index = 0 },
	}
	for name, corrupt := range cases {
		c := valid()
		corrupt(c)
		if err := c.validateEntries(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func newUpdateTestClient(t *testing.T, entrypoint string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		Entrypoint:   entrypoint,
		BindAddr:     "127.0.0.1:0",
		TVUAddr:      "127.0.0.1:8002",
		AdvertisedIP: "127.0.0.1",
		ShredVersion: 4321,
		PushInterval: time.Hour,
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func publishedContact(c *Client) *ContactInfo {
	c.contactMu.RLock()
	defer c.contactMu.RUnlock()
	return c.contact
}

func TestClientUpdateContactPublishesACopy(t *testing.T) {
	client := newUpdateTestClient(t, "127.0.0.1:8000")
	if err := client.initializeContact(udp(net.ParseIP("127.0.0.1"), 65400)); err != nil {
		t.Fatalf("initializeContact: %v", err)
	}
	before := publishedContact(client)
	beforeWire := encodeContact(t, before)

	if err := client.SetClientID(ClientIDMithrilHarmonic); err != nil {
		t.Fatalf("SetClientID: %v", err)
	}
	after := publishedContact(client)
	if after == before {
		t.Fatalf("the published contact was modified in place")
	}
	if !bytes.Equal(beforeWire, encodeContact(t, before)) {
		t.Fatalf("the previously published contact changed")
	}
	if after.Version.Client != ClientIDMithrilHarmonic || before.Version.Client != versionClientMithril {
		t.Fatalf("client id after=%#x before=%#x", after.Version.Client, before.Version.Client)
	}
	if after.Outset != before.Outset {
		t.Fatalf("outset changed: %d -> %d", before.Outset, after.Outset)
	}
	select {
	case <-client.contactChanged:
	default:
		t.Fatalf("an update must request an immediate push")
	}

	// An update peers would reject publishes nothing.
	err := client.UpdateContact(func(contact *ContactInfo) error {
		return contact.SetSocket(socketTagTPUQUIC, udp(net.ParseIP("2001:db8::1"), 9000))
	})
	if err == nil {
		t.Fatalf("expected an IPv6 socket to be rejected")
	}
	if publishedContact(client) != after {
		t.Fatalf("a rejected update was published")
	}
	select {
	case <-client.contactChanged:
		t.Fatalf("a rejected update must not request a push")
	default:
	}
}

func TestClientAppliesContactUpdatesQueuedBeforeRun(t *testing.T) {
	client := newUpdateTestClient(t, "127.0.0.1:8000")
	if err := client.SetTPUQUIC(udp(net.ParseIP("127.0.0.1"), 8009)); err != nil {
		t.Fatalf("SetTPUQUIC: %v", err)
	}
	if err := client.SetTPUSockets(TPUSockets{QUIC: udp(relayerIP, 11228), ForwardsQUIC: udp(relayerIP, 11229)}); err != nil {
		t.Fatalf("SetTPUSockets: %v", err)
	}
	if err := client.SetClientID(ClientIDMithrilHarmonic); err != nil {
		t.Fatalf("SetClientID: %v", err)
	}
	if err := client.initializeContact(udp(net.ParseIP("127.0.0.1"), 65400)); err != nil {
		t.Fatalf("initializeContact: %v", err)
	}
	contact := publishedContact(client)
	if got := socketAddr(contact, socketTagTPUQUIC); got.String() != "198.51.100.7:11228" {
		t.Fatalf("TPU QUIC = %v; queued updates must apply in order", got)
	}
	if contact.Version.Client != ClientIDMithrilHarmonic {
		t.Fatalf("client id = %#x", contact.Version.Client)
	}
	requireAcceptedByPeers(t, contact)
}

func TestClientContactUpdatesDoNotRaceWithPushes(t *testing.T) {
	client := newUpdateTestClient(t, "127.0.0.1:8000")
	if err := client.initializeContact(udp(net.ParseIP("127.0.0.1"), 65400)); err != nil {
		t.Fatalf("initializeContact: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 500 {
			if err := client.SetTPUQUIC(udp(relayerIP, 9000+i%2)); err != nil {
				t.Errorf("SetTPUQUIC: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 500 {
			if _, err := client.currentContactValue(); err != nil {
				t.Errorf("currentContactValue: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

func TestClientPushesContactUpdatesImmediately(t *testing.T) {
	peer, err := net.ListenUDP("udp", udp(net.ParseIP("127.0.0.1"), 0))
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer peer.Close()
	client := newUpdateTestClient(t, peer.LocalAddr().String())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	// nextPushedClientID returns the client ID in the next contact pushed to
	// the peer, ignoring pings.
	nextPushedClientID := func() uint16 {
		t.Helper()
		buf := make([]byte, packetDataSize)
		if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		for {
			n, _, err := peer.ReadFromUDP(buf)
			if err != nil {
				t.Fatalf("no contact push arrived: %v", err)
			}
			d := newDecoder(buf[:n])
			if kind, err := d.variant(); err != nil || kind != protocolPushMessage {
				continue
			}
			if _, err := d.read(32); err != nil {
				t.Fatalf("push sender: %v", err)
			}
			if count, err := d.u64(); err != nil || count != 1 {
				t.Fatalf("push value count = %d: %v", count, err)
			}
			value, err := decodeCrdsValue(d)
			if err != nil || value.ContactInfo == nil {
				t.Fatalf("pushed value is not a contact: %v", err)
			}
			return value.ContactInfo.Version.Client
		}
	}
	if got := nextPushedClientID(); got != versionClientMithril {
		t.Fatalf("initial client id = %#x", got)
	}
	if err := client.SetClientID(ClientIDMithrilHarmonic); err != nil {
		t.Fatalf("SetClientID: %v", err)
	}
	// The push interval is an hour, so this push comes from the update.
	if got := nextPushedClientID(); got != ClientIDMithrilHarmonic {
		t.Fatalf("pushed client id = %#x, want 15", got)
	}
}
