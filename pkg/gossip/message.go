package gossip

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
)

var errUnsupportedCRDSValue = errors.New("unsupported CRDS value")

type Ping struct {
	From      Pubkey
	Token     [32]byte
	Signature Signature
}

type Pong struct {
	From      Pubkey
	Hash      Hash
	Signature Signature
}

func newPing(identity ed25519.PrivateKey) (Ping, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return Ping{}, err
	}
	pubkey, err := pubkeyFromPrivateKey(identity)
	if err != nil {
		return Ping{}, err
	}
	sig := ed25519.Sign(identity, token[:])
	ping := Ping{From: pubkey, Token: token}
	copy(ping.Signature[:], sig)
	return ping, nil
}

func newPong(ping Ping, identity ed25519.PrivateKey) (Pong, error) {
	pubkey, err := pubkeyFromPrivateKey(identity)
	if err != nil {
		return Pong{}, err
	}
	hash := hashPingToken(ping.Token)
	sig := ed25519.Sign(identity, hash[:])
	pong := Pong{From: pubkey, Hash: hash}
	copy(pong.Signature[:], sig)
	return pong, nil
}

func (p Ping) Verify() bool {
	return narya.VerifyStrict(p.From[:], p.Token[:], p.Signature[:])
}

func (p Pong) Verify() bool {
	return narya.VerifyStrict(p.From[:], p.Hash[:], p.Signature[:])
}

func encodePingMessage(ping Ping) []byte {
	var e encoder
	e.variant(protocolPingMessage)
	e.fixed(ping.From[:])
	e.fixed(ping.Token[:])
	e.fixed(ping.Signature[:])
	return e.bytes()
}

func encodePongMessage(pong Pong) []byte {
	var e encoder
	e.variant(protocolPongMessage)
	e.fixed(pong.From[:])
	e.fixed(pong.Hash[:])
	e.fixed(pong.Signature[:])
	return e.bytes()
}

func encodePushMessage(from Pubkey, values []CrdsValue) ([]byte, error) {
	var e encoder
	e.variant(protocolPushMessage)
	e.fixed(from[:])
	e.u64(uint64(len(values)))
	for _, value := range values {
		value.encode(&e)
	}
	if len(e.bytes()) > packetDataSize {
		return nil, fmt.Errorf("push message size %d exceeds packet size %d", len(e.bytes()), packetDataSize)
	}
	return e.bytes(), nil
}

func encodePullResponse(from Pubkey, values []CrdsValue) ([]byte, error) {
	var e encoder
	e.variant(protocolPullResponse)
	e.fixed(from[:])
	e.u64(uint64(len(values)))
	for _, value := range values {
		value.encode(&e)
	}
	if len(e.bytes()) > packetDataSize {
		return nil, fmt.Errorf("pull response size %d exceeds packet size %d", len(e.bytes()), packetDataSize)
	}
	return e.bytes(), nil
}

func decodePing(d *decoder) (Ping, error) {
	var ping Ping
	from, err := d.read(32)
	if err != nil {
		return Ping{}, err
	}
	token, err := d.read(32)
	if err != nil {
		return Ping{}, err
	}
	sig, err := d.read(64)
	if err != nil {
		return Ping{}, err
	}
	copy(ping.From[:], from)
	copy(ping.Token[:], token)
	copy(ping.Signature[:], sig)
	return ping, nil
}

func decodePong(d *decoder) (Pong, error) {
	var pong Pong
	from, err := d.read(32)
	if err != nil {
		return Pong{}, err
	}
	hash, err := d.read(32)
	if err != nil {
		return Pong{}, err
	}
	sig, err := d.read(64)
	if err != nil {
		return Pong{}, err
	}
	copy(pong.From[:], from)
	copy(pong.Hash[:], hash)
	copy(pong.Signature[:], sig)
	return pong, nil
}

func decodeContactRecords(d *decoder, handleContact func(contactRecord)) (int, []*ContactInfo, error) {
	n, err := d.u64()
	if err != nil {
		return 0, nil, err
	}
	if n > 128 {
		return 0, nil, fmt.Errorf("too many CRDS values in packet: %d", n)
	}
	var contacts []*ContactInfo
	if handleContact == nil {
		contacts = make([]*ContactInfo, 0, n)
	}
	contactCount := 0
	for i := uint64(0); i < n; i++ {
		record, err := decodeCrdsContactRecord(d)
		if err != nil {
			if errors.Is(err, errUnsupportedCRDSValue) {
				return contactCount, contacts, errUnsupportedCRDSValue
			}
			return contactCount, contacts, err
		}
		if !record.Verify() {
			continue
		}
		contactCount++
		if handleContact != nil {
			handleContact(record)
		} else {
			contacts = append(contacts, record.ContactInfo())
		}
	}
	return contactCount, contacts, nil
}

type packetKind int

const (
	packetIgnored packetKind = iota
	packetPing
	packetPong
	packetContacts
	packetPullRequest
)

type decodedPacket struct {
	Kind         packetKind
	Ping         Ping
	Pong         Pong
	Contacts     []*ContactInfo
	ContactCount int
}

func decodePacket(packet []byte) (decodedPacket, error) {
	return decodePacketWithContactHandler(packet, nil)
}

func decodePacketWithContactHandler(packet []byte, handleContact func(contactRecord)) (decodedPacket, error) {
	d := newDecoder(packet)
	variant, err := d.variant()
	if err != nil {
		return decodedPacket{}, err
	}

	switch variant {
	case protocolPullRequest:
		return decodedPacket{Kind: packetPullRequest}, nil
	case protocolPullResponse, protocolPushMessage:
		if _, err := d.read(32); err != nil {
			return decodedPacket{}, err
		}
		contactCount, contacts, err := decodeContactRecords(d, handleContact)
		if err != nil {
			if errors.Is(err, errUnsupportedCRDSValue) {
				return decodedPacket{Kind: packetIgnored}, nil
			}
			return decodedPacket{}, err
		}
		return decodedPacket{Kind: packetContacts, Contacts: contacts, ContactCount: contactCount}, nil
	case protocolPingMessage:
		ping, err := decodePing(d)
		if err != nil {
			return decodedPacket{}, err
		}
		return decodedPacket{Kind: packetPing, Ping: ping}, nil
	case protocolPongMessage:
		pong, err := decodePong(d)
		if err != nil {
			return decodedPacket{}, err
		}
		return decodedPacket{Kind: packetPong, Pong: pong}, nil
	case protocolPruneMessage:
		return decodedPacket{Kind: packetIgnored}, nil
	default:
		return decodedPacket{Kind: packetIgnored}, nil
	}
}

func sendUDP(conn *net.UDPConn, packet []byte, addr *net.UDPAddr) error {
	if len(packet) > packetDataSize {
		return fmt.Errorf("gossip packet size %d exceeds packet size %d", len(packet), packetDataSize)
	}
	_, err := conn.WriteToUDP(packet, addr)
	return err
}
