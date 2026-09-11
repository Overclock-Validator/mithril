package repair

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
)

const (
	repairProtocolPingResponse = uint32(0)
	repairProtocolPong         = uint32(7)

	repairProtocolWindowIndex        = uint32(8)
	repairProtocolHighestWindowIndex = uint32(9)

	repairSignatureOffset = 4
	repairSignatureSize   = 64
	repairPingSize        = 4 + 32 + 32 + 64
	repairRequestSize     = 4 + repairSignatureSize + 32 + 32 + 8 + 4 + 8 + 8
)

const (
	// RequestPacketSize is the exact bincode wire size of a modern signed
	// WindowIndex or HighestWindowIndex request.
	RequestPacketSize = repairRequestSize
	// RequestSignableSize excludes the signature field from RequestPacketSize.
	RequestSignableSize = repairRequestSize - repairSignatureSize
)

// RequestKind identifies the modern signed Solana repair requests Mithril
// can answer. Legacy, orphan, and ancestor-hash requests are deliberately not
// admitted by DecodeRequest.
type RequestKind uint8

const (
	RequestWindowIndex RequestKind = iota
	RequestHighestWindowIndex
)

// Request is the pointer-free portion of a modern signed repair request. The
// signature is verified against the original packet with VerifySignedRequest;
// keeping it out of this value avoids accidentally authenticating a
// re-encoded representation instead of the exact wire bytes.
type Request struct {
	Kind       RequestKind
	Sender     gossip.Pubkey
	Recipient  gossip.Pubkey
	Timestamp  uint64
	Nonce      uint32
	Slot       uint64
	ShredIndex uint64
}

type Ping struct {
	From      gossip.Pubkey
	Token     [32]byte
	Signature gossip.Signature
}

func NewWindowIndexRequest(identity ed25519.PrivateKey, recipient gossip.Pubkey, slot uint64, shredIndex uint64) ([]byte, uint32, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, 0, err
	}
	packet, err := BuildWindowIndexRequest(identity, recipient, slot, shredIndex, nonce)
	return packet, nonce, err
}

func BuildWindowIndexRequest(identity ed25519.PrivateKey, recipient gossip.Pubkey, slot uint64, shredIndex uint64, nonce uint32) ([]byte, error) {
	return buildRequest(identity, recipient, repairProtocolWindowIndex, slot, shredIndex, nonce, uint64(time.Now().UnixMilli()))
}

func NewHighestWindowIndexRequest(identity ed25519.PrivateKey, recipient gossip.Pubkey, slot uint64, shredIndex uint64) ([]byte, uint32, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, 0, err
	}
	packet, err := BuildHighestWindowIndexRequest(identity, recipient, slot, shredIndex, nonce)
	return packet, nonce, err
}

func BuildHighestWindowIndexRequest(identity ed25519.PrivateKey, recipient gossip.Pubkey, slot uint64, shredIndex uint64, nonce uint32) ([]byte, error) {
	return buildRequest(identity, recipient, repairProtocolHighestWindowIndex, slot, shredIndex, nonce, uint64(time.Now().UnixMilli()))
}

// DecodeRequest decodes exactly one modern signed WindowIndex or
// HighestWindowIndex request. It rejects trailing bytes just like Solana's
// bincode repair decoder. Call VerifySignedRequest before trusting the decoded
// sender or serving data.
func DecodeRequest(packet []byte) (Request, bool) {
	if len(packet) != repairRequestSize {
		return Request{}, false
	}

	var request Request
	switch binary.LittleEndian.Uint32(packet[:4]) {
	case repairProtocolWindowIndex:
		request.Kind = RequestWindowIndex
	case repairProtocolHighestWindowIndex:
		request.Kind = RequestHighestWindowIndex
	default:
		return Request{}, false
	}
	copy(request.Sender[:], packet[68:100])
	copy(request.Recipient[:], packet[100:132])
	request.Timestamp = binary.LittleEndian.Uint64(packet[132:140])
	request.Nonce = binary.LittleEndian.Uint32(packet[140:144])
	request.Slot = binary.LittleEndian.Uint64(packet[144:152])
	request.ShredIndex = binary.LittleEndian.Uint64(packet[152:160])
	return request, true
}

// CopyRequestSignable copies the exact bytes authenticated by a repair
// request into dst. Keeping this as a fixed-size copy lets packet consumers
// batch strict Ed25519 verification without allocating a re-encoded request.
func CopyRequestSignable(dst []byte, packet []byte) bool {
	if len(packet) != repairRequestSize || len(dst) < RequestSignableSize {
		return false
	}
	copy(dst[:repairSignatureOffset], packet[:repairSignatureOffset])
	copy(dst[repairSignatureOffset:RequestSignableSize], packet[repairSignatureOffset+repairSignatureSize:])
	return true
}

// RequestSignature returns the signature bytes in an exact-size request.
func RequestSignature(packet []byte) ([]byte, bool) {
	if len(packet) != repairRequestSize {
		return nil, false
	}
	return packet[repairSignatureOffset : repairSignatureOffset+repairSignatureSize], true
}

// IsPong reports whether packet has the canonical Solana repair Pong shape.
func IsPong(packet []byte) bool {
	return len(packet) == repairPingSize && binary.LittleEndian.Uint32(packet[:4]) == repairProtocolPong
}

func ResponseNonce(packet []byte) (uint32, bool) {
	if len(packet) < 4 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(packet[len(packet)-4:]), true
}

func DecodePing(packet []byte) (Ping, bool) {
	var ping Ping
	if len(packet) != repairPingSize {
		return ping, false
	}
	if binary.LittleEndian.Uint32(packet[0:4]) != repairProtocolPingResponse {
		return ping, false
	}
	copy(ping.From[:], packet[4:36])
	copy(ping.Token[:], packet[36:68])
	copy(ping.Signature[:], packet[68:132])
	if !narya.VerifyStrict(ping.From[:], ping.Token[:], ping.Signature[:]) {
		return Ping{}, false
	}
	return ping, true
}

func BuildPong(identity ed25519.PrivateKey, ping Ping) ([]byte, error) {
	sender, err := senderPubkey(identity)
	if err != nil {
		return nil, err
	}
	hash := hashPingToken(ping.Token)
	signature := ed25519.Sign(identity, hash[:])

	var packet []byte
	packet = binary.LittleEndian.AppendUint32(packet, repairProtocolPong)
	packet = append(packet, sender[:]...)
	packet = append(packet, hash[:]...)
	packet = append(packet, signature...)
	return packet, nil
}

func buildRequest(identity ed25519.PrivateKey, recipient gossip.Pubkey, variant uint32, slot uint64, shredIndex uint64, nonce uint32, timestamp uint64) ([]byte, error) {
	sender, err := senderPubkey(identity)
	if err != nil {
		return nil, err
	}

	var packet []byte
	packet = binary.LittleEndian.AppendUint32(packet, variant)
	packet = append(packet, make([]byte, repairSignatureSize)...)
	packet = append(packet, sender[:]...)
	packet = append(packet, recipient[:]...)
	packet = binary.LittleEndian.AppendUint64(packet, timestamp)
	packet = binary.LittleEndian.AppendUint32(packet, nonce)
	packet = binary.LittleEndian.AppendUint64(packet, slot)
	packet = binary.LittleEndian.AppendUint64(packet, shredIndex)

	signRepairPacket(identity, packet)
	return packet, nil
}

func senderPubkey(identity ed25519.PrivateKey) (gossip.Pubkey, error) {
	var out gossip.Pubkey
	if len(identity) != ed25519.PrivateKeySize {
		return out, fmt.Errorf("invalid repair identity size %d", len(identity))
	}
	pub, ok := identity.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return out, fmt.Errorf("invalid repair identity public key")
	}
	copy(out[:], pub)
	return out, nil
}

func signRepairPacket(identity ed25519.PrivateKey, packet []byte) {
	signable := make([]byte, 0, len(packet)-repairSignatureSize)
	signable = append(signable, packet[:repairSignatureOffset]...)
	signable = append(signable, packet[repairSignatureOffset+repairSignatureSize:]...)
	signature := ed25519.Sign(identity, signable)
	copy(packet[repairSignatureOffset:repairSignatureOffset+repairSignatureSize], signature)
}

// VerifySignedRequest authenticates an inbound repair request with Solana's
// strict Ed25519 predicate. The serve-repair packet loop uses the batched
// sigverify path; this single-request helper remains useful to callers and
// protocol tests.
func VerifySignedRequest(packet []byte, sender gossip.Pubkey) bool {
	var signable [RequestSignableSize]byte
	if !CopyRequestSignable(signable[:], packet) {
		return false
	}
	signature, _ := RequestSignature(packet)
	return narya.VerifyStrict(sender[:], signable[:], signature)
}

func hashPingToken(token [32]byte) gossip.Hash {
	h := sha256.New()
	_, _ = h.Write([]byte("SOLANA_PING_PONG"))
	_, _ = h.Write(token[:])
	var out gossip.Hash
	copy(out[:], h.Sum(nil))
	return out
}

func randomNonce() (uint32, error) {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(raw[:]), nil
}
