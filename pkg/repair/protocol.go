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
)

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

// VerifySignedRequest authenticates an INBOUND repair request — a peer asking
// us to serve it a shred. Mithril is requester-side only today, so nothing
// outside tests calls this yet.
//
// When repair serving is wired up, this becomes a packet-rate consumer fed by
// a UDP socket loop, which is the shape that wants batching: signatures are
// verified eight per AVX-512 group, so verifying one packet at a time pays for
// a whole group and uses one lane of it. Drain the socket's work queue and
// verify a group per pass, exactly as pkg/tpu/pipeline does — sigverify.Drain
// and sigverify.FairShare exist for this, and take a share rather than the
// whole queue so batching does not eat the parallelism across workers.
//
// The predicate here is already the strict one, which matters more than the
// throughput: a repair request authenticated under plain stdlib rules would
// accept small-order sender keys that Agave rejects.
func VerifySignedRequest(packet []byte, sender gossip.Pubkey) bool {
	if len(packet) < repairSignatureOffset+repairSignatureSize {
		return false
	}
	signable := make([]byte, 0, len(packet)-repairSignatureSize)
	signable = append(signable, packet[:repairSignatureOffset]...)
	signable = append(signable, packet[repairSignatureOffset+repairSignatureSize:]...)
	return narya.VerifyStrict(sender[:], signable, packet[repairSignatureOffset:repairSignatureOffset+repairSignatureSize])
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
