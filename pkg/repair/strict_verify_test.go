package repair

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// Real ed25519vectors (via Firedancer's CCTV corpus) that Go's
// crypto/ed25519.Verify ACCEPTS but mainnet's verify_strict REJECTS,
// because the public key A or the signature point R is small-order.
// A repair request carrying such a signature must be rejected — this
// is the exact adversarial class the strict fix closes.
var strictDivergentVectors = []struct {
	name, pub, msg, sig string
}{
	{
		name: "small-order A (all-zero pubkey)",
		pub:  "0000000000000000000000000000000000000000000000000000000000000000",
		msg:  "65643235353139766563746f72732033",
		sig:  "36684ea91032ba5b1dbab2d02f4debc74c3327f2b3802e2e4d371aa42b12b56b05ba9a796274d80437afa36f1236563f2f3b0aa84cecddc3d20914615ba4fe02",
	},
	{
		name: "small-order R (all-zero R point)",
		pub:  "10eb7c3acfb2bed3e0d6ab89bf5a3d6afddd1176ce4812e38d9fd485058fdb1f",
		msg:  "65643235353139766563746f72732033",
		sig:  "00000000000000000000000000000000000000000000000000000000000000009472a69cd9a701a50d130ed52189e2455b23767db52cacb8716fb896ffeeac09",
	},
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestVerifySignedRequestRejectsSmallOrder is the rigorous divergence
// proof: for each vector, the standard library accepts (precondition),
// and VerifySignedRequest — now routed through strict verification —
// rejects. A repair request's signable message is the packet with the
// signature field spliced out, so we build a packet whose reconstructed
// signable equals the vector's message.
func TestVerifySignedRequestRejectsSmallOrder(t *testing.T) {
	for _, v := range strictDivergentVectors {
		pub := mustHex(t, v.pub)
		msg := mustHex(t, v.msg)
		sig := mustHex(t, v.sig)
		if len(msg) < repairSignatureOffset {
			t.Fatalf("%s: message too short to embed", v.name)
		}

		// Precondition: the standard library accepts this signature, so
		// the test genuinely exercises the strict/stdlib divergence.
		if !ed25519.Verify(pub, msg, sig) {
			t.Fatalf("%s: precondition failed, stdlib should accept", v.name)
		}

		// packet = msg[:4] ‖ sig ‖ msg[4:], so
		// signable = packet[:4] ‖ packet[68:] = msg.
		packet := make([]byte, 0, len(msg)+repairSignatureSize)
		packet = append(packet, msg[:repairSignatureOffset]...)
		packet = append(packet, sig...)
		packet = append(packet, msg[repairSignatureOffset:]...)

		var sender gossip.Pubkey
		copy(sender[:], pub)

		if VerifySignedRequest(packet, sender) {
			t.Fatalf("%s: strict verification accepted a signature mainnet rejects", v.name)
		}
	}
}

// TestVerifySignedRequestHappyPath guards against the strict change
// breaking honest repair requests: a properly signed request verifies.
func TestVerifySignedRequestHappyPath(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var recipient gossip.Pubkey
	packet, err := BuildWindowIndexRequest(priv, recipient, 42, 7, 99)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := senderPubkey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifySignedRequest(packet, sender) {
		t.Fatal("honest repair request failed strict verification")
	}
}
