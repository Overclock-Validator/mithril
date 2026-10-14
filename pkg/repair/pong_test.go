package repair

import (
	"crypto/ed25519"
	"testing"
)

func TestRepairChallengePongRejectsMalformedAndForgedPackets(t *testing.T) {
	_, server, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pub, client, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	packet, expected, err := BuildPing(server)
	if err != nil {
		t.Fatal(err)
	}
	ping, ok := DecodePing(packet)
	if !ok {
		t.Fatal("generated ping did not verify")
	}
	encoded, err := BuildPong(client, ping)
	if err != nil {
		t.Fatal(err)
	}
	pong, ok := DecodePong(encoded)
	if !ok || pong.Hash != expected || string(pong.From[:]) != string(pub) {
		t.Fatal("pong did not match the challenge")
	}
	for _, offset := range []int{0, 4, 36, 68, 131} {
		forged := append([]byte(nil), encoded...)
		forged[offset] ^= 1
		if _, ok := DecodePong(forged); ok {
			t.Fatalf("mutation at byte %d accepted", offset)
		}
	}
	for _, malformed := range [][]byte{nil, encoded[:131], append(append([]byte(nil), encoded...), 0)} {
		if _, ok := DecodePong(malformed); ok {
			t.Fatal("malformed pong accepted")
		}
	}
	if _, _, err := BuildPing(nil); err == nil {
		t.Fatal("invalid identity accepted")
	}
}
