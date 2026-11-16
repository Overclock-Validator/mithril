package gossip

import (
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
)

type countingVerifyCache struct {
	*contactVerifyCache
	calls int
}

func newCountingVerifyCache() *countingVerifyCache {
	c := &countingVerifyCache{contactVerifyCache: newContactVerifyCache()}
	c.verifySignature = func(r contactRecord) bool {
		c.calls++
		return r.Verify()
	}
	return c
}

func signedForeignContact(t testing.TB, identity ed25519.PrivateKey, wallclock uint64, tvuPort int) []byte {
	t.Helper()
	pubkey := Pubkey(identity.Public().(ed25519.PublicKey))
	info, err := NewContactInfo(pubkey, 4321,
		&net.UDPAddr{IP: net.ParseIP("203.0.113.30"), Port: 9000},
		&net.UDPAddr{IP: net.ParseIP("203.0.113.30"), Port: tvuPort})
	if err != nil {
		t.Fatalf("NewContactInfo: %v", err)
	}
	info.Wallclock = wallclock
	value, err := signCrdsContactInfo(info, identity)
	if err != nil {
		t.Fatalf("signCrdsContactInfo: %v", err)
	}
	var e encoder
	value.encode(&e)
	return e.bytes()
}

func decodeRecord(t testing.TB, encoded []byte) contactRecord {
	t.Helper()
	// Decode from a copy so cached data can never alias the caller's slice.
	record, err := decodeCrdsContactRecord(newDecoder(append([]byte(nil), encoded...)))
	if err != nil {
		t.Fatalf("decodeCrdsContactRecord: %v", err)
	}
	return record
}

// forge flips one byte of the signature, leaving the payload intact.
func forge(encoded []byte) []byte {
	out := append([]byte(nil), encoded...)
	out[10] ^= 0x01
	return out
}

func newIdentity(t testing.TB) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return priv
}

func TestContactVerifyCacheSkipsExactRepeat(t *testing.T) {
	cache := newCountingVerifyCache()
	encoded := signedForeignContact(t, newIdentity(t), 1000, 9001)
	for i := 0; i < 5; i++ {
		if !cache.verify(decodeRecord(t, encoded)) {
			t.Fatalf("repeat %d rejected", i)
		}
	}
	if cache.calls != 1 {
		t.Fatalf("signature checks = %d, want 1", cache.calls)
	}
}

func TestContactVerifyCacheVerifiesChangedRecords(t *testing.T) {
	cache := newCountingVerifyCache()
	id := newIdentity(t)
	older := signedForeignContact(t, id, 1000, 9001)
	newer := signedForeignContact(t, id, 2000, 9005)
	if !cache.verify(decodeRecord(t, older)) || !cache.verify(decodeRecord(t, newer)) {
		t.Fatal("genuine records rejected")
	}
	// The newer record is a different message and was verified; the older
	// one, repeated, is still an exact match for its own verified entry.
	if !cache.verify(decodeRecord(t, older)) {
		t.Fatal("genuine older record rejected")
	}
	if cache.calls != 2 {
		t.Fatalf("signature checks = %d, want 2", cache.calls)
	}
}

func TestContactVerifyCacheRejectsForgeries(t *testing.T) {
	cache := newCountingVerifyCache()
	id := newIdentity(t)
	genuine := signedForeignContact(t, id, 1000, 9001)
	if !cache.verify(decodeRecord(t, genuine)) {
		t.Fatal("genuine record rejected")
	}
	// Same payload (equal wallclock) with a broken signature.
	if cache.verify(decodeRecord(t, forge(genuine))) {
		t.Fatal("forged signature accepted")
	}
	if cache.verify(decodeRecord(t, forge(signedForeignContact(t, id, 500, 9009)))) {
		t.Fatal("forged stale record accepted")
	}
	// A valid signature from a different key, presented under id's pubkey.
	staleRecord := decodeRecord(t, signedForeignContact(t, newIdentity(t), 500, 9009))
	staleRecord.Pubkey = Pubkey(id.Public().(ed25519.PublicKey))
	if cache.verify(staleRecord) {
		t.Fatal("record signed by another key accepted")
	}
	// The cache still holds the genuine record and skips it without a check.
	before := cache.calls
	if !cache.verify(decodeRecord(t, genuine)) || cache.calls != before {
		t.Fatalf("genuine repeat after forgery: calls %d -> %d", before, cache.calls)
	}
}

func TestContactVerifyCacheForgeryDoesNotSuppressGenuine(t *testing.T) {
	cache := newCountingVerifyCache()
	id := newIdentity(t)
	newer := signedForeignContact(t, id, 2000, 9005)
	// A forgery of the genuine newer bytes arrives first.
	if cache.verify(decodeRecord(t, forge(newer))) {
		t.Fatal("forgery accepted")
	}
	if !cache.verify(decodeRecord(t, newer)) {
		t.Fatal("genuine record suppressed by an earlier forgery")
	}
}

func TestClientForgedContactChangesNothing(t *testing.T) {
	client := newUpdateTestClient(t, "127.0.0.1:8000")
	id := newIdentity(t)
	genuine := signedForeignContact(t, id, 1000, 9001)
	from := Pubkey(id.Public().(ed25519.PublicKey))
	push := func(encoded []byte) {
		t.Helper()
		var e encoder
		e.variant(protocolPushMessage)
		e.fixed(from[:])
		e.u64(1)
		e.fixed(encoded)
		if err := client.handlePacket(nil, e.bytes(), nil); err != nil {
			t.Fatalf("handlePacket: %v", err)
		}
	}
	push(forge(genuine))
	if peers := client.TVUPeers(); len(peers) != 0 {
		t.Fatalf("forged record added TVU peers: %v", peers)
	}
	push(genuine)
	push(genuine)
	push(forge(signedForeignContact(t, id, 3000, 9007)))
	found := false
	for _, peer := range client.TVUPeers() {
		if peer.Pubkey == from {
			found = true
			if peer.TVUAddr.Port != 9001 {
				t.Fatalf("TVU port = %d, want 9001", peer.TVUAddr.Port)
			}
		}
	}
	if !found {
		t.Fatal("genuine record not learned")
	}
}

func BenchmarkHandleRepeatedContacts(b *testing.B) {
	for _, cached := range []bool{false, true} {
		name := "verify-every"
		if cached {
			name = "verify-cache"
		}
		b.Run(name, func(b *testing.B) {
			client, err := NewClient(Config{Entrypoint: "127.0.0.1:8000", BindAddr: "127.0.0.1:0", ShredVersion: 4321})
			if err != nil {
				b.Fatal(err)
			}
			if !cached {
				client.contactVerify = nil
			}
			// A push of 8 records from 8 identities, delivered repeatedly as
			// relays do.
			var e encoder
			e.variant(protocolPushMessage)
			var from Pubkey
			e.fixed(from[:])
			e.u64(8)
			for i := 0; i < 8; i++ {
				e.fixed(signedForeignContact(b, newIdentity(b), 1000, 9001+i))
			}
			packet := e.bytes()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := client.handlePacket(nil, packet, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Relays hold different versions of one origin's record, so old and new
// copies arrive interleaved. Both must stay cached; a single slot per pubkey
// made them evict each other and nothing hit.
func TestContactVerifyCacheInterleavedVersionsHit(t *testing.T) {
	cache := newCountingVerifyCache()
	id := newIdentity(t)
	older := signedForeignContact(t, id, 1000, 9001)
	newer := signedForeignContact(t, id, 2000, 9005)
	for i := 0; i < 6; i++ {
		encoded := older
		if i%2 == 1 {
			encoded = newer
		}
		if !cache.verify(decodeRecord(t, encoded)) {
			t.Fatalf("delivery %d rejected", i)
		}
	}
	if cache.calls != 2 {
		t.Fatalf("signature checks = %d, want 2", cache.calls)
	}
	if s := cache.stats(); s.Hits != 4 || s.Verified != 2 || s.Size != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

// A forgery reusing a cached signature over different bytes, or under another
// pubkey, must be verified and rejected, and must not evict the genuine entry.
func TestContactVerifyCacheSignatureCollisionForgery(t *testing.T) {
	cache := newCountingVerifyCache()
	id := newIdentity(t)
	genuine := signedForeignContact(t, id, 1000, 9001)
	other := signedForeignContact(t, id, 2000, 9005)
	// Genuine signature, different payload.
	spliced := append(append([]byte(nil), genuine[:64]...), other[64:]...)
	if !cache.verify(decodeRecord(t, genuine)) {
		t.Fatal("genuine rejected")
	}
	if cache.verify(decodeRecord(t, spliced)) {
		t.Fatal("spliced record accepted")
	}
	wrongKey := decodeRecord(t, genuine)
	wrongKey.Pubkey = Pubkey(newIdentity(t).Public().(ed25519.PublicKey))
	if cache.verify(wrongKey) {
		t.Fatal("cached bytes under another pubkey accepted")
	}
	before := cache.calls
	if !cache.verify(decodeRecord(t, genuine)) || cache.calls != before {
		t.Fatal("genuine entry lost after forgeries")
	}
	if s := cache.stats(); s.Rejected != 2 {
		t.Fatalf("rejected = %d, want 2", s.Rejected)
	}
}

func TestContactVerifyCacheBoundedGenerations(t *testing.T) {
	cache := newContactVerifyCache()
	cache.verifySignature = func(contactRecord) bool { return true }
	for i := 0; i < 3*contactVerifyGeneration; i++ {
		var r contactRecord
		r.signature[0], r.signature[1], r.signature[2] = byte(i), byte(i>>8), byte(i>>16)
		r.data = []byte{1}
		cache.verify(r)
	}
	if s := cache.stats(); s.Size > 2*contactVerifyGeneration {
		t.Fatalf("cache size %d exceeds bound", s.Size)
	}
}
