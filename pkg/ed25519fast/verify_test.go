package ed25519fast

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	mrand "math/rand"
	"testing"
)

// check asserts the package-level, cached-first-use and cached-hot
// paths all match crypto/ed25519.Verify.
func check(t *testing.T, c *Cache, pub *[32]byte, msg, sig []byte) {
	t.Helper()
	want := ed25519.Verify(pub[:], msg, sig)
	if got := Verify(pub, msg, sig); got != want {
		t.Fatalf("Verify=%v want %v\npub %x\nmsg %x\nsig %x", got, want, pub, msg, sig)
	}
	// Enough repetitions to cross the admission threshold, so the last
	// calls verify through the comb table.
	for i := 0; i < buildThreshold+2; i++ {
		if got := c.Verify(pub, msg, sig); got != want {
			t.Fatalf("Cache.Verify(#%d)=%v want %v\npub %x\nmsg %x\nsig %x", i, got, want, pub, msg, sig)
		}
	}
}

func TestDifferential(t *testing.T) {
	c := &Cache{}
	rng := mrand.New(mrand.NewSource(1))

	for round := 0; round < 300; round++ {
		pubk, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		var pub [32]byte
		copy(pub[:], pubk)
		msg := make([]byte, rng.Intn(300))
		rng.Read(msg)
		sig := ed25519.Sign(priv, msg)

		check(t, c, &pub, msg, sig)

		// Corrupt one bit somewhere in the signature, message or key.
		bad := append([]byte(nil), sig...)
		bad[rng.Intn(64)] ^= 1 << rng.Intn(8)
		check(t, c, &pub, msg, bad)

		if len(msg) > 0 {
			badMsg := append([]byte(nil), msg...)
			badMsg[rng.Intn(len(msg))] ^= 1 << rng.Intn(8)
			check(t, c, &pub, badMsg, sig)
		}

		badPub := pub
		badPub[rng.Intn(32)] ^= 1 << rng.Intn(8)
		check(t, c, &badPub, msg, sig)

		// Random garbage signature.
		garbage := make([]byte, 64)
		rng.Read(garbage)
		check(t, c, &pub, msg, garbage)

		// s with the top three bits set (quick-reject path) and
		// non-canonical s (s + L still under 2^256 for small s).
		high := append([]byte(nil), sig...)
		high[63] |= 224
		check(t, c, &pub, msg, high)

		nonCanonicalS := append([]byte(nil), sig...)
		// L = 2^252 + 27742317777372353535851937790883648493
		l := [32]byte{0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58,
			0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10}
		var carry uint16
		for i := 0; i < 32; i++ {
			v := carry + uint16(nonCanonicalS[32+i]) + uint16(l[i])
			nonCanonicalS[32+i] = byte(v)
			carry = v >> 8
		}
		if carry == 0 { // only meaningful when s+L fits 256 bits
			check(t, c, &pub, msg, nonCanonicalS)
		}

		// Wrong-length signatures.
		check(t, c, &pub, msg, sig[:63])
		check(t, c, &pub, msg, append(append([]byte(nil), sig...), 0))
	}
}

// Encodings with known edge-case behavior: small-order points, the
// identity, and non-canonical field encodings.
var edgePoints = []string{
	"0000000000000000000000000000000000000000000000000000000000000000",
	"0100000000000000000000000000000000000000000000000000000000000000",
	"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac03fa",
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc85",
	// Non-canonical encodings: y >= p.
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	"f3ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
}

func TestDifferentialEdgePoints(t *testing.T) {
	c := &Cache{}
	rng := mrand.New(mrand.NewSource(2))
	msg := []byte("edge case message")

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	goodSig := ed25519.Sign(priv, msg)
	goodPub := priv.Public().(ed25519.PublicKey)

	for _, hexEnc := range edgePoints {
		raw, err := hex.DecodeString(hexEnc)
		if err != nil || len(raw) != 32 {
			t.Fatalf("bad edge vector %q", hexEnc)
		}
		var edge [32]byte
		copy(edge[:], raw)

		// Edge encoding as the public key, with assorted signatures.
		check(t, c, &edge, msg, goodSig)
		for i := 0; i < 8; i++ {
			s := make([]byte, 64)
			rng.Read(s)
			s[63] &= 0x1f // keep s canonical-ish so we reach the equation
			check(t, c, &edge, msg, s)
			// Edge encoding as R, random s, honest key.
			var pub [32]byte
			copy(pub[:], goodPub)
			sig := make([]byte, 64)
			copy(sig[:32], edge[:])
			rng.Read(sig[32:])
			sig[63] &= 0x1f
			check(t, c, &pub, msg, sig)
		}
	}
}

func TestCacheAdmission(t *testing.T) {
	c := &Cache{}
	pubk, priv, _ := ed25519.GenerateKey(rand.Reader)
	var pub [32]byte
	copy(pub[:], pubk)
	msg := []byte("admission")
	sig := ed25519.Sign(priv, msg)

	for i := 1; i < buildThreshold; i++ {
		if !c.Verify(&pub, msg, sig) {
			t.Fatalf("verify %d failed", i)
		}
		if _, ok := c.tables.Load(pub); ok {
			t.Fatalf("table built after %d sightings", i)
		}
	}
	if !c.Verify(&pub, msg, sig) {
		t.Fatal("threshold verify failed")
	}
	if _, ok := c.tables.Load(pub); !ok {
		t.Fatal("table not built at the admission threshold")
	}
}

func BenchmarkStdlibVerify(b *testing.B) {
	pubk, priv, _ := ed25519.GenerateKey(rand.Reader)
	msg := make([]byte, 200)
	sig := ed25519.Sign(priv, msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !ed25519.Verify(pubk, msg, sig) {
			b.Fatal("verify failed")
		}
	}
}

func BenchmarkVerifyUncached(b *testing.B) {
	pubk, priv, _ := ed25519.GenerateKey(rand.Reader)
	var pub [32]byte
	copy(pub[:], pubk)
	msg := make([]byte, 200)
	sig := ed25519.Sign(priv, msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !Verify(&pub, msg, sig) {
			b.Fatal("verify failed")
		}
	}
}

func BenchmarkVerifyCached(b *testing.B) {
	c := &Cache{}
	pubk, priv, _ := ed25519.GenerateKey(rand.Reader)
	var pub [32]byte
	copy(pub[:], pubk)
	msg := make([]byte, 200)
	sig := ed25519.Sign(priv, msg)
	c.Verify(&pub, msg, sig)
	c.Verify(&pub, msg, sig) // build the table
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !c.Verify(&pub, msg, sig) {
			b.Fatal("verify failed")
		}
	}
}
