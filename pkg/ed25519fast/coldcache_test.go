package ed25519fast

import (
	"crypto/ed25519"
	"crypto/rand"
	"sync/atomic"
	"testing"
)

// BenchmarkVerifyCachedManyKeys measures the cached path when the
// table working set exceeds the CPU caches, cycling through N keys.
func benchManyKeys(b *testing.B, n int) {
	c := &Cache{MaxEntries: int64(n) + 10}
	pubs := make([][32]byte, n)
	sigs := make([][]byte, n)
	msg := make([]byte, 200)
	for i := range pubs {
		pubk, priv, _ := ed25519.GenerateKey(rand.Reader)
		copy(pubs[i][:], pubk)
		sigs[i] = ed25519.Sign(priv, msg)
		for j := 0; j < buildThreshold; j++ { // build table
			c.Verify(&pubs[i], msg, sigs[i])
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := i % n
		if !c.Verify(&pubs[k], msg, sigs[k]) {
			b.Fatal("verify failed")
		}
	}
}

func BenchmarkVerifyCached16Keys(b *testing.B)   { benchManyKeys(b, 16) }
func BenchmarkVerifyCached512Keys(b *testing.B)  { benchManyKeys(b, 512) }
func BenchmarkVerifyCached4096Keys(b *testing.B) { benchManyKeys(b, 4096) }

func BenchmarkTableBuild(b *testing.B) {
	pubk, _, _ := ed25519.GenerateKey(rand.Reader)
	var pub [32]byte
	copy(pub[:], pubk)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &Cache{}
		n := new(atomic.Int32)
		n.Store(buildThreshold)
		c.seen.Store(pub, n)
		msg := []byte("m")
		c.Verify(&pub, msg, make([]byte, 64))
	}
}
