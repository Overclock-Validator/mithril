package watch

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func randKey(r *rand.Rand) solana.PublicKey {
	var pk solana.PublicKey
	r.Read(pk[:])
	return pk
}

func TestFilterMatchesOnlyMembers(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	keys := make([]solana.PublicKey, 1000)
	for i := range keys {
		keys[i] = randKey(r)
	}
	var sig solana.Signature
	r.Read(sig[:])
	f := Build(keys, []solana.Signature{sig})
	for i := range keys {
		if !f.HasKey(&keys[i]) {
			t.Fatalf("member %d missing", i)
		}
	}
	for i := 0; i < 10000; i++ {
		k := randKey(r)
		if f.HasKey(&k) {
			t.Fatalf("non-member %s matched", k)
		}
	}
	if !f.HasSignature(&sig) {
		t.Fatal("watched signature missing")
	}
	// A key equal to the signature's first 32 bytes must not match as a key.
	var aliased solana.PublicKey
	copy(aliased[:], sig[:32])
	if f.HasKey(&aliased) {
		t.Fatal("signature prefix matched as an account key")
	}
}

func TestFilterPrefixCollisionsAreResolvedByFullKey(t *testing.T) {
	var a, b, c solana.PublicKey
	binary.LittleEndian.PutUint64(a[:], 42)
	binary.LittleEndian.PutUint64(b[:], 42)
	binary.LittleEndian.PutUint64(c[:], 42)
	a[31], b[31], c[31] = 1, 2, 3
	var zero solana.PublicKey // prefix 0 is stored as 1
	f := Build([]solana.PublicKey{a, b, zero}, nil)
	if !f.HasKey(&a) || !f.HasKey(&b) || !f.HasKey(&zero) {
		t.Fatal("colliding members must all match")
	}
	if f.HasKey(&c) {
		t.Fatal("a key sharing a prefix but not watched matched")
	}
}

func TestNilFilterMatchesNothing(t *testing.T) {
	var f *Filter
	k := solana.PublicKey{1}
	if f.HasKey(&k) || f.MatchTransaction([]solana.PublicKey{k}, nil) || f.Len() != 0 {
		t.Fatal("nil filter matched")
	}
	if Build(nil, nil) != nil {
		t.Fatal("empty build should return nil")
	}
}

func TestRegistryPublishesSnapshots(t *testing.T) {
	reg := NewRegistry()
	wallet, program := solana.PublicKey{1}, solana.PublicKey{2}
	if reg.Current().Match != nil {
		t.Fatal("empty registry must publish a nil filter")
	}
	added := reg.AddKeys(Key{Key: wallet, Kind: Wallet}, Key{Key: program, Kind: Program}, Key{Key: wallet, Kind: Wallet})
	if len(added) != 2 {
		t.Fatalf("added %d keys, want 2", len(added))
	}
	snap := reg.Current()
	if !snap.Match.HasKey(&wallet) || !snap.Match.HasKey(&program) {
		t.Fatal("match filter misses registered keys")
	}
	if !snap.Owners.HasKey(&wallet) || snap.Owners.HasKey(&program) {
		t.Fatal("owner filter must hold wallets only")
	}

	// A second tenant shares the key; removing one tenant keeps it watched.
	reg.AddKeys(Key{Tenant: 7, Key: wallet, Kind: Wallet})
	reg.RemoveKey(0, wallet, Wallet)
	if !reg.Current().Match.HasKey(&wallet) {
		t.Fatal("key dropped while another tenant still watches it")
	}
	if got := reg.Tenants(wallet); len(got) != 1 || got[0] != 7 {
		t.Fatalf("tenants %v, want [7]", got)
	}

	var sig solana.Signature
	sig[0] = 9
	reg.AddSignature(sig, 100)
	if !reg.Current().Match.HasSignature(&sig) {
		t.Fatal("sent signature not watched")
	}
	reg.ExpireSignatures(101)
	if reg.Current().Match.HasSignature(&sig) {
		t.Fatal("expired signature still watched")
	}
}

func benchmarkHasKey(b *testing.B, n int, hit bool) {
	r := rand.New(rand.NewSource(2))
	keys := make([]solana.PublicKey, n)
	for i := range keys {
		keys[i] = randKey(r)
	}
	f := Build(keys, nil)
	probes := make([]solana.PublicKey, 1024)
	for i := range probes {
		if hit {
			probes[i] = keys[i%n]
		} else {
			probes[i] = randKey(r)
		}
	}
	b.ResetTimer()
	matched := 0
	for i := 0; i < b.N; i++ {
		if f.HasKey(&probes[i&1023]) {
			matched++
		}
	}
	_ = matched
}

func BenchmarkHasKeyMiss10(b *testing.B)    { benchmarkHasKey(b, 10, false) }
func BenchmarkHasKeyMiss10000(b *testing.B) { benchmarkHasKey(b, 10000, false) }
func BenchmarkHasKeyHit10000(b *testing.B)  { benchmarkHasKey(b, 10000, true) }

func TestRegistryRefusesKeysPastTheCap(t *testing.T) {
	saved := MaxKeys
	MaxKeys = 2
	defer func() { MaxKeys = saved }()
	reg := NewRegistry()
	added := reg.AddKeys(Key{Key: solana.PublicKey{1}, Kind: Wallet}, Key{Key: solana.PublicKey{2}, Kind: TokenAccount}, Key{Key: solana.PublicKey{3}, Kind: TokenAccount})
	if len(added) != 2 || reg.Refused() != 1 {
		t.Fatalf("added %d refused %d, want 2 and 1", len(added), reg.Refused())
	}
	// Adding a kind to an existing key is not limited.
	if got := reg.AddKeys(Key{Key: solana.PublicKey{1}, Kind: Program}); len(got) != 1 {
		t.Fatal("existing key should accept a new kind")
	}
}
