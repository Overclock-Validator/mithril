package turbine

import (
	"fmt"
	"sync"
	"sync/atomic"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
	"github.com/gagliardetto/solana-go"
)

// The leader signs ONE merkle root per FEC set; every shred of the set
// carries that same signature plus a per-shred proof. So of a set's ~64
// shreds only the first needs the expensive ed25519 verify — a sibling whose
// proof (still walked per shred: the hash chain is what binds THIS shred's
// bytes to the root) resolves to an already-verified (leader, root,
// signature) triple is authenticated by the chain alone. Signature verifies
// drop from one per shred to one per FEC set while staying bit-for-bit
// equivalent to verifying each: strict verification is deterministic, so a
// hit reproduces exactly the result of re-running it on the same inputs.
// Tampered content can never hit — different bytes yield a different root,
// hence a different key. Failures are never cached.
// ShredSignatureVerifier authenticates Merkle shreds with the same bounded,
// per-root result cache used by UDPReceiver. The cache never stores failures;
// each packet's Merkle proof is still evaluated before a cache lookup.
//
// It is exported so deterministic and loopback ingress harnesses can exercise
// production validation without constructing a UDPReceiver.
type ShredSignatureVerifier struct {
	mu   sync.Mutex
	cur  map[shredSigCacheKey]struct{}
	prev map[shredSigCacheKey]struct{}

	hits     atomic.Uint64
	verifies atomic.Uint64
}

// Keep the internal receiver/test name as an alias; there is one
// implementation and one cache contract.
type shredSigCache = ShredSignatureVerifier

type shredSigCacheKey struct {
	leader solana.PublicKey
	root   solana.Hash
	sig    solana.Signature
}

// shredSigCacheGenCap bounds each of the two rotating generations. Hits are
// overwhelmingly intra-set within ~100ms of arrival; 4096 roots spans many
// slots' worth of sets, so rotation almost never evicts a still-hot entry.
const shredSigCacheGenCap = 4096

// verifyShred authenticates a shred exactly like Shred.VerifySignature, with
// the per-root ed25519 result cached.
func (c *ShredSignatureVerifier) verifyShred(s *Shred, leader solana.PublicKey) error {
	root, err := s.MerkleRoot()
	if err != nil {
		return err
	}
	key := shredSigCacheKey{leader: leader, root: root, sig: s.Signature}

	c.mu.Lock()
	if _, ok := c.cur[key]; ok {
		c.mu.Unlock()
		c.hits.Add(1)
		return nil
	}
	if _, ok := c.prev[key]; ok {
		// Promote: a set straddling a rotation keeps its entry hot.
		c.addLocked(key)
		c.mu.Unlock()
		c.hits.Add(1)
		return nil
	}
	c.mu.Unlock()

	c.verifies.Add(1)
	if !narya.VerifyStrict(leader[:], root[:], s.Signature[:]) {
		return fmt.Errorf("%w: slot %d shred %d", ErrInvalidSignature, s.Slot, s.Index)
	}
	c.mu.Lock()
	c.addLocked(key)
	c.mu.Unlock()
	return nil
}

// Verify authenticates one shred and retains successful root/signature tuples
// for sibling shreds in the same FEC set.
func (c *ShredSignatureVerifier) Verify(s *Shred, leader solana.PublicKey) error {
	return c.verifyShred(s, leader)
}

func (c *ShredSignatureVerifier) addLocked(key shredSigCacheKey) {
	if c.cur == nil {
		c.cur = make(map[shredSigCacheKey]struct{}, shredSigCacheGenCap)
	}
	if len(c.cur) >= shredSigCacheGenCap {
		c.prev = c.cur
		c.cur = make(map[shredSigCacheKey]struct{}, shredSigCacheGenCap)
	}
	c.cur[key] = struct{}{}
}

func (c *ShredSignatureVerifier) stats() (hits, verifies uint64) {
	return c.hits.Load(), c.verifies.Load()
}

// Stats reports cache hits and actual Ed25519 verifications.
func (c *ShredSignatureVerifier) Stats() (hits, verifies uint64) {
	return c.stats()
}
