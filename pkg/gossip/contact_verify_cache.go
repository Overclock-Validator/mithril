package gossip

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// contactVerifyGeneration bounds each of the cache's two generations. Every
// node re-signs its ContactInfo with a fresh wallclock periodically, so the
// cache needs room for the distinct values seen over a few refresh periods,
// not one per peer. Entries cost about 64 bytes of key plus a payload of a
// few hundred bytes, so two full generations stay around 20 MB at most.
const contactVerifyGeneration = 32768

// contactVerifyCache remembers the exact signed bytes of contact records whose
// signatures verified. Gossip delivers the same CRDS value many times (pull
// responses and pushes relayed by several peers), and verifying each copy was
// most of the receive path's CPU.
//
// Entries are keyed by signature, not by origin pubkey: relays hold different
// versions of one origin's record, so older and newer copies arrive
// interleaved, and a single slot per pubkey made them evict each other.
//
// The rule is byte-for-byte: a record skips verification only when an entry
// with its signature holds the same pubkey and the same signed payload, that
// is, the identical message already passed narya.VerifyStrict. Anything else
// is verified as before. Entries are written only after a successful
// verification, so a forged record can neither be accepted nor displace or
// suppress a genuine one.
//
// Eviction is generational: when the current map fills, it becomes the
// previous map and the older one is dropped. Lookups check both, so values
// still being relayed survive one rotation.
type contactVerifyCache struct {
	mu       sync.Mutex
	current  map[Signature]verifiedContact
	previous map[Signature]verifiedContact
	// verifySignature is contactRecord.Verify; tests replace it to count calls.
	verifySignature func(contactRecord) bool

	hits     atomic.Uint64
	verified atomic.Uint64
	rejected atomic.Uint64
}

type verifiedContact struct {
	pubkey Pubkey
	data   []byte
}

func newContactVerifyCache() *contactVerifyCache {
	return &contactVerifyCache{
		current:         make(map[Signature]verifiedContact),
		verifySignature: contactRecord.Verify,
	}
}

func (e verifiedContact) matches(record contactRecord) bool {
	return e.pubkey == record.Pubkey && bytes.Equal(e.data, record.data)
}

// verify reports whether record carries a valid signature, using the cache to
// skip the signature check for an exact repeat of an already verified record.
// A nil cache always verifies.
func (c *contactVerifyCache) verify(record contactRecord) bool {
	if c == nil {
		return record.Verify()
	}
	c.mu.Lock()
	entry, ok := c.current[record.signature]
	if !ok {
		entry, ok = c.previous[record.signature]
	}
	hit := ok && entry.matches(record)
	c.mu.Unlock()
	if hit {
		c.hits.Add(1)
		return true
	}
	if !c.verifySignature(record) {
		c.rejected.Add(1)
		return false
	}
	c.verified.Add(1)
	// record.data aliases the receive buffer, so keep a private copy.
	stored := verifiedContact{pubkey: record.Pubkey, data: append([]byte(nil), record.data...)}
	c.mu.Lock()
	if len(c.current) >= contactVerifyGeneration {
		c.previous = c.current
		c.current = make(map[Signature]verifiedContact, contactVerifyGeneration)
	}
	c.current[record.signature] = stored
	c.mu.Unlock()
	return true
}

type contactVerifyStats struct {
	Hits, Verified, Rejected uint64
	Size                     int
}

func (c *contactVerifyCache) stats() contactVerifyStats {
	if c == nil {
		return contactVerifyStats{}
	}
	c.mu.Lock()
	size := len(c.current) + len(c.previous)
	c.mu.Unlock()
	return contactVerifyStats{Hits: c.hits.Load(), Verified: c.verified.Load(), Rejected: c.rejected.Load(), Size: size}
}
