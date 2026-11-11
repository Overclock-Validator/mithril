package replay

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/base58"
)

// hashEncodeCacheGeneration bounds one generation of the base58 cache. The
// resume context encodes the RecentBlockhashes (150) and SlotHashes (512)
// windows on every block; each block adds one or two hashes to them, so two
// generations of this size keep every hash a block needs.
const hashEncodeCacheGeneration = 2048

// hashEncodeCache remembers base58 encodings of 32-byte hashes, so a block
// encodes only the hashes that entered the sysvar windows since the last one
// instead of all ~660. Encoded strings are immutable and safe to share.
var hashEncodeCache = struct {
	sync.Mutex
	cur, prev map[[32]byte]string
}{cur: make(map[[32]byte]string, hashEncodeCacheGeneration)}

func encodeHashCached(h [32]byte) string {
	c := &hashEncodeCache
	c.Lock()
	defer c.Unlock()
	if s, ok := c.cur[h]; ok {
		return s
	}
	s, ok := c.prev[h]
	if !ok {
		s = base58.Encode(h[:])
	}
	if len(c.cur) >= hashEncodeCacheGeneration {
		c.prev, c.cur = c.cur, make(map[[32]byte]string, hashEncodeCacheGeneration)
	}
	c.cur[h] = s
	return s
}
