// Package ed25519fast verifies ed25519 signatures with acceptance
// behavior bit-identical to crypto/ed25519.Verify, sped up for signers
// that recur: a Cache holds a fixed-base comb table per hot public key
// (Solana vote authorities sign every slot, and busy fee payers repeat
// across blocks), which removes the doubling chain from verification.
// The arithmetic is the vendored crypto/ed25519 internals, so the two
// implementations agree on every decoding edge case by construction;
// TestDifferential exercises that claim.
package ed25519fast

import (
	"bytes"
	"crypto/sha512"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/ed25519fast/internal/edwards25519"
)

// Verify reports whether sig is a valid signature of message by pub,
// with the exact acceptance behavior of crypto/ed25519.Verify.
func Verify(pub *[32]byte, message, sig []byte) bool {
	return verify(pub, message, sig, nil)
}

func verify(pub *[32]byte, message, sig []byte, table *edwards25519.PubkeyTable) bool {
	if len(sig) != 64 || sig[63]&224 != 0 {
		return false
	}

	var minusA *edwards25519.Point
	if table == nil {
		A, err := (&edwards25519.Point{}).SetBytes(pub[:])
		if err != nil {
			return false
		}
		minusA = (&edwards25519.Point{}).Negate(A)
	}

	kh := sha512.New()
	kh.Write(sig[:32])
	kh.Write(pub[:])
	kh.Write(message)
	hramDigest := kh.Sum(nil)
	k, err := edwards25519.NewScalar().SetUniformBytes(hramDigest)
	if err != nil {
		return false
	}

	s, err := edwards25519.NewScalar().SetCanonicalBytes(sig[32:])
	if err != nil {
		return false
	}

	// R = [s]B - [k]A must re-encode to the signature's R bytes. The
	// cached table holds -A, so the comb path is all additions.
	var r *edwards25519.Point
	if table != nil {
		r = (&edwards25519.Point{}).VarTimeDoubleCombMult(k, table, s)
	} else {
		r = (&edwards25519.Point{}).VarTimeDoubleScalarBaseMult(k, minusA, s)
	}
	return bytes.Equal(sig[:32], r.Bytes())
}

// Cache verifies signatures, building a per-key comb table for public
// keys that keep recurring. It is safe for concurrent use.
//
// A table costs about eight verifications to build, so admission waits
// for buildThreshold sightings: vote authorities and busy fee payers
// cross it immediately, one-shot keys never earn a table. The sighting
// counters are cleared when their map grows large, which only makes a
// hot key re-earn its table. Tables are never evicted: a stale entry
// costs 30 KiB and hot keys are stable on the scale of epochs.
type Cache struct {
	// MaxEntries bounds the number of cached tables (~30 KiB each).
	// Zero means DefaultMaxEntries.
	MaxEntries int64

	tables    sync.Map // [32]byte -> *edwards25519.PubkeyTable
	count     atomic.Int64
	seen      sync.Map // [32]byte -> *atomic.Int32
	seenCount atomic.Int64
}

const DefaultMaxEntries = 4096

const buildThreshold = 8

const seenResetThreshold = 1 << 17

// Verify reports whether sig is a valid signature of message by pub,
// exactly like the package-level Verify.
func (c *Cache) Verify(pub *[32]byte, message, sig []byte) bool {
	if t, ok := c.tables.Load(*pub); ok {
		return verify(pub, message, sig, t.(*edwards25519.PubkeyTable))
	}

	v, seenBefore := c.seen.LoadOrStore(*pub, new(atomic.Int32))
	if !seenBefore {
		if c.seenCount.Add(1) > seenResetThreshold {
			c.seenCount.Store(0)
			c.seen.Clear()
		}
	}
	if v.(*atomic.Int32).Add(1) < buildThreshold {
		return verify(pub, message, sig, nil)
	}

	max := c.MaxEntries
	if max == 0 {
		max = DefaultMaxEntries
	}
	if c.count.Load() < max {
		if A, err := (&edwards25519.Point{}).SetBytes(pub[:]); err == nil {
			table := edwards25519.NewPubkeyTable((&edwards25519.Point{}).Negate(A))
			if _, loaded := c.tables.LoadOrStore(*pub, table); !loaded {
				c.count.Add(1)
			}
			return verify(pub, message, sig, table)
		}
	}
	return verify(pub, message, sig, nil)
}
