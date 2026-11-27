// Package watch decides which transactions RPC capture records.
//
// A Filter is an immutable membership set over account keys and transaction
// signatures. Public keys and signatures are uniformly random, so the first 8
// bytes already work as a hash: the table stores those prefixes in an
// open-addressed array kept at most half full, and confirms a prefix hit
// against the full key. A probe needs no hashing and, for personal watch sets,
// stays in cache.
//
// Replay loads the current Filter once per transaction through an
// atomic pointer. A nil Filter matches nothing, so an empty watch set costs
// one pointer load.
package watch

import (
	"encoding/binary"
	"math/bits"

	"github.com/gagliardetto/solana-go"
)

// Filter is immutable after Build.
type Filter struct {
	mask     uint64
	prefixes []uint64 // 0 marks an empty slot; a real zero prefix is stored as 1
	slots    []int32  // index into keys or sigs; negative is -(sig index)-1
	keys     []solana.PublicKey
	sigs     []solana.Signature
}

// Build returns a filter over keys and signatures, or nil if both are empty.
func Build(keys []solana.PublicKey, sigs []solana.Signature) *Filter {
	n := len(keys) + len(sigs)
	if n == 0 {
		return nil
	}
	size := uint64(1) << bits.Len64(uint64(2*n-1)) // a power of two >= 2n
	if size < 8 {
		size = 8
	}
	f := &Filter{
		mask:     size - 1,
		prefixes: make([]uint64, size),
		slots:    make([]int32, size),
		keys:     append([]solana.PublicKey(nil), keys...),
		sigs:     append([]solana.Signature(nil), sigs...),
	}
	for i := range f.keys {
		f.insert(prefix(f.keys[i][:]), int32(i))
	}
	for i := range f.sigs {
		f.insert(prefix(f.sigs[i][:]), -int32(i)-1)
	}
	return f
}

func prefix(b []byte) uint64 {
	p := binary.LittleEndian.Uint64(b)
	if p == 0 {
		return 1
	}
	return p
}

func (f *Filter) insert(p uint64, slot int32) {
	for i := p & f.mask; ; i = (i + 1) & f.mask {
		if f.prefixes[i] == 0 {
			f.prefixes[i] = p
			f.slots[i] = slot
			return
		}
	}
}

// Len is the number of keys and signatures in the filter.
func (f *Filter) Len() int {
	if f == nil {
		return 0
	}
	return len(f.keys) + len(f.sigs)
}

// HasKey reports whether pk is a watched account key.
func (f *Filter) HasKey(pk *solana.PublicKey) bool {
	if f == nil {
		return false
	}
	p := prefix(pk[:])
	for i := p & f.mask; ; i = (i + 1) & f.mask {
		switch q := f.prefixes[i]; {
		case q == 0:
			return false
		case q == p:
			if s := f.slots[i]; s >= 0 && f.keys[s] == *pk {
				return true
			}
		}
	}
}

// HasSignature reports whether sig is a watched signature.
func (f *Filter) HasSignature(sig *solana.Signature) bool {
	if f == nil || len(f.sigs) == 0 {
		return false
	}
	p := prefix(sig[:])
	for i := p & f.mask; ; i = (i + 1) & f.mask {
		switch q := f.prefixes[i]; {
		case q == 0:
			return false
		case q == p:
			if s := f.slots[i]; s < 0 && f.sigs[-s-1] == *sig {
				return true
			}
		}
	}
}

// MatchTransaction reports whether a transaction with these account keys and
// first signature is watched. keys must include lookup-table-loaded keys.
func (f *Filter) MatchTransaction(keys []solana.PublicKey, sig *solana.Signature) bool {
	if f == nil {
		return false
	}
	for i := range keys {
		if f.HasKey(&keys[i]) {
			return true
		}
	}
	return sig != nil && f.HasSignature(sig)
}
