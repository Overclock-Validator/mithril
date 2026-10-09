package lthash

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/zeebo/blake3"
)

const numElements = 1024

type LtHash struct {
	value [numElements]uint16
}

// AccountHasher hashes accounts into reusable LtHash storage. It is intentionally
// stateful and must not be shared between goroutines.
//
// Its zero value is ready to use. Keeping one AccountHasher per worker avoids
// rebuilding the BLAKE3 state for every old and new account value.
type AccountHasher struct {
	hasher      blake3.Hasher
	initialized bool
}

func (hasher *AccountHasher) reset() {
	if hasher.initialized {
		hasher.hasher.Reset()
		return
	}

	hasher.hasher = *blake3.New()
	hasher.initialized = true
}

// HashInto replaces dst with the LtHash contribution for acct. Accounts with
// zero lamports do not contribute to the accounts LtHash.
func (hasher *AccountHasher) HashInto(dst *LtHash, acct *accounts.Account) {
	if acct.Lamports == 0 {
		dst.Reset()
		return
	}

	hasher.reset()

	var lamportBytes [8]byte
	binary.LittleEndian.PutUint64(lamportBytes[:], acct.Lamports)
	_, _ = hasher.hasher.Write(lamportBytes[:])

	_, _ = hasher.hasher.Write(acct.Data)

	var executableByte [1]byte
	if acct.Executable {
		executableByte[0] = 1
	}
	_, _ = hasher.hasher.Write(executableByte[:])

	_, _ = hasher.hasher.Write(acct.Owner[:])
	_, _ = hasher.hasher.Write(acct.Key[:])

	// BLAKE3's XOF writes directly into the reusable LtHash. The previous
	// implementation first allocated a separate 2 KiB output and copied it.
	_, _ = hasher.hasher.Digest().Read(dst.bytes())
}

func (ltHash *LtHash) InitWithAcct(acct *accounts.Account) *LtHash {
	var hasher AccountHasher
	hasher.HashInto(ltHash, acct)

	return ltHash
}

func (ltHash *LtHash) InitWithBytes(data []byte) *LtHash {
	hasher := blake3.New()
	hasher.Write(data)

	var output [2048]byte
	digest := hasher.Digest()
	digest.Read(output[:])

	for i := range numElements {
		val := binary.LittleEndian.Uint16(output[i*2 : (i*2)+2])
		ltHash.value[i] = val
	}

	return ltHash
}

func (ltHash *LtHash) InitWithHash(data []byte) *LtHash {
	if len(data) != numElements*2 {
		panic(fmt.Sprintf("wrong len of input data (%d)", len(data)))
	}

	for i := range numElements {
		ltHash.value[i] = binary.LittleEndian.Uint16(data[i*2 : (i*2)+2])
	}

	return ltHash
}

func (ltHash *LtHash) initRandom() *LtHash {
	rand.Read(ltHash.bytes())
	return ltHash
}

func (ltHash *LtHash) bytes() []byte {
	return unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
}

func (ltHash *LtHash) Reset() {
	clear(ltHash.value[:])
}

func (ltHash *LtHash) Clone() *LtHash {
	new := &LtHash{}
	copy(new.value[:], ltHash.value[:])
	return new
}

// MixIn adds other's 1024 lanes to ltHash, lane-wise modulo 2^16. The
// lane arithmetic is vectorized where the platform supports it (see mix.go).
func (ltHash *LtHash) MixIn(other *LtHash) {
	mixIn(&ltHash.value, &other.value)
}

// MixOut subtracts other's lanes from ltHash, the inverse of MixIn.
func (ltHash *LtHash) MixOut(other *LtHash) {
	mixOut(&ltHash.value, &other.value)
}

func (ltHash *LtHash) Add(other *LtHash) *LtHash {
	ltHash.MixIn(other)
	return ltHash
}

func (ltHash *LtHash) Sub(other *LtHash) *LtHash {
	ltHash.MixOut(other)
	return ltHash
}

func (ltHash *LtHash) Equals(other *LtHash) bool {
	return ltHash.value == other.value
}

func (ltHash *LtHash) Checksum() []byte {
	hasher := blake3.New()
	hasher.Write(ltHash.bytes())
	return hasher.Sum(nil)
}

func (ltHash *LtHash) Hash() []byte {
	return ltHash.bytes()
}
