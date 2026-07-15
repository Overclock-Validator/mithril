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

// HashByteLen is the serialized size of an LtHash accumulator. LtHash is a
// homomorphic 1024-element uint16 vector, not a conventional 32-byte digest.
const HashByteLen = numElements * 2

type LtHash struct {
	value [numElements]uint16
}

func (ltHash *LtHash) calculateAcctHash(acct *accounts.Account) []byte {
	hasher := blake3.New()

	var lamportBytes [8]byte
	binary.LittleEndian.PutUint64(lamportBytes[:], acct.Lamports)
	_, _ = hasher.Write(lamportBytes[:])

	_, _ = hasher.Write(acct.Data)

	if acct.Executable {
		_, _ = hasher.Write([]byte{1})
	} else {
		_, _ = hasher.Write([]byte{0})
	}

	_, _ = hasher.Write(acct.Owner[:])
	_, _ = hasher.Write(acct.Key[:])

	var data [HashByteLen]byte
	digest := hasher.Digest()
	digest.Read(data[:])

	return data[:]
}

func (ltHash *LtHash) InitWithAcct(acct *accounts.Account) *LtHash {
	if acct.Lamports == 0 {
		return &LtHash{}
	}

	hashData := ltHash.calculateAcctHash(acct)

	bytes := unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
	copy(bytes, hashData)

	return ltHash
}

func (ltHash *LtHash) InitWithBytes(data []byte) *LtHash {
	hasher := blake3.New()
	hasher.Write(data)

	var output [HashByteLen]byte
	digest := hasher.Digest()
	digest.Read(output[:])

	for i := range numElements {
		val := binary.LittleEndian.Uint16(output[i*2 : (i*2)+2])
		ltHash.value[i] = val
	}

	return ltHash
}

func (ltHash *LtHash) InitWithHash(data []byte) *LtHash {
	if len(data) != HashByteLen {
		panic(fmt.Sprintf("wrong len of input data (%d)", len(data)))
	}

	bytes := unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
	copy(bytes, data)

	return ltHash
}

func (ltHash *LtHash) initRandom() *LtHash {
	randBytes := unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
	rand.Read(randBytes)
	return ltHash
}

func (ltHash *LtHash) Clone() *LtHash {
	new := &LtHash{}
	copy(new.value[:], ltHash.value[:])
	return new
}

func (ltHash *LtHash) MixIn(other *LtHash) {
	for i := range numElements {
		ltHash.value[i] = ltHash.value[i] + other.value[i]
	}
}

func (ltHash *LtHash) MixOut(other *LtHash) {
	for i := range numElements {
		ltHash.value[i] = ltHash.value[i] - other.value[i]
	}
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
	for i, element := range ltHash.value {
		if element != other.value[i] {
			return false
		}
	}
	return true
}

func (ltHash *LtHash) Checksum() []byte {
	data := unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
	hasher := blake3.New()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func (ltHash *LtHash) Hash() []byte {
	data := unsafe.Slice((*uint8)(unsafe.Pointer(&ltHash.value[0])), numElements*2)
	return data
}
