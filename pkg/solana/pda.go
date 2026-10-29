package solana

import (
	"crypto/sha256"
	"errors"
	"sync"

	"filippo.io/edwards25519"
)

const MaxSeeds = 16
const MaxSeedLen = 32
const PublicKeyLength = 32
const PdaMarker = "ProgramDerivedAddress"

var (
	ErrSeedLength          = errors.New("Max seeds (16) exceeded")
	ErrAddressLength       = errors.New("Wrong key length; addresses are 32 bytes long")
	ErrOnCurveInvalidSeeds = errors.New("Invalid seeds - generated address must be off-curve")
)

func CreateProgramAddressBytes(seeds [][]byte, programID []byte) ([]byte, error) {
	if len(seeds) > MaxSeeds {
		return nil, ErrSeedLength
	}

	if len(programID) != PublicKeyLength {
		return nil, ErrAddressLength
	}

	hasher := sha256.New()
	for _, seed := range seeds {
		if len(seed) > MaxSeedLen {
			return nil, ErrSeedLength
		}
		hasher.Write(seed)
	}

	hasher.Write(programID)
	hasher.Write([]byte(PdaMarker))
	hash := hasher.Sum(nil)

	if IsOnCurve(hash[:]) {
		return nil, ErrOnCurveInvalidSeeds
	}

	return hash[:], nil
}

// IsOnCurve checks if 'b' is on the ed25519 curve
// The on-curve check (a field square root) is most of a PDA derivation's cost,
// and programs derive the same addresses over and over. It is a pure function of
// the 32 bytes, so recent answers are remembered, in shards that are simply
// emptied when full to bound memory.
const (
	onCurveMemoShards     = 64
	onCurveMemoShardLimit = 4096
)

var onCurveMemo [onCurveMemoShards]struct {
	mu sync.RWMutex
	m  map[[32]byte]bool
}

func IsOnCurve(b []byte) bool {
	if len(b) != 32 {
		return isOnCurve(b)
	}
	key := [32]byte(b)
	shard := &onCurveMemo[key[0]%onCurveMemoShards]
	shard.mu.RLock()
	onCurve, ok := shard.m[key]
	shard.mu.RUnlock()
	if ok {
		return onCurve
	}
	onCurve = isOnCurve(b)
	shard.mu.Lock()
	if shard.m == nil || len(shard.m) >= onCurveMemoShardLimit {
		shard.m = make(map[[32]byte]bool, onCurveMemoShardLimit)
	}
	shard.m[key] = onCurve
	shard.mu.Unlock()
	return onCurve
}

func isOnCurve(b []byte) bool {
	_, err := new(edwards25519.Point).SetBytes(b)
	return err == nil
}
