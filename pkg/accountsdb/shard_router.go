package accountsdb

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dgryski/go-sip13"
	"github.com/gagliardetto/solana-go"
)

const (
	// DefaultPersistentIndexShards is part of the AccountsDB V2 on-disk
	// format. It deliberately differs from snapshot-index temporary sort
	// sharding: changing temporary build parallelism must never change where a
	// persistent account-index key lives.
	DefaultPersistentIndexShards = 1024
	MaxPersistentIndexShards     = 4096
)

var ErrInvalidPersistentIndexShardCount = errors.New("accountsdb: invalid persistent account-index shard count")

// PersistentIndexRoutingKey is the per-store account-index randomization key
// for durable
// shard routing. It is persisted in the root selector and copied into every
// immutable shard header. The key is local hash-flood randomization, not a
// long-term secret. It remains unchanged when a rewind rotates the chain
// lineage; changing it requires rebuilding every shard as a new index format.
type PersistentIndexRoutingKey [16]byte

// PersistentIndexShardRouter maps a Solana public key to a persistent index
// shard. Shard counts are powers of two, so routing consumes exactly the high
// log2(N) bits of a keyed hash over the complete public key. This keeps the
// expected load uniform even when valid traffic deliberately repeats a raw
// public-key prefix.
//
// The shard count and routing key are stored in the root catalog and are
// immutable for the life of the account-index store.
type PersistentIndexShardRouter struct {
	count uint32
	bits  uint8
	key   PersistentIndexRoutingKey
	k0    uint64
	k1    uint64
}

// NewPersistentIndexRoutingKey returns a non-zero routing identity suitable
// for a new account-index lineage.
func NewPersistentIndexRoutingKey() (PersistentIndexRoutingKey, error) {
	for {
		var key PersistentIndexRoutingKey
		if _, err := io.ReadFull(cryptorand.Reader, key[:]); err != nil {
			return key, fmt.Errorf("accountsdb: generate persistent shard routing key: %w", err)
		}
		if !allZero(key[:]) {
			return key, nil
		}
	}
}

// ValidatePersistentIndexShardCount accepts power-of-two counts from one
// through 4096. Production catalogs normally use
// DefaultPersistentIndexShards; the wider range permits small deterministic
// tests and a future explicitly migrated format.
func ValidatePersistentIndexShardCount(count int) error {
	if count < 1 || count > MaxPersistentIndexShards || count&(count-1) != 0 {
		return fmt.Errorf(
			"%w: got %d, want a power of two in [1,%d]",
			ErrInvalidPersistentIndexShardCount,
			count,
			MaxPersistentIndexShards,
		)
	}
	return nil
}

func NewPersistentIndexShardRouter(
	count int,
	key PersistentIndexRoutingKey,
) (PersistentIndexShardRouter, error) {
	if err := ValidatePersistentIndexShardCount(count); err != nil {
		return PersistentIndexShardRouter{}, err
	}
	if allZero(key[:]) {
		return PersistentIndexShardRouter{}, errors.New("accountsdb: zero persistent shard routing key")
	}
	bits := uint8(0)
	for n := count; n > 1; n >>= 1 {
		bits++
	}
	return PersistentIndexShardRouter{
		count: uint32(count),
		bits:  bits,
		key:   key,
		k0:    binary.LittleEndian.Uint64(key[0:8]),
		k1:    binary.LittleEndian.Uint64(key[8:16]),
	}, nil
}

func (router PersistentIndexShardRouter) Count() int {
	return int(router.count)
}

func (router PersistentIndexShardRouter) PrefixBits() uint8 {
	return router.bits
}

func (router PersistentIndexShardRouter) RoutingKey() PersistentIndexRoutingKey {
	return router.key
}

// Shard returns the shard selected by a keyed hash of the complete public key.
// SipHash-1-3 is the hash-table-flood-resistant variant used by several
// language runtimes: it prevents a remotely chosen raw prefix from collapsing
// all maintenance onto one shard while adding only a few nanoseconds per key.
// The zero-value router is invalid.
func (router PersistentIndexShardRouter) Shard(key solana.PublicKey) uint32 {
	if router.count == 0 || allZero(router.key[:]) {
		panic("accountsdb: use of an uninitialized persistent index shard router")
	}
	if router.bits == 0 {
		return 0
	}
	hashed := sip13.Sum64(router.k0, router.k1, key[:])
	return uint32(hashed >> (64 - router.bits))
}
