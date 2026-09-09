package accountsdb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/gagliardetto/solana-go"
)

// mergedShardIndexSource is the bounded-memory input to a rolling base
// rebase. The base and delta must be pinned by the caller for every Scan. The
// exact, sorted delta wins on equal keys; tombstones suppress the older base
// record. Mutations newer than the delta's covered sequence intentionally stay
// in the mutable head and are not folded into this generation.
type mergedShardIndexSource struct {
	base  *ShardedStreamBaseShard
	delta *DeltaCheckpoint
}

func newMergedShardIndexSource(base *ShardedStreamBaseShard, delta *DeltaCheckpoint) (StreamIndexSource, error) {
	if base == nil {
		return nil, errors.New("accountsdb: rolling rebase requires an immutable base shard")
	}
	base.mu.RLock()
	closed := base.closed || base.idx == nil || base.catalog.Load() == nil || base.scanInfo == nil
	base.mu.RUnlock()
	if closed {
		return nil, errors.New("accountsdb: rolling rebase base shard is closed")
	}
	return &mergedShardIndexSource{base: base, delta: delta}, nil
}

type shardedBaseMergeCursor struct {
	shard    *ShardedStreamBaseShard
	catalog  *PersistentExtentCatalog
	reader   *bufio.Reader
	encoded  [shardedBaseScanRecordSize]byte
	ordinal  uint64
	bodyCRC  hash32
	previous solana.PublicKey
	havePrev bool
}

// hash32 is the subset of hash.Hash32 used by the cursor. Keeping the narrow
// interface makes the cursor easy to exercise with the standard CRC32C hash.
type hash32 interface {
	Write([]byte) (int, error)
	Sum32() uint32
}

func newShardedBaseMergeCursor(
	shard *ShardedStreamBaseShard,
	catalog *PersistentExtentCatalog,
	scan *os.File,
) *shardedBaseMergeCursor {
	// Keep reads sequential and bounded without changing the file's offset.
	records := io.NewSectionReader(
		scan, shardedBaseMetadataSize, int64(shard.metadata.KeyCount)*shardedBaseScanRecordSize,
	)
	return &shardedBaseMergeCursor{
		shard:   shard,
		catalog: catalog,
		reader:  bufio.NewReaderSize(records, 256<<10),
		bodyCRC: crc32.New(shardedBaseCRC),
	}
}

func (cursor *shardedBaseMergeCursor) next() (solana.PublicKey, AccountIndexEntry, bool, error) {
	if cursor.ordinal >= cursor.shard.metadata.KeyCount {
		if got, want := cursor.bodyCRC.Sum32(), cursor.shard.metadata.BodyCRC; got != want {
			return solana.PublicKey{}, AccountIndexEntry{}, false, fmt.Errorf(
				"%w: shard %d scan body CRC mismatch during rebase",
				ErrInvalidShardedStreamBase,
				cursor.shard.metadata.ShardID,
			)
		}
		return solana.PublicKey{}, AccountIndexEntry{}, false, nil
	}

	encoded := cursor.encoded[:]
	if _, err := io.ReadFull(cursor.reader, encoded); err != nil {
		return solana.PublicKey{}, AccountIndexEntry{}, false, fmt.Errorf(
			"accountsdb: read shard %d base record %d: %w",
			cursor.shard.metadata.ShardID,
			cursor.ordinal,
			err,
		)
	}
	_, _ = cursor.bodyCRC.Write(encoded[:])
	key := solana.PublicKey(encoded[:32])
	if cursor.havePrev && bytes.Compare(cursor.previous[:], key[:]) >= 0 {
		return solana.PublicKey{}, AccountIndexEntry{}, false, fmt.Errorf(
			"%w: shard %d scan record %d is not strictly ordered",
			ErrInvalidShardedStreamBase,
			cursor.shard.metadata.ShardID,
			cursor.ordinal,
		)
	}
	if routed := cursor.shard.router.Shard(key); routed != cursor.shard.metadata.ShardID {
		return solana.PublicKey{}, AccountIndexEntry{}, false, fmt.Errorf(
			"%w: scan record %d routes to shard %d, want %d",
			ErrInvalidShardedStreamBase,
			cursor.ordinal,
			routed,
			cursor.shard.metadata.ShardID,
		)
	}
	entry, err := cursor.catalog.UnpackAccountIndexEntry(uint48(encoded[32:]))
	if err != nil {
		return solana.PublicKey{}, AccountIndexEntry{}, false, err
	}
	cursor.previous = key
	cursor.havePrev = true
	cursor.ordinal++
	return key, entry, true, nil
}

func (source *mergedShardIndexSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) (retErr error) {
	if ctx == nil {
		return errors.New("accountsdb: nil rolling-rebase scan context")
	}
	if visit == nil {
		return errors.New("accountsdb: nil rolling-rebase visitor")
	}
	if source == nil || source.base == nil {
		return errors.New("accountsdb: nil rolling-rebase source")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	source.base.mu.RLock()
	defer source.base.mu.RUnlock()
	catalog := source.base.catalog.Load()
	if catalog == nil {
		return errors.New("accountsdb: rolling-rebase base shard is closed")
	}
	scan, err := source.base.openScanLockedContext(ctx)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, source.base.validateOpenScanIdentity(scan), scan.Close())
	}()

	// DeltaCheckpoint.Close takes the write side. Runtime generation pinning is
	// the normal lifetime guarantee; this lock is defense in depth for direct
	// tests and tooling that construct a source without an IndexReadView.
	if source.delta != nil {
		source.delta.mu.RLock()
		defer source.delta.mu.RUnlock()
		if source.delta.closed {
			return errors.New("accountsdb: rolling-rebase delta checkpoint is closed")
		}
	}

	base := newShardedBaseMergeCursor(source.base, catalog, scan)
	baseKey, baseValue, haveBase, err := base.next()
	if err != nil {
		return err
	}
	var deltaOrdinal uint64
	var deltaKey solana.PublicKey
	var deltaValue deltaIndexValue
	haveDelta := false
	nextDelta := func() error {
		if source.delta == nil || deltaOrdinal >= source.delta.recordCount {
			haveDelta = false
			return nil
		}
		key, value, err := decodeDeltaCheckpointRecord(source.delta.records, deltaOrdinal)
		if err != nil {
			return fmt.Errorf("accountsdb: decode rolling-rebase delta record %d: %w", deltaOrdinal, err)
		}
		deltaOrdinal++
		deltaKey, deltaValue, haveDelta = key, value, true
		return nil
	}
	if err := nextDelta(); err != nil {
		return err
	}

	var emitted uint64
	for haveBase || haveDelta {
		if emitted%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		switch {
		case !haveDelta || (haveBase && bytes.Compare(baseKey[:], deltaKey[:]) < 0):
			if err := visit(baseKey, baseValue); err != nil {
				return err
			}
			emitted++
			baseKey, baseValue, haveBase, err = base.next()
			if err != nil {
				return err
			}

		case !haveBase || bytes.Compare(deltaKey[:], baseKey[:]) < 0:
			if !deltaValue.Tombstone {
				if err := visit(deltaKey, deltaValue.Entry); err != nil {
					return err
				}
				emitted++
			}
			if err := nextDelta(); err != nil {
				return err
			}

		default:
			// Equal key: the delta is newer. Its tombstone removes the key.
			if !deltaValue.Tombstone {
				if err := visit(deltaKey, deltaValue.Entry); err != nil {
					return err
				}
				emitted++
			}
			baseKey, baseValue, haveBase, err = base.next()
			if err != nil {
				return err
			}
			if err := nextDelta(); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
