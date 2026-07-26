// Package txstatus contains transaction-status identities and interchange
// types shared by snapshot ingestion, replay, and block production.
package txstatus

import (
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/wincode"
)

const (
	// SnapshotSeedFileName is the raw snapshots/status_cache member retained
	// from the latest Agave snapshot archive used to build AccountsDB.
	SnapshotSeedFileName = "snapshot_status_cache.bin"

	CachedKeySize = 20
	// Agave clamps the offset with len(key)-(CACHED_KEY_SIZE+1). Transaction
	// message hashes are 32 bytes, so an imported offset greater than 11 could
	// not be queried with the runtime key and must be rejected.
	MaxCachedKeyIndex = 11
)

// SnapshotSlotDelta is one slot entry from Agave's snapshots/status_cache.
// Statuses contains both transaction-message and signature key slices. Agave
// intentionally mixes them in the same 160-bit KeyMap, so consumers must keep
// every key to preserve its lookup semantics.
type SnapshotSlotDelta struct {
	Slot     uint64
	IsRoot   bool
	Statuses []SnapshotStatus
}

// SnapshotStatus groups cached key slices by the transaction recent
// blockhash and by Agave's per-blockhash slice offset.
type SnapshotStatus struct {
	RecentBlockhash [32]byte
	KeyIndex        uint64
	Keys            [][CachedKeySize]byte
}

// DecodeAgaveSnapshot decodes the raw snapshots/status_cache member written by
// Agave v4.2's wincode SerdeBankSlotDelta format. Transaction results are
// validated and skipped: AlreadyProcessed admission only needs their keys.
// Unknown enum variants fail closed so a format upgrade cannot silently seed
// an incomplete cache.
func DecodeAgaveSnapshot(data []byte) ([]SnapshotSlotDelta, error) {
	r := wincode.NewReader(data)
	slotCount, err := readCount(r, "slot deltas", 17)
	if err != nil {
		return nil, err
	}

	deltas := make([]SnapshotSlotDelta, 0, slotCount)
	for slotIdx := 0; slotIdx < slotCount; slotIdx++ {
		slot, err := r.ReadU64()
		if err != nil {
			return nil, fmt.Errorf("status cache slot delta %d: read slot: %w", slotIdx, err)
		}
		rootByte, err := r.ReadU8()
		if err != nil {
			return nil, fmt.Errorf("status cache slot delta %d: read root flag: %w", slotIdx, err)
		}
		if rootByte > 1 {
			return nil, fmt.Errorf("status cache slot delta %d: invalid root flag %d", slotIdx, rootByte)
		}

		statusCount, err := readCount(r, fmt.Sprintf("slot delta %d statuses", slotIdx), 48)
		if err != nil {
			return nil, err
		}
		delta := SnapshotSlotDelta{
			Slot:     slot,
			IsRoot:   rootByte == 1,
			Statuses: make([]SnapshotStatus, 0, statusCount),
		}
		for statusIdx := 0; statusIdx < statusCount; statusIdx++ {
			blockhash, err := r.ReadBytes(32)
			if err != nil {
				return nil, fmt.Errorf("status cache slot delta %d status %d: read blockhash: %w", slotIdx, statusIdx, err)
			}
			keyIndex, err := r.ReadU64() // usize is encoded as u64 on Agave's 64-bit target
			if err != nil {
				return nil, fmt.Errorf("status cache slot delta %d status %d: read key index: %w", slotIdx, statusIdx, err)
			}
			if keyIndex > MaxCachedKeyIndex {
				return nil, fmt.Errorf("status cache slot delta %d status %d: key index %d exceeds maximum %d for a 32-byte key", slotIdx, statusIdx, keyIndex, MaxCachedKeyIndex)
			}

			keyCount, err := readCount(r, fmt.Sprintf("slot delta %d status %d keys", slotIdx, statusIdx), CachedKeySize+4)
			if err != nil {
				return nil, err
			}
			status := SnapshotStatus{KeyIndex: keyIndex, Keys: make([][CachedKeySize]byte, 0, keyCount)}
			copy(status.RecentBlockhash[:], blockhash)
			for keyIdx := 0; keyIdx < keyCount; keyIdx++ {
				keyBytes, err := r.ReadBytes(CachedKeySize)
				if err != nil {
					return nil, fmt.Errorf("status cache slot delta %d status %d key %d: read key: %w", slotIdx, statusIdx, keyIdx, err)
				}
				var key [CachedKeySize]byte
				copy(key[:], keyBytes)
				if err := skipTransactionResult(r); err != nil {
					return nil, fmt.Errorf("status cache slot delta %d status %d key %d: %w", slotIdx, statusIdx, keyIdx, err)
				}
				status.Keys = append(status.Keys, key)
			}
			delta.Statuses = append(delta.Statuses, status)
		}
		deltas = append(deltas, delta)
	}

	if err := r.EnsureEOF(); err != nil {
		return nil, fmt.Errorf("status cache: %w", err)
	}
	return deltas, nil
}

func readCount(r *wincode.Reader, name string, minElementBytes uint64) (int, error) {
	n, err := r.ReadU64()
	if err != nil {
		return 0, fmt.Errorf("status cache: read %s length: %w", name, err)
	}
	if n > uint64(math.MaxInt) {
		return 0, fmt.Errorf("status cache: %s length %d exceeds platform int", name, n)
	}
	if minElementBytes != 0 && n > uint64(r.Remaining())/minElementBytes {
		return 0, fmt.Errorf("status cache: %s length %d cannot fit in %d remaining bytes", name, n, r.Remaining())
	}
	return int(n), nil
}

// Result<(), SerdeTransactionError> uses wincode's configured u32 tag.
func skipTransactionResult(r *wincode.Reader) error {
	resultTag, err := r.ReadU32()
	if err != nil {
		return fmt.Errorf("read transaction result tag: %w", err)
	}
	switch resultTag {
	case 0: // Ok(())
		return nil
	case 1: // Err(SerdeTransactionError)
		return skipTransactionError(r)
	default:
		return fmt.Errorf("unknown transaction result tag %d", resultTag)
	}
}

func skipTransactionError(r *wincode.Reader) error {
	tag, err := r.ReadU32()
	if err != nil {
		return fmt.Errorf("read transaction error tag: %w", err)
	}
	switch tag {
	case 8: // InstructionError(u8, SerdeInstructionError)
		if _, err := r.ReadU8(); err != nil {
			return fmt.Errorf("read instruction index: %w", err)
		}
		return skipInstructionError(r)
	case 30, // DuplicateInstruction(u8)
		31, // InsufficientFundsForRent { account_index: u8 }
		35: // ProgramExecutionTemporarilyRestricted { account_index: u8 }
		if _, err := r.ReadU8(); err != nil {
			return fmt.Errorf("read transaction error %d payload: %w", tag, err)
		}
		return nil
	default:
		// All remaining v4.2 variants are unit variants. The highest is
		// CommitCancelled (38).
		if tag <= 38 {
			return nil
		}
		return fmt.Errorf("unknown transaction error tag %d", tag)
	}
}

func skipInstructionError(r *wincode.Reader) error {
	tag, err := r.ReadU32()
	if err != nil {
		return fmt.Errorf("read instruction error tag: %w", err)
	}
	switch tag {
	case 25: // Custom(u32)
		if _, err := r.ReadU32(); err != nil {
			return fmt.Errorf("read custom instruction error: %w", err)
		}
		return nil
	default:
		// Custom(u32) is the only payload variant: BorshIoError (44) lost its
		// String in SDK v3. All remaining v4.2 variants are unit variants. The
		// highest is BuiltinProgramsMustConsumeComputeUnits (53).
		if tag <= 53 {
			return nil
		}
		return fmt.Errorf("unknown instruction error tag %d", tag)
	}
}
