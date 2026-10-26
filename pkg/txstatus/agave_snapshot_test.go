package txstatus

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestDecodeAgaveSnapshot(t *testing.T) {
	var hashA, hashB [32]byte
	for i := range hashA {
		hashA[i] = byte(i + 1)
		hashB[i] = byte(200 + i)
	}
	key := func(seed byte) [CachedKeySize]byte {
		var out [CachedKeySize]byte
		for i := range out {
			out[i] = seed + byte(i)
		}
		return out
	}

	ok := encU32(0)
	duplicateInstruction := append(append(encU32(1), encU32(30)...), byte(7))
	customInstruction := append(append(append(append(encU32(1), encU32(8)...), byte(2)), encU32(25)...), encU32(0xdecafbad)...)
	borshInstruction := append(append(append(append(encU32(1), encU32(8)...), byte(3)), encU32(44)...), encString("io")...)
	unitError := append(encU32(1), encU32(6)...)

	data := encU64(2)
	data = append(data, encU64(42)...)
	data = append(data, 1)
	data = append(data, encU64(2)...)
	data = appendStatus(data, hashA, 3, []encodedKey{
		{key(1), ok},
		{key(2), duplicateInstruction},
		{key(3), customInstruction},
		{key(4), borshInstruction},
	})
	data = appendStatus(data, hashB, MaxCachedKeyIndex, []encodedKey{{key(5), unitError}})
	data = append(data, encU64(45)...)
	data = append(data, 0)
	data = append(data, encU64(0)...)

	deltas, err := DecodeAgaveSnapshot(data)
	if err != nil {
		t.Fatalf("DecodeAgaveSnapshot: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("decoded %d deltas, want 2", len(deltas))
	}
	if deltas[0].Slot != 42 || !deltas[0].IsRoot || len(deltas[0].Statuses) != 2 {
		t.Fatalf("unexpected first delta: %+v", deltas[0])
	}
	if deltas[0].Statuses[0].RecentBlockhash != hashA || deltas[0].Statuses[0].KeyIndex != 3 || len(deltas[0].Statuses[0].Keys) != 4 {
		t.Fatalf("unexpected first status: %+v", deltas[0].Statuses[0])
	}
	if got := deltas[0].Statuses[0].Keys[3]; got != key(4) {
		t.Fatalf("decoded key %x, want %x", got, key(4))
	}
	if deltas[1].Slot != 45 || deltas[1].IsRoot || len(deltas[1].Statuses) != 0 {
		t.Fatalf("unexpected second delta: %+v", deltas[1])
	}
}

// Wire tags come from SerdeTransactionError in Agave v4.2, commit
// 961d762037f1e92a022e8f369cf7ca79ca703b88,
// runtime/src/serde_snapshot/status_cache.rs. In particular, tag 29 is the
// unit variant WouldExceedAccountDataTotalLimit, not DuplicateInstruction.
func TestDecodeAgaveSnapshotTransactionErrorVariants(t *testing.T) {
	for tag := uint32(0); tag <= 38; tag++ {
		t.Run(fmt.Sprintf("tag_%d", tag), func(t *testing.T) {
			result := append(encU32(1), encU32(tag)...)
			switch tag {
			case 8: // InstructionError(7, Custom(0xdecafbad))
				result = append(result, 7)
				result = append(result, encU32(25)...)
				result = append(result, encU32(0xdecafbad)...)
			case 30, 31, 35: // DuplicateInstruction, InsufficientFundsForRent, ProgramExecutionTemporarilyRestricted
				result = append(result, 0xa5)
			}
			firstKey := [CachedKeySize]byte{0x11}
			nextKey := [CachedKeySize]byte{0xee, 0xdd, 0xcc, 0xbb}
			data := append(encU64(2), encU64(42)...)
			data = append(data, 1)
			data = append(data, encU64(1)...)
			data = appendStatus(data, [32]byte{9}, 3, []encodedKey{
				{firstKey, result},
				{nextKey, encU32(0)},
			})
			data = append(data, encU64(43)...)
			data = append(data, 0)
			data = append(data, encU64(0)...)
			deltas, err := DecodeAgaveSnapshot(data)
			if err != nil {
				t.Fatalf("decode Agave transaction error %d: %v", tag, err)
			}
			if len(deltas) != 2 || deltas[1].Slot != 43 || deltas[1].IsRoot || len(deltas[0].Statuses) != 1 {
				t.Fatalf("transaction error %d corrupted subsequent slot: %+v", tag, deltas)
			}
			keys := deltas[0].Statuses[0].Keys
			if len(keys) != 2 || keys[0] != firstKey || keys[1] != nextKey {
				t.Fatalf("transaction error %d corrupted cached keys: %x", tag, keys)
			}
		})
	}
}

func TestDecodeAgaveSnapshotRejectsMissingTransactionErrorPayload(t *testing.T) {
	for _, tag := range []uint32{30, 31, 35} {
		t.Run(fmt.Sprintf("tag_%d", tag), func(t *testing.T) {
			data := append(encU64(1), encU64(42)...)
			data = append(data, 1)
			data = append(data, encU64(1)...)
			data = appendStatus(data, [32]byte{}, 0, []encodedKey{
				{[CachedKeySize]byte{}, append(encU32(1), encU32(tag)...)},
			})
			if _, err := DecodeAgaveSnapshot(data); err == nil {
				t.Fatal("accepted a transaction error without its account/instruction index")
			}
		})
	}
}

func TestDecodeAgaveSnapshotRejectsMalformed(t *testing.T) {
	var hash [32]byte
	var key [CachedKeySize]byte
	validStatus := func(keyIndex uint64, result []byte) []byte {
		return appendStatus(nil, hash, keyIndex, []encodedKey{{key, result}})
	}
	validSlot := func(root byte, status []byte) []byte {
		data := append(encU64(1), encU64(9)...)
		data = append(data, root)
		data = append(data, encU64(1)...)
		return append(data, status...)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"invalid root bool", validSlot(2, validStatus(0, encU32(0)))},
		{"invalid key index", validSlot(1, validStatus(MaxCachedKeyIndex+1, encU32(0)))},
		{"unknown result", validSlot(1, validStatus(0, encU32(2)))},
		{"unknown transaction error", validSlot(1, validStatus(0, append(encU32(1), encU32(39)...)))},
		{"unknown instruction error", validSlot(1, validStatus(0, append(append(append(encU32(1), encU32(8)...), 0), encU32(54)...)))},
		{"truncated", validSlot(1, validStatus(0, []byte{0, 0}))},
		{"trailing bytes", append(validSlot(1, validStatus(0, encU32(0))), 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeAgaveSnapshot(tt.data); err == nil {
				t.Fatal("expected decode error")
			}
		})
	}
}

// TestDecodeAgaveSnapshotExternalFixture is skipped in normal test runs. It
// lets CI or an operator validate the decoder against a status_cache extracted
// from a real Agave archive without checking a multi-megabyte fixture into git.
func TestDecodeAgaveSnapshotExternalFixture(t *testing.T) {
	path := os.Getenv("MITHRIL_AGAVE_STATUS_CACHE_FIXTURE")
	if path == "" {
		t.Skip("MITHRIL_AGAVE_STATUS_CACHE_FIXTURE is not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	deltas, err := DecodeAgaveSnapshot(data)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(deltas) == 0 {
		t.Fatal("fixture contained no slot deltas")
	}
	keyCount := 0
	for _, delta := range deltas {
		for _, status := range delta.Statuses {
			keyCount += len(status.Keys)
		}
	}
	if keyCount == 0 {
		t.Fatal("fixture contained no cached keys")
	}
	t.Logf("decoded %d slot deltas and %d cached keys", len(deltas), keyCount)
}

type encodedKey struct {
	key    [CachedKeySize]byte
	result []byte
}

func appendStatus(dst []byte, hash [32]byte, keyIndex uint64, keys []encodedKey) []byte {
	dst = append(dst, hash[:]...)
	dst = append(dst, encU64(keyIndex)...)
	dst = append(dst, encU64(uint64(len(keys)))...)
	for _, key := range keys {
		dst = append(dst, key.key[:]...)
		dst = append(dst, key.result...)
	}
	return dst
}

func encString(s string) []byte {
	return append(encU64(uint64(len(s))), []byte(s)...)
}

func encU32(v uint32) []byte {
	return binary.LittleEndian.AppendUint32(nil, v)
}

func encU64(v uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, v)
}
