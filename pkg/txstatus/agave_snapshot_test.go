package txstatus

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/wincode"
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
	accountDataLimit := append(encU32(1), encU32(29)...)
	duplicateInstruction := append(append(encU32(1), encU32(30)...), byte(7))
	insufficientFundsForRent := append(append(encU32(1), encU32(31)...), byte(8))
	resanitizationNeeded := append(encU32(1), encU32(34)...)
	programExecutionRestricted := append(append(encU32(1), encU32(35)...), byte(9))
	customInstruction := append(append(append(append(encU32(1), encU32(8)...), byte(2)), encU32(25)...), encU32(0xdecafbad)...)
	// BorshIoError (44) is a unit variant since SDK v3: no trailing string.
	borshInstruction := append(append(append(encU32(1), encU32(8)...), byte(3)), encU32(44)...)
	unitError := append(encU32(1), encU32(6)...)

	data := encU64(2)
	data = append(data, encU64(42)...)
	data = append(data, 1)
	data = append(data, encU64(2)...)
	data = appendStatus(data, hashA, 3, []encodedKey{
		{key(1), ok},
		{key(2), accountDataLimit},
		{key(3), duplicateInstruction},
		{key(4), insufficientFundsForRent},
		{key(5), resanitizationNeeded},
		{key(6), programExecutionRestricted},
		{key(7), customInstruction},
		{key(8), borshInstruction},
	})
	data = appendStatus(data, hashB, MaxCachedKeyIndex, []encodedKey{{key(9), unitError}})
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
	if deltas[0].Statuses[0].RecentBlockhash != hashA || deltas[0].Statuses[0].KeyIndex != 3 || len(deltas[0].Statuses[0].Keys) != 8 {
		t.Fatalf("unexpected first status: %+v", deltas[0].Statuses[0])
	}
	if got := deltas[0].Statuses[0].Keys[7]; got != key(8) {
		t.Fatalf("decoded key %x, want %x", got, key(8))
	}
	if deltas[1].Slot != 45 || deltas[1].IsRoot || len(deltas[1].Statuses) != 0 {
		t.Fatalf("unexpected second delta: %+v", deltas[1])
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

func encU32(v uint32) []byte {
	return binary.LittleEndian.AppendUint32(nil, v)
}

func encU64(v uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, v)
}

// The payload arity per TransactionError tag is the whole decode contract: a
// tag treated as a unit variant when it carries a u8 leaves that byte in the
// stream and desyncs everything after it. Drive the decoder per tag rather
// than restating the case list, so a shifted discriminant fails here.
func TestSkipTransactionErrorPayloadArity(t *testing.T) {
	const sentinel = 0x5A
	for _, tc := range []struct {
		name    string
		tag     uint32
		payload int // bytes the variant carries after the tag
	}{
		{"WouldExceedAccountDataTotalLimit", 29, 0},
		{"DuplicateInstruction", 30, 1},
		{"InsufficientFundsForRent", 31, 1},
		{"ResanitizationNeeded", 34, 0},
		{"ProgramExecutionTemporarilyRestricted", 35, 1},
		{"CommitCancelled", 38, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf []byte
			buf = binary.LittleEndian.AppendUint32(buf, tc.tag)
			buf = append(buf, make([]byte, tc.payload)...)
			buf = append(buf, sentinel)

			r := wincode.NewReader(buf)
			if err := skipTransactionError(r); err != nil {
				t.Fatalf("tag %d: %v", tc.tag, err)
			}
			// Exactly the payload must be consumed: the sentinel is what the
			// next field would read, and it has to still be there.
			next, err := r.ReadU8()
			if err != nil {
				t.Fatalf("tag %d: decoder overran the variant: %v", tc.tag, err)
			}
			if next != sentinel {
				t.Fatalf("tag %d: consumed the wrong number of payload bytes (next byte %#x, want %#x)",
					tc.tag, next, sentinel)
			}
		})
	}
}

// Beyond the known variants the decoder must refuse rather than guess.
func TestSkipTransactionErrorRejectsUnknownTag(t *testing.T) {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, 39)
	if err := skipTransactionError(wincode.NewReader(buf)); err == nil {
		t.Fatal("tag 39 is past CommitCancelled and must be rejected")
	}
}
