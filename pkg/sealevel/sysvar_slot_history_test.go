package sealevel

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// referenceSlotHistoryMarshal is the encoder MustMarshal replaced: one
// generic write per field.
func referenceSlotHistoryMarshal(t *testing.T, sr *SysvarSlotHistory) []byte {
	t.Helper()
	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if sr.Bits.Bits.BlocksLen != 0 {
		must(enc.WriteByte(1))
		must(enc.WriteUint64(sr.Bits.Bits.BlocksLen, bin.LE))
		for count := 0; count < int(sr.Bits.Bits.BlocksLen); count++ {
			must(enc.WriteUint64(sr.Bits.Bits.Blocks[count], bin.LE))
		}
	} else {
		must(enc.WriteByte(0))
	}
	must(enc.WriteUint64(sr.Bits.Len, bin.LE))
	must(enc.WriteUint64(sr.NextSlot, bin.LE))
	return data.Bytes()
}

// referenceSlotHistoryUnmarshal is the decoder's word-by-word form.
func referenceSlotHistoryUnmarshal(sh *SysvarSlotHistory, decoder *bin.Decoder) error {
	opt, err := decoder.ReadByte()
	if opt != 0 {
		if sh.Bits.Bits.BlocksLen, err = decoder.ReadUint64(bin.LE); err != nil {
			return err
		}
		for count := 0; count < int(sh.Bits.Bits.BlocksLen); count++ {
			block, err := decoder.ReadUint64(bin.LE)
			if err != nil {
				return err
			}
			sh.Bits.Bits.Blocks = append(sh.Bits.Bits.Blocks, block)
		}
	}
	if sh.Bits.Len, err = decoder.ReadUint64(bin.LE); err != nil {
		return err
	}
	sh.NextSlot, err = decoder.ReadUint64(bin.LE)
	return err
}

func randomSlotHistory(rng *rand.Rand, words int) *SysvarSlotHistory {
	sh := &SysvarSlotHistory{NextSlot: rng.Uint64()}
	sh.Bits.Len = rng.Uint64()
	sh.Bits.Bits.BlocksLen = uint64(words)
	for range words {
		sh.Bits.Bits.Blocks = append(sh.Bits.Bits.Blocks, rng.Uint64())
	}
	return sh
}

func TestSlotHistoryMarshalMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, words := range []int{0, 1, 2, 7, 64, 1000, slotHistoryMaxEntries / bitsPerBlock} {
		sh := randomSlotHistory(rng, words)
		want := referenceSlotHistoryMarshal(t, sh)
		if got := sh.MustMarshal(); !bytes.Equal(got, want) {
			t.Fatalf("%d words: encoding differs from the reference (%d against %d bytes)", words, len(got), len(want))
		}
	}
}

func TestSlotHistoryUnmarshalMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for _, words := range []int{0, 1, 2, 7, 64, 1000, slotHistoryMaxEntries / bitsPerBlock} {
		data := randomSlotHistory(rng, words).MustMarshal()
		// every truncation of a short input, and a few of a long one
		cuts := []int{len(data), len(data) - 1, len(data) - 8, len(data) - 16, len(data) - 17, 9, 1, 0}
		if len(data) < 200 {
			cuts = cuts[:0]
			for cut := 0; cut <= len(data); cut++ {
				cuts = append(cuts, cut)
			}
		}
		for _, cut := range cuts {
			if cut < 0 || cut > len(data) {
				continue
			}
			for _, preset := range []int{0, 3} { // decoding appends to words already held
				var got, want SysvarSlotHistory
				for i := range preset {
					got.Bits.Bits.Blocks = append(got.Bits.Bits.Blocks, uint64(i))
					want.Bits.Bits.Blocks = append(want.Bits.Bits.Blocks, uint64(i))
				}
				gotErr := got.UnmarshalWithDecoder(bin.NewBinDecoder(data[:cut]))
				wantErr := referenceSlotHistoryUnmarshal(&want, bin.NewBinDecoder(data[:cut]))
				if (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("%d words cut at %d: error %v, reference %v", words, cut, gotErr, wantErr)
				}
				if wantErr != nil {
					continue // a failed decode is discarded by every caller
				}
				if got.NextSlot != want.NextSlot || got.Bits.Len != want.Bits.Len || got.Bits.Bits.BlocksLen != want.Bits.Bits.BlocksLen {
					t.Fatalf("%d words cut at %d: fields differ from the reference", words, cut)
				}
				if len(got.Bits.Bits.Blocks) != len(want.Bits.Bits.Blocks) {
					t.Fatalf("%d words cut at %d: %d words decoded, reference %d", words, cut, len(got.Bits.Bits.Blocks), len(want.Bits.Bits.Blocks))
				}
				for i := range got.Bits.Bits.Blocks {
					if got.Bits.Bits.Blocks[i] != want.Bits.Bits.Blocks[i] {
						t.Fatalf("%d words cut at %d: word %d differs", words, cut, i)
					}
				}
			}
		}
	}
}

// A word count the data cannot hold is an error. The word-by-word decoder
// converted the count to int, so a count of 2^63 or more read no words and
// went on to decode the next sixteen bytes as the two trailing fields.
func TestSlotHistoryUnmarshalRejectsOversizedLength(t *testing.T) {
	for _, count := range []uint64{3, 1 << 40, 1 << 63, ^uint64(0)} {
		data := make([]byte, 1+8+16)
		data[0] = 1
		binary.LittleEndian.PutUint64(data[1:], count)
		var sh SysvarSlotHistory
		if err := sh.UnmarshalWithDecoder(bin.NewBinDecoder(data)); err == nil {
			t.Fatalf("a count of %d words decoded from %d bytes without error", count, len(data))
		}
		if len(sh.Bits.Bits.Blocks) != 0 {
			t.Fatalf("a count of %d words left %d words decoded", count, len(sh.Bits.Bits.Blocks))
		}
	}
}

func BenchmarkSlotHistoryMarshal(b *testing.B) {
	sh := randomSlotHistory(rand.New(rand.NewSource(3)), slotHistoryMaxEntries/bitsPerBlock)
	b.SetBytes(int64(len(sh.MustMarshal())))
	for b.Loop() {
		sh.MustMarshal()
	}
}

func BenchmarkSlotHistoryUnmarshal(b *testing.B) {
	data := randomSlotHistory(rand.New(rand.NewSource(4)), slotHistoryMaxEntries/bitsPerBlock).MustMarshal()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		var sh SysvarSlotHistory
		if err := sh.UnmarshalWithDecoder(bin.NewBinDecoder(data)); err != nil {
			b.Fatal(err)
		}
	}
}
