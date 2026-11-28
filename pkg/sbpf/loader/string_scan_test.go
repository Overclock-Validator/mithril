package loader

import (
	"bytes"
	"debug/elf"
	"math"
	"math/rand"
	"testing"
)

func compareStringReaders(t *testing.T, data []byte, section elf.Section64, offset uint32, limit uint16) {
	t.Helper()
	old := directELFStrings
	directELFStrings = true
	t.Cleanup(func() { directELFStrings = old })
	fast, err := NewLoaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := NewLoaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	slow.input = nil // Exercise the original ReaderAt implementation.
	got, gotErr := fast.getString(&section, offset, limit)
	want, wantErr := slow.getString(&section, offset, limit)
	if got != want || (gotErr == nil) != (wantErr == nil) || (gotErr != nil && gotErr.Error() != wantErr.Error()) {
		t.Fatalf("section=%+v offset=%d limit=%d: fast=(%q,%v), reader=(%q,%v)", section, offset, limit, got, gotErr, want, wantErr)
	}
}

func TestStringScanMatchesReader(t *testing.T) {
	data := []byte("prefix\x00first\x00second\x00tail")
	for _, start := range []uint64{0, 7, uint64(len(data)), uint64(len(data) + 1), math.MaxUint64} {
		for _, size := range []uint64{0, 1, 6, uint64(len(data)), math.MaxUint64} {
			for _, off := range []uint32{0, 1, 6, uint32(len(data)), math.MaxUint32} {
				for _, limit := range []uint16{0, 1, 5, 6, 32, math.MaxUint16} {
					compareStringReaders(t, data, elf.Section64{Type: uint32(elf.SHT_STRTAB), Off: start, Size: size}, off, limit)
				}
			}
		}
	}
	compareStringReaders(t, data, elf.Section64{Type: uint32(elf.SHT_PROGBITS), Size: 20}, 0, 16)
	rng := rand.New(rand.NewSource(330))
	for i := 0; i < 10000; i++ {
		data := make([]byte, rng.Intn(512))
		rng.Read(data)
		compareStringReaders(t, data, elf.Section64{Type: uint32(elf.SHT_STRTAB), Off: uint64(rng.Intn(600)), Size: uint64(rng.Intn(600))}, uint32(rng.Intn(600)), uint16(rng.Intn(600)))
	}
}

func TestStringScanOwnsResult(t *testing.T) {
	old := directELFStrings
	directELFStrings = true
	t.Cleanup(func() { directELFStrings = old })
	data := []byte("symbol\x00")
	l, _ := NewLoaderFromBytes(data)
	got, err := l.getString(&elf.Section64{Type: uint32(elf.SHT_STRTAB), Size: uint64(len(data))}, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	copy(data, "change")
	if got != "symbol" {
		t.Fatalf("returned string aliases input: %q", got)
	}
}

func FuzzStringScanMatchesReader(f *testing.F) {
	f.Add([]byte("abc\x00def"), uint64(0), uint64(7), uint32(0), uint16(7))
	f.Add([]byte{}, uint64(0), uint64(1), uint32(0), uint16(0))
	f.Fuzz(func(t *testing.T, data []byte, start, size uint64, off uint32, limit uint16) {
		if len(data) > 65536 {
			t.Skip()
		}
		compareStringReaders(t, data, elf.Section64{Type: uint32(elf.SHT_STRTAB), Off: start, Size: size}, off, limit)
	})
}

func BenchmarkLoaderStringScan(b *testing.B) {
	old := directELFStrings
	directELFStrings = true
	defer func() { directELFStrings = old }()
	data := bytes.Repeat([]byte("sol_get_clock_sysvar\x00"), 64)
	for _, direct := range []bool{false, true} {
		name := "reader"
		if direct {
			name = "slice"
		}
		b.Run(name, func(b *testing.B) {
			l, _ := NewLoaderFromBytes(data)
			if !direct {
				l.input = nil
			}
			section := elf.Section64{Type: uint32(elf.SHT_STRTAB), Size: uint64(len(data))}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := l.getString(&section, 0, 256)
				if err != nil || s != "sol_get_clock_sysvar" {
					b.Fatal(s, err)
				}
			}
		})
	}
}
