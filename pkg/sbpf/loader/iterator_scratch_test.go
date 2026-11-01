package loader

import (
	"bytes"
	"debug/elf"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestIteratorScratchParity(t *testing.T) {
	old := iteratorScratch
	defer func() { iteratorScratch = old }()
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i*19 + 7)
	}
	// Include short records: old decoding zero-filled the unread suffix each time.
	for _, size := range []uint16{1, 8, 15, 16, 24, 55, 56, 63, 64, 80} {
		for _, start := range []uint64{0, 3, 1020, math.MaxInt64, math.MaxUint64} {
			l := &Loader{rd: bytes.NewReader(data), fileSize: uint64(len(data))}
			collect := func(enabled bool) []any {
				iteratorScratch = enabled
				var out []any
				sh := shTableIter{l: l, off: start, count: 3, elemSize: size}
				ph := phTableIter{l: l, off: start, count: 3, elemSize: size}
				dy := dynTableIter{l: l, off: start, count: 3, elemSize: size}
				re := relTableIter{l: l, off: start, count: 3, elemSize: size}
				for range 4 {
					a, ae := sh.getNext()
					b, be := ph.getNext()
					c, ce := dy.getNext()
					d, de := re.getNext()
					out = append(out, a, ae, sh.Item(), b, be, ph.Item(), c, ce, dy.Item(), d, de, re.Item())
				}
				return out
			}
			a, b := collect(false), collect(true)
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("size=%d start=%d: scratch changes iteration", size, start)
			}
		}
	}
	// Item returns a value that remains valid after scratch is overwritten.
	iteratorScratch = true
	l := &Loader{rd: bytes.NewReader(data), fileSize: uint64(len(data))}
	it := relTableIter{l: l, count: 2, elemSize: relLen}
	if !it.Next() {
		t.Fatal(it.Err())
	}
	first := it.Item()
	want := elf.Rel64{Off: first.Off, Info: first.Info}
	if !it.Next() {
		t.Fatal(it.Err())
	}
	if first != want {
		t.Fatal("retained item changed")
	}
}

func BenchmarkELFIteratorScratch(b *testing.B) {
	old := iteratorScratch
	defer func() { iteratorScratch = old }()
	data := make([]byte, 256*relLen)
	l := &Loader{rd: bytes.NewReader(data), fileSize: uint64(len(data))}
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			iteratorScratch = enabled
			it := &relTableIter{l: l, count: 256, elemSize: relLen}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				it.i, it.off = 0, 0
				for it.Next() {
				}
				if it.Err() != nil {
					b.Fatal(it.Err())
				}
			}
		})
	}
}
