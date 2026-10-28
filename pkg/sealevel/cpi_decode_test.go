package sealevel

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

var studySinkC []SolAccountInfoC
var studySinkRust []SolAccountInfoRust

func TestCPIDecodeCompleteRecords(t *testing.T) {
	rng := rand.New(rand.NewSource(330))
	for i := 0; i < 10000; i++ {
		c := make([]byte, SolAccountInfoCSize)
		rng.Read(c)
		var a, b SolAccountInfoC
		if err := a.Unmarshal(bytes.NewReader(c)); err != nil {
			t.Fatal(err)
		}
		decodeAccountInfoC(&b, c)
		if a != b {
			t.Fatal("C mismatch")
		}
		r := make([]byte, SolAccountInfoRustSize)
		rng.Read(r)
		var x, y SolAccountInfoRust
		if err := x.Unmarshal(bytes.NewReader(r)); err != nil {
			t.Fatal(err)
		}
		decodeAccountInfoRust(&y, r)
		if x != y {
			t.Fatal("Rust mismatch")
		}
	}
}
func BenchmarkCPIDecode(b *testing.B) {
	for _, n := range []int{8, 32} {
		for _, abi := range []string{"C", "Rust"} {
			for _, mode := range []string{"reader", "direct"} {
				b.Run(fmt.Sprintf("%s/%d/%s", abi, n, mode), func(b *testing.B) {
					size := SolAccountInfoCSize
					if abi == "Rust" {
						size = SolAccountInfoRustSize
					}
					data := make([]byte, n*size)
					rand.New(rand.NewSource(330)).Read(data)
					b.ReportAllocs()
					b.ResetTimer()
					for k := 0; k < b.N; k++ {
						if abi == "C" {
							out := make([]SolAccountInfoC, n)
							if mode == "reader" {
								r := bytes.NewReader(data)
								for i := range out {
									if err := out[i].Unmarshal(r); err != nil {
										b.Fatal(err)
									}
								}
							} else {
								for i := range out {
									decodeAccountInfoC(&out[i], data[i*size:(i+1)*size])
								}
							}
							studySinkC = out
						} else {
							out := make([]SolAccountInfoRust, n)
							if mode == "reader" {
								r := bytes.NewReader(data)
								for i := range out {
									if err := out[i].Unmarshal(r); err != nil {
										b.Fatal(err)
									}
								}
							} else {
								for i := range out {
									decodeAccountInfoRust(&out[i], data[i*size:(i+1)*size])
								}
							}
							studySinkRust = out
						}
					}
				})
			}
		}
	}
}

func TestCPIDecodeRefCells(t *testing.T) {
	rng := rand.New(rand.NewSource(331))
	for i := 0; i < 10000; i++ {
		data := make([]byte, RefCellVecRustSize)
		rng.Read(data)
		var cell RefCellRust
		var vec RefCellVecRust
		if err := cell.Unmarshal(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if err := vec.Unmarshal(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if cell != decodeRefCellRust(data) || vec != decodeRefCellVecRust(data) {
			t.Fatal("RefCell mismatch")
		}
	}
}
