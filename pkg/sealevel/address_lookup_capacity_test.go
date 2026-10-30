package sealevel

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func lookupCapacityWire(n int, authority bool) []byte {
	data := make([]byte, AddressLookupTableMetaSize+n*solana.PublicKeyLength)
	binary.LittleEndian.PutUint32(data, AddressLookupTableProgramStateLookupTable)
	binary.LittleEndian.PutUint64(data[4:], ^uint64(0))
	binary.LittleEndian.PutUint64(data[12:], 1234)
	data[20] = 7
	if authority {
		data[21] = 1
		for i := 22; i < 54; i++ {
			data[i] = byte(i)
		}
	}
	for i := AddressLookupTableMetaSize; i < len(data); i++ {
		data[i] = byte(i*17 + i/32)
	}
	return data
}

func TestAddressLookupExactCapacityParityAndOwnership(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 31, 32, 33, 127, 128, 129, 255, 256, 257, 513} {
		for _, authority := range []bool{false, true} {
			data := lookupCapacityWire(n, authority)
			want, err := unmarshalAddressLookupTable(data, false)
			if err != nil {
				t.Fatal(err)
			}
			got, err := unmarshalAddressLookupTable(data, true)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("n=%d authority=%v: %v", n, authority, err)
			}
			if n == 0 && got.Addresses != nil {
				t.Fatal("empty addresses must remain nil")
			}
			// Both the addresses and optional authority must own their bytes.
			clear(data)
			if !reflect.DeepEqual(got, want) {
				t.Fatal("decoded table aliases wire data")
			}
			if n > 0 {
				got.Addresses[0][0] ^= 0xff
				if reflect.DeepEqual(got.Addresses, want.Addresses) {
					t.Fatal("separate decodes alias addresses")
				}
			}
		}
	}
}

func TestAddressLookupExactCapacityMalformedParity(t *testing.T) {
	rng := rand.New(rand.NewSource(251029))
	check := func(data []byte) {
		t.Helper()
		want, oldErr := unmarshalAddressLookupTable(data, false)
		got, newErr := unmarshalAddressLookupTable(data, true)
		if !reflect.DeepEqual(oldErr, newErr) || !reflect.DeepEqual(got, want) {
			t.Fatalf("parity failure: len=%d old=%v new=%v", len(data), oldErr, newErr)
		}
	}
	for _, authority := range []bool{false, true} {
		wire := lookupCapacityWire(3, authority)
		for end := 0; end <= len(wire); end++ {
			check(wire[:end])
		}
	}
	for i := 0; i < 5000; i++ {
		wire := lookupCapacityWire(rng.Intn(258), rng.Intn(2) == 1)
		for j := 0; j < 1+rng.Intn(5); j++ {
			wire[rng.Intn(len(wire))] = byte(rng.Intn(256))
		}
		if rng.Intn(2) == 1 {
			wire = wire[:rng.Intn(len(wire)+1)]
		}
		check(wire)
	}
}

// Decode-only benchmark: no SBF execution, so CU and replay timing intervals do
// not apply. Input creation is excluded, and both arms retain owned addresses.
func BenchmarkAddressLookupExactCapacity(b *testing.B) {
	for _, n := range []int{1, 32, 128, 256} {
		data := lookupCapacityWire(n, true)
		for _, exact := range []bool{false, true} {
			b.Run(fmt.Sprintf("addresses=%d/exact=%t", n, exact), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					table, err := unmarshalAddressLookupTable(data, exact)
					if err != nil || len(table.Addresses) != n {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
