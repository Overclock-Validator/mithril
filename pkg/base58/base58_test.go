package base58

import (
	"encoding/hex"
	"testing"
)

var testVector32 = []struct {
	hex string
	b58 string
}{
	{
		hex: "0000000000000000000000000000000000000000000000000000000000000000",
		b58: "11111111111111111111111111111111",
	},
	{
		hex: "0000000000000000000000000000000000000000000000000000000000000001",
		b58: "11111111111111111111111111111112",
	},
	{
		hex: "0000000000000000000000000000000000000000000000000000000000000101",
		b58: "1111111111111111111111111111115S",
	},
	{
		hex: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		b58: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG",
	},
	{
		hex: "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
		b58: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFF",
	},
}

var testVector64 = []struct {
	hex string
	b58 string
}{
	{
		hex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		b58: "1111111111111111111111111111111111111111111111111111111111111111",
	},
	{
		hex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
		b58: "1111111111111111111111111111111111111111111111111111111111111112",
	},
	{
		hex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000101",
		b58: "111111111111111111111111111111111111111111111111111111111111115S",
	},
	{
		hex: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		b58: "67rpwLCuS5DGA8KGZXKsVQ7dnPb9goRLoKfgGbLfQg9WoLUgNY77E2jT11fem3coV9nAkguBACzrU1iyZM4B8roQ",
	},
	{
		hex: "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
		b58: "67rpwLCuS5DGA8KGZXKsVQ7dnPb9goRLoKfgGbLfQg9WoLUgNY77E2jT11fem3coV9nAkguBACzrU1iyZM4B8roP",
	},
}

func TestEncode32(t *testing.T) {
	for _, test := range testVector32 {
		var in [32]byte
		hex.Decode(in[:], []byte(test.hex))

		var out [44]byte
		outLen := Encode32(&out, in)

		outStr := string(out[:outLen])
		if outStr != test.b58 {
			t.Errorf("Encode32(%s) = %s, want %s", test.hex, outStr, test.b58)
		}
	}
}

func TestDecode32(t *testing.T) {
	for _, test := range testVector32 {
		var out [32]byte
		if !Decode32(&out, []byte(test.b58)) {
			t.Errorf("Decode32(%s) failed", test.b58)
			continue
		}

		outStr := hex.EncodeToString(out[:])
		if outStr != test.hex {
			t.Errorf("Decode32(%s) = %s, want %s", test.b58, outStr, test.hex)
		}
	}
}

func TestEncode64(t *testing.T) {
	for _, test := range testVector64 {
		var in [64]byte
		hex.Decode(in[:], []byte(test.hex))

		var out [88]byte
		outLen := Encode64(&out, in)
		outStr := string(out[:outLen])
		if outStr != test.b58 {
			t.Errorf("Encode64(%s) = %s, want %s", test.hex, outStr, test.b58)
		}
	}
}

func TestDecode64(t *testing.T) {
	for _, test := range testVector64 {
		var out [64]byte
		if !Decode64(&out, []byte(test.b58)) {
			t.Errorf("Decode64(%s) failed", test.b58)
			continue
		}

		outStr := hex.EncodeToString(out[:])
		if outStr != test.hex {
			t.Errorf("Decode64(%s) = %s, want %s", test.b58, outStr, test.hex)
		}
	}
}

func BenchmarkEncode32(b *testing.B) {
	test := testVector32[0]
	var in [32]byte
	var out [44]byte

	hex.Decode(in[:], []byte(test.hex))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Encode32(&out, in)
	}
}

func BenchmarkDecode32(b *testing.B) {
	test := testVector32[0]
	var out [32]byte

	b.ReportAllocs()

	for b.Loop() {
		if !Decode32(&out, []byte(test.b58)) {
			b.Errorf("Decode32(%s) failed", test.b58)
			continue
		}
	}
}

func BenchmarkEncode64(b *testing.B) {
	test := testVector64[0]
	var in [64]byte
	var out [88]byte

	hex.Decode(in[:], []byte(test.hex))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Encode64(&out, in)
	}
}

func BenchmarkDecode64(b *testing.B) {
	test := testVector64[0]
	var out [64]byte

	b.ReportAllocs()

	for b.Loop() {
		if !Decode64(&out, []byte(test.b58)) {
			b.Errorf("Decode64(%s) failed", test.b58)
			continue
		}
	}
}
