package rpcserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/mr-tron/base58"
)

func TestHistoryBase58EncodeVectors(t *testing.T) {
	for _, test := range []struct {
		data []byte
		want string
	}{
		{nil, ""}, {[]byte{}, ""}, {[]byte{0}, "1"}, {[]byte{0, 0}, "11"},
		{[]byte{1}, "2"}, {[]byte{0, 0, 1}, "112"},
		{[]byte("a"), "2g"}, {[]byte("bbb"), "a3gV"},
		{[]byte("Hello World"), "JxF12TrwUP45BMd"},
	} {
		if got := historyBase58Encode(test.data); got != test.want {
			t.Errorf("encode %x: got %q want %q", test.data, got, test.want)
		}
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	for digit := range len(alphabet) {
		if got := historyBase58Encode([]byte{byte(digit)}); got != alphabet[digit:digit+1] {
			t.Errorf("digit %d: got %q want %q", digit, got, alphabet[digit:digit+1])
		}
	}
}

func TestHistoryBase58EncodeMatchesInstalled(t *testing.T) {
	check := func(data []byte) {
		t.Helper()
		if got, want := historyBase58Encode(data), base58.Encode(data); got != want {
			t.Fatalf("base58 mismatch for %d bytes", len(data))
		}
	}
	for _, size := range []int{0, 1, 2, 31, 32, 33, 63, 64, 65, 255, 1024, 10 * 1024} {
		check(make([]byte, size))
		check(bytes.Repeat([]byte{255}, size))
		check(historyBase58Fixture(size, 17))
		data := historyBase58Fixture(size, 42)
		clear(data[:size/2])
		check(data)
	}
	for value := range 256 {
		check([]byte{byte(value)})
		check([]byte{0, 0, byte(value)})
	}
	random := rand.New(rand.NewSource(147))
	for range 128 {
		data := make([]byte, random.Intn(513))
		if _, err := random.Read(data); err != nil {
			t.Fatal(err)
		}
		check(data)
	}
}

func TestHistoryBase58EncodeConcurrent(t *testing.T) {
	var workers sync.WaitGroup
	for seed := range 8 {
		workers.Go(func() {
			data := historyBase58Fixture(256, seed)
			want := base58.Encode(data)
			for range 16 {
				if got := historyBase58Encode(data); got != want {
					t.Errorf("concurrent base58 mismatch for seed %d", seed)
					return
				}
			}
		})
	}
	workers.Wait()
}

func historyBase58Fixture(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i+seed)%251 + 1)
	}
	return data
}

var historyBase58BenchmarkResult string

// This is an encoder-only fixture, not a full RPC or captured-record benchmark.
func BenchmarkHistoryBase58CaptureEnvelope(b *testing.B) {
	for _, fixture := range []struct{ entries, bytes int }{{1, 1024}, {1, 10 * 1024}, {64, 10 * 1024}} {
		b.Run(fmt.Sprintf("entries=%d/bytes=%d", fixture.entries, fixture.bytes), func(b *testing.B) {
			data := make([][]byte, fixture.entries)
			for i := range data {
				data[i] = historyBase58Fixture(fixture.bytes, i)
			}
			b.ReportAllocs()
			b.SetBytes(int64(fixture.entries * fixture.bytes))
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for _, input := range data {
					historyBase58BenchmarkResult = historyBase58Encode(input)
				}
			}
		})
	}
}
