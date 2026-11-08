package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"testing"

	gsolana "github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// CPI signer derivation used solana-go's CreateProgramAddress before it moved to
// CreateProgramAddressBytes. Both must accept and reject the same inputs and
// derive the same addresses.
func TestCreateProgramAddressMatchesSolanaGo(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var ok, offCurveRejects int
	for i := 0; i < 200_000; i++ {
		var program [32]byte
		rng.Read(program[:])
		seeds := make([][]byte, rng.Intn(MaxSeeds+2))
		for j := range seeds {
			n := rng.Intn(MaxSeedLen + 1)
			if rng.Intn(50) == 0 {
				n = MaxSeedLen + 1
			}
			seeds[j] = make([]byte, n)
			rng.Read(seeds[j])
		}
		got, gotErr := CreateProgramAddressBytes(seeds, program[:])
		want, wantErr := gsolana.CreateProgramAddress(seeds, gsolana.PublicKeyFromBytes(program[:]))
		require.Equal(t, wantErr == nil, gotErr == nil, "input %d", i)
		if wantErr == nil {
			require.Equal(t, want[:], got, "input %d", i)
			ok++
		} else if gotErr == ErrOnCurveInvalidSeeds {
			offCurveRejects++
		}
	}
	require.Greater(t, ok, 10_000)
	require.Greater(t, offCurveRejects, 10_000)
}

// The on-curve checks behind the two derivations must agree on every 32-byte
// encoding, including non-canonical ones (y >= p) and set sign bits.
func TestIsOnCurveMatchesSolanaGo(t *testing.T) {
	check := func(b [32]byte) {
		t.Helper()
		require.Equal(t, gsolana.IsOnCurve(b[:]), isOnCurve(b[:]), "%x", b)
		b[31] ^= 0x80
		require.Equal(t, gsolana.IsOnCurve(b[:]), isOnCurve(b[:]), "%x", b)
	}
	for i := 0; i < 200_000; i++ {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(i))
		check(sha256.Sum256(seed[:]))
	}
	// y = p + k (non-canonical) and y = p - 1 - k, for p = 2^255 - 19.
	for k := 0; k < 64; k++ {
		var hi, lo [32]byte
		for j := range hi {
			hi[j], lo[j] = 0xff, 0xff
		}
		hi[31], lo[31] = 0x7f, 0x7f
		if k < 19 {
			hi[0] = byte(0xed + k)
			check(hi)
		}
		lo[0] = byte(0xec - k)
		check(lo)
	}
	// Small y, including y = 0, 1 (x = 0) and -1.
	for y := 0; y < 256; y++ {
		var b [32]byte
		b[0] = byte(y)
		check(b)
	}
}
