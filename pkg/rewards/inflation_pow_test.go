package rewards

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInflationMatchesAgaveLibmAtTestnetEpoch1059(t *testing.T) {
	// Testnet slot 451964256. Rust f64::powf/libm returns the last bit below;
	// Go math.Pow rounded down, producing 392970281850804 instead of ...805.
	year := math.Float64frombits(0x4013cfbf3689bf94)
	require.Equal(t, uint64(0x3fdc9d8deed6b724), math.Float64bits(inflationPow(0.85, year)))
	inflation := Inflation{Initial: 0.15, Terminal: 0.015, Taper: 0.15}
	require.Equal(t, uint64(0x3fb12b5528e73aaf), math.Float64bits(inflation.Validator(year)))
	require.Equal(t, 0.15, inflation.Validator(0))
	require.Equal(t, 0.015, inflation.Validator(100))
}
