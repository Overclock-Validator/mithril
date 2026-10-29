package rewards

/*
#cgo LDFLAGS: -lm
#include <math.h>
*/
import "C"

// inflationPow uses the platform libm, like Rust's f64::powf in
// solana-inflation 3.2.0, Inflation::total, and Firedancer d2f7c311
// src/flamenco/rewards/fd_rewards.c:218. Go's math.Pow can differ by one
// ULP, enough to change an integer reward budget and therefore the bank hash.
// At testnet epoch 1059 (slot 451964256), pow(0.85, 4.9528778573584482)
// must be 0x3fdc9d8deed6b724; math.Pow returned 0x3fdc9d8deed6b723.
// This is called only when calculating epoch inflation, outside the hot path.
func inflationPow(base, exponent float64) float64 {
	return float64(C.pow(C.double(base), C.double(exponent)))
}
