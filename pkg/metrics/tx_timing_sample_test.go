package metrics

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

func TestTxTimingSampledShiftZeroRecordsEverything(t *testing.T) {
	previous := SetTxTimingSampleShift(0)
	defer SetTxTimingSampleShift(previous)

	for i := 0; i < 64; i++ {
		var sig [64]byte
		sig[0] = byte(i)
		if !TxTimingSampled(sig[:]) {
			t.Fatalf("shift 0 must sample every signature, rejected %d", i)
		}
	}
	if !TxTimingSampled(nil) {
		t.Fatal("shift 0 must sample a missing signature")
	}
	var timing Timing
	timing.AddSampledTiming(5*time.Nanosecond, TxTimingSampleShift())
	if timing.Count != 1 || timing.SumNanoseconds != 5 {
		t.Fatalf("shift 0 must not scale: got count %d sum %d", timing.Count, timing.SumNanoseconds)
	}
}

func TestTxTimingSampledIsDeterministicAndUniform(t *testing.T) {
	previous := SetTxTimingSampleShift(3)
	defer SetTxTimingSampleShift(previous)

	rng := rand.New(rand.NewSource(1))
	const n = 200000
	sampled := 0
	for i := 0; i < n; i++ {
		var sig [64]byte
		rng.Read(sig[:])
		first := TxTimingSampled(sig[:])
		if first != TxTimingSampled(sig[:]) {
			t.Fatal("sampling decision must be a pure function of the signature")
		}
		if first != (binary.LittleEndian.Uint64(sig[:])&7 == 0) {
			t.Fatal("sampling must key on the low bits of the first signature word")
		}
		if first {
			sampled++
		}
	}
	// 1/8 of 200k is 25k; allow +-5% (about 7 standard deviations).
	if sampled < 23750 || sampled > 26250 {
		t.Fatalf("sampled %d of %d signatures, expected about %d", sampled, n, n/8)
	}
}

func TestTxTimingSampledUnsignedFallsBackToRoundRobin(t *testing.T) {
	previous := SetTxTimingSampleShift(2)
	defer SetTxTimingSampleShift(previous)

	var zero [64]byte
	sampled := 0
	for i := 0; i < 400; i++ {
		if TxTimingSampled(zero[:]) {
			sampled++
		}
	}
	if sampled != 100 {
		t.Fatalf("unsigned transactions must be sampled 1 in 4 by the counter, got %d of 400", sampled)
	}
	sampled = 0
	for i := 0; i < 400; i++ {
		if TxTimingSampled(nil) {
			sampled++
		}
	}
	if sampled != 100 {
		t.Fatalf("missing signatures must be sampled 1 in 4 by the counter, got %d of 400", sampled)
	}
}

func TestAddSampledTimingScalesToTheUnsampledTotal(t *testing.T) {
	previous := SetTxTimingSampleShift(3)
	defer SetTxTimingSampleShift(previous)

	var timing Timing
	timing.AddSampledTiming(100*time.Nanosecond, TxTimingSampleShift())
	if timing.Count != 8 || timing.SumNanoseconds != 800 {
		t.Fatalf("shift 3 must scale by 8: got count %d sum %d", timing.Count, timing.SumNanoseconds)
	}
	timing.AddSampledTimingSince(time.Time{}, TxTimingSampleShift())
	if timing.Count != 8 || timing.SumNanoseconds != 800 {
		t.Fatal("a zero start must record nothing")
	}
	timing.AddSampledTimingSince(time.Now().Add(-time.Microsecond), TxTimingSampleShift())
	if timing.Count != 16 || timing.SumNanoseconds < 800+8*1000 {
		t.Fatalf("elapsed sample must be scaled: got count %d sum %d", timing.Count, timing.SumNanoseconds)
	}
}

func TestSetTxTimingSampleShiftClamps(t *testing.T) {
	previous := SetTxTimingSampleShift(0)
	defer SetTxTimingSampleShift(previous)

	if got := SetTxTimingSampleShift(40); got != 0 {
		t.Fatalf("previous shift = %d, want 0", got)
	}
	if got := TxTimingSampleShift(); got != maxTxTimingSampleShift {
		t.Fatalf("shift = %d, want clamp to %d", got, maxTxTimingSampleShift)
	}
	if DefaultTxTimingSampleShift > maxTxTimingSampleShift {
		t.Fatal("default shift exceeds the clamp")
	}
}

func TestCapturedTimingScaleDoesNotChange(t *testing.T) {
	previous := SetTxTimingSampleShift(3)
	defer SetTxTimingSampleShift(previous)
	sig := make([]byte, 64)
	sig[0] = 8
	sample := CaptureTxTiming(sig)
	SetTxTimingSampleShift(0)
	var got Timing
	if !sample.Valid || !sample.Sampled || sample.Shift != 3 {
		t.Fatal(sample)
	}
	got.AddSampledTiming(5*time.Nanosecond, sample.Shift)
	if got.Count != 8 || got.SumNanoseconds != 40 {
		t.Fatal(got)
	}
}

func TestSignatureWithZeroPrefixRemainsDeterministic(t *testing.T) {
	old := SetTxTimingSampleShift(3)
	defer SetTxTimingSampleShift(old)
	sig := make([]byte, 64)
	sig[63] = 1
	for i := 0; i < 20; i++ {
		if !CaptureTxTiming(sig).Sampled {
			t.Fatal("nonzero signature used unsigned fallback")
		}
	}
}
