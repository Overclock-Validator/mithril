package metrics

import (
	"math"
	"strings"
	"testing"
)

func TestBlockIORecord(t *testing.T) {
	for _, input := range []struct {
		text  string
		count int
	}{
		{"1 2 3 4 5 6 7 8 9 10 11\n", 11},
		{"1 2 3 4 5 6 7 8 9 10 11 12 13 14 15\n", 15},
		{"1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 18446744073709551615\n", 17},
	} {
		got, count, err := parseBlockIO([]byte(input.text))
		if err != nil || count != input.count || got.InFlight != 9 || got.ReadSectors != 3 || got.WriteSectors != 7 ||
			count == 17 && got.FlushMilliseconds != math.MaxUint64 {
			t.Fatalf("record %q: %+v, %d, %v", input.text, got, count, err)
		}
	}
	for _, text := range []string{"", "1 2\n", "1 2 3 4 5 6 7 8 9 10 11", "1 2 3 4 5 6 7 8 9 10 11\n\n",
		"+1 2 3 4 5 6 7 8 9 10 11\n", "01 2 3 4 5 6 7 8 9 10 11\n", "-1 2 3 4 5 6 7 8 9 10 11\n",
		"18446744073709551616 2 3 4 5 6 7 8 9 10 11\n", strings.Repeat("0 ", 2048) + "\n"} {
		if got, count, err := parseBlockIO([]byte(text)); err == nil || count != 0 || got != (BlockIOCounters{}) {
			t.Fatalf("invalid record accepted: %q", text)
		}
	}
	if got, ok := NewBlockIOSampler(false).Sample("/must-not-be-read"); ok || got != (BlockIOObservation{}) {
		t.Fatal("disabled sampler performed an observation")
	}
}
