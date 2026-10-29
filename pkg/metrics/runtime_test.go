package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGCCPUFractionExcludesUnusedCapacity(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		gc, total, idle, want float64
	}{
		{"fully busy", 2, 8, 0, 0.25},
		{"mostly idle", 1, 8, 6, 0.5},
		{"no GC", 0, 8, 4, 0},
		{"empty", 0, 0, 0, 0},
		{"all idle", 0, 8, 8, 0},
		{"estimate exceeds active", 2, 8, 7, 1},
		{"inconsistent idle estimate", 1, 8, 9, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, nonIdleGCCPUFraction(tc.gc, tc.total, tc.idle))
		})
	}
}
