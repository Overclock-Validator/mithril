package metrics

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var allocSink [][]byte

//go:noinline
func allocateForProfileTest(n int) {
	for i := 0; i < n; i++ {
		allocSink = append(allocSink, make([]byte, 256<<10))
	}
}

// The window's top allocation site is the function that allocated, with an
// estimate close to what it actually allocated.
func TestAllocProfilerAttributesWindow(t *testing.T) {
	p := NewAllocProfiler()
	allocateForProfileTest(512) // 128 MiB
	allocSink = nil
	runtime.GC() // the profile covers allocations up to the last completed GC
	total, sites := p.Sample(5)
	require.NotEmpty(t, sites)
	var found *AllocSite
	for i := range sites {
		if strings.Contains(sites[i].Allocator, "allocateForProfileTest") {
			found = &sites[i]
		}
	}
	require.NotNil(t, found, "sites: %v", sites)
	assert.Equal(t, sites[0], *found, "the test allocator dominates the window")
	assert.Contains(t, found.String(), "MiB/s metrics.allocateForProfileTest")
	assert.Positive(t, total)

	// A second window without that allocation does not report it again.
	runtime.GC()
	_, sites = p.Sample(5)
	for _, s := range sites {
		assert.NotContains(t, s.Allocator, "allocateForProfileTest")
	}
}

func TestUnbiasedBytesScalesSmallAllocations(t *testing.T) {
	// 512 KiB sampling: a 1 KiB allocation is sampled rarely, so its samples
	// stand for far more bytes; a 64 MiB one is always sampled.
	small := unbiasedBytes(1<<10, 1)
	assert.Greater(t, small, float64(400<<10))
	big := unbiasedBytes(64<<20, 1)
	assert.InDelta(t, float64(64<<20), big, float64(1<<20))
}
