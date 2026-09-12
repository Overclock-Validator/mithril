package node

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/stretchr/testify/assert"
)

// Interval lines are differences between monotonic snapshots. Getting this
// wrong is not loud: the reporter keeps writing plausible-looking lines, and a
// collapse in batch width during one interval stays hidden behind a healthy
// cumulative total.
func TestDiffWidthsSubtractsByBucketNotPosition(t *testing.T) {
	// Stats omits empty buckets, so the two snapshots need not share a shape.
	// Matching by position would misattribute counts across bucket boundaries.
	before := []sigverify.WidthBucket{
		{Upper: 1, Batches: 10},
		{Upper: 8, Batches: 4},
	}
	after := []sigverify.WidthBucket{
		{Upper: 1, Batches: 12},
		{Upper: 4, Batches: 7}, // a bucket absent from `before` entirely
		{Upper: 8, Batches: 9},
	}

	assert.ElementsMatch(t, []sigverify.WidthBucket{
		{Upper: 1, Batches: 2},
		{Upper: 4, Batches: 7},
		{Upper: 8, Batches: 5},
	}, diffWidths(before, after))
}

// A bucket that saw no traffic during the interval must be dropped rather than
// reported as zero, or every line carries every bucket forever and the shape of
// the distribution stops being readable at a glance.
func TestDiffWidthsOmitsUnchangedBuckets(t *testing.T) {
	same := []sigverify.WidthBucket{{Upper: 8, Batches: 5}}
	assert.Empty(t, diffWidths(same, same))
}

func TestDiffWidthsHandlesAnEmptyBaseline(t *testing.T) {
	after := []sigverify.WidthBucket{{Upper: 2, Batches: 3}}
	assert.Equal(t, after, diffWidths(nil, after))
}
