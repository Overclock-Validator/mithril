package sigverify

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The counters are process-global and monotonic, so assertions are written as
// deltas around the work rather than against absolute values. Another test in
// this package verifying signatures concurrently must not be able to break
// these.
func TestBatchWidthIsRecordedByVerify(t *testing.T) {
	before := Stats()

	widths := []int{1, 1, 3, 8, 8, 8, 40}
	for _, width := range widths {
		var batch Batch
		for i := 0; i < width; i++ {
			signed := makeSigned(t, i, true)
			batch.Add(&signed.pub, signed.msg, signed.sig)
		}
		require.True(t, batch.Verify())
	}

	after := Stats()

	var expectedSignatures int
	for _, w := range widths {
		expectedSignatures += w
	}

	assert.Equal(t, uint64(len(widths)), after.Batches-before.Batches,
		"every Verify must be counted, whatever its width")
	assert.Equal(t, uint64(expectedSignatures), after.Signatures-before.Signatures)
}

// An empty batch is a real event -- a drain that found nothing -- and must be
// counted without being charged signatures, or the mean width is wrong.
func TestEmptyBatchIsCountedSeparately(t *testing.T) {
	before := Stats()

	var batch Batch
	require.True(t, batch.Verify(), "an empty batch trivially passes")

	after := Stats()
	assert.Equal(t, uint64(1), after.Batches-before.Batches)
	assert.Equal(t, uint64(1), after.EmptyBatches-before.EmptyBatches)
	assert.Equal(t, uint64(0), after.Signatures-before.Signatures,
		"an empty batch must not contribute signatures")
}

// MeanWidth exists to be read at a glance, so its denominator has to exclude
// empty batches. Counting them would drag the mean toward zero and make a
// healthy drain look starved.
func TestMeanWidthExcludesEmptyBatches(t *testing.T) {
	snapshot := Snapshot{Batches: 10, EmptyBatches: 6, Signatures: 32}
	assert.InDelta(t, 8.0, snapshot.MeanWidth(), 0.001,
		"four non-empty batches carrying 32 signatures is a mean of 8")

	empty := Snapshot{Batches: 3, EmptyBatches: 3}
	assert.Zero(t, empty.MeanWidth(), "no non-empty batches must not divide by zero")
}

// The rendered line is what lands in the per-run log, so it is worth pinning
// that the fields an operator would grep for are actually present.
func TestSnapshotRendersTheFieldsWorthGrepping(t *testing.T) {
	line := Snapshot{
		Backend:    "r51",
		Batches:    9,
		Signatures: 40,
		Widths:     []WidthBucket{{Upper: 8, Batches: 5}, {Upper: 0, Batches: 1}},
	}.String()

	for _, want := range []string{"backend=r51", "batches=9", "signatures=40", "mean_width=", "full_group_share=", "widths=["} {
		assert.Contains(t, line, want)
	}
	assert.Contains(t, line, ">1024:1", "the overflow bucket must be legible")
	assert.NotContains(t, line, "empty=", "a zero empty count should not add noise")
}

func TestSnapshotReportsEmptyBatchesWhenPresent(t *testing.T) {
	line := Snapshot{Backend: "generic", Batches: 4, EmptyBatches: 2, Signatures: 8}.String()
	assert.Contains(t, line, "empty=2")
}

// Stats must be safe to call from a reporter goroutine while verification runs.
func TestStatsIsSafeUnderConcurrentVerification(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			var batch Batch
			signed := makeSigned(t, i, true)
			batch.Add(&signed.pub, signed.msg, signed.sig)
			batch.Verify()
		}
	}()

	for i := 0; i < 200; i++ {
		if s := Stats(); strings.TrimSpace(s.String()) == "" {
			t.Fatal("snapshot rendered empty")
		}
	}
	<-done
}
