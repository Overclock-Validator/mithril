package sigverify

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Batch width is the single number that decides what verification costs. The
// library verifies eight signatures per AVX-512 group, so a stream of width-1
// batches pays roughly 3.7x per signature what the same work costs at width 8.
// A total signature count cannot show that: a million signatures arriving one
// at a time and the same million arriving in groups of eight produce identical
// totals and wildly different cost. Only the distribution distinguishes them,
// which is why this is a histogram rather than a counter.
//
// Buckets are per-width up to 8 because that is where the group boundary sits
// and where the interesting behaviour is, then coarse above it: past 8 the
// question is only "are we filling groups", and the answer is yes.
var batchWidthBuckets = [...]int{1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024}

// One extra slot for everything above the last bucket.
var batchWidthCounts [len(batchWidthBuckets) + 1]atomic.Uint64

var (
	batchesObserved   atomic.Uint64
	signaturesTotal   atomic.Uint64
	emptyBatchesTotal atomic.Uint64
)

// observeBatchWidth records one verified batch. It is called from Verify rather
// than from each drain site so that a new caller cannot forget to instrument
// itself, and it is a plain atomic add into a fixed array: no allocation, no
// lock, and nothing that can fail on the verification path.
func observeBatchWidth(width int) {
	batchesObserved.Add(1)
	if width <= 0 {
		emptyBatchesTotal.Add(1)
		return
	}
	signaturesTotal.Add(uint64(width))

	index := len(batchWidthBuckets)
	for i, upper := range batchWidthBuckets {
		if width <= upper {
			index = i
			break
		}
	}
	batchWidthCounts[index].Add(1)
}

// WidthBucket is one row of the batch-width distribution. Upper is inclusive;
// the final row reports Upper of 0 to mean "wider than every named bucket".
type WidthBucket struct {
	Upper   int
	Batches uint64
}

// Snapshot is a point-in-time view of verification behaviour. It is a value, so
// a reporter can take one and format it without holding anything.
type Snapshot struct {
	Backend                string
	InternalFaultFallbacks uint64
	Batches                uint64
	Signatures             uint64
	EmptyBatches           uint64
	Widths                 []WidthBucket
}

// MeanWidth is the average number of signatures per non-empty batch. It is the
// one-number summary; the distribution is what actually matters, because a mean
// of 4 is produced both by every batch being width 4 and by half being width 1
// and half width 7, and those cost very different amounts.
func (s Snapshot) MeanWidth() float64 {
	nonEmpty := s.Batches - s.EmptyBatches
	if nonEmpty == 0 {
		return 0
	}
	return float64(s.Signatures) / float64(nonEmpty)
}

// FullGroupShare is the fraction of signatures that arrived in a batch of at
// least eight, which is the fraction getting the accelerated path's full
// benefit. This is the number to watch when deciding whether a drain policy is
// working.
func (s Snapshot) FullGroupShare() float64 {
	if s.Signatures == 0 {
		return 0
	}
	var wide uint64
	for _, b := range s.Widths {
		if b.Upper == 0 || b.Upper >= 8 {
			// Approximate: a bucket's signatures are not recorded per bucket,
			// only its batch count, so weight by the bucket's upper bound. For
			// the per-width buckets at and below 8 this is exact.
			wide += b.Batches * uint64(max(b.Upper, 8))
		}
	}
	if wide > s.Signatures {
		return 1
	}
	return float64(wide) / float64(s.Signatures)
}

// Stats returns the current snapshot. Counters are monotonic for the life of
// the process; a reporter that wants deltas should difference two snapshots.
func Stats() Snapshot {
	widths := make([]WidthBucket, 0, len(batchWidthCounts))
	for i := range batchWidthCounts {
		count := batchWidthCounts[i].Load()
		if count == 0 {
			continue
		}
		upper := 0
		if i < len(batchWidthBuckets) {
			upper = batchWidthBuckets[i]
		}
		widths = append(widths, WidthBucket{Upper: upper, Batches: count})
	}

	return Snapshot{
		Backend:                Backend(),
		InternalFaultFallbacks: InternalFaultFallbacks(),
		Batches:                batchesObserved.Load(),
		Signatures:             signaturesTotal.Load(),
		EmptyBatches:           emptyBatchesTotal.Load(),
		Widths:                 widths,
	}
}

// String renders the snapshot as one line per report. Callers write this to the
// per-run log directory; it is deliberately not printed to the terminal, where
// it would compete with replay progress for an operator's attention.
func (s Snapshot) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "backend=%s fallbacks=%d batches=%d signatures=%d mean_width=%.2f full_group_share=%.1f%%",
		s.Backend, s.InternalFaultFallbacks, s.Batches, s.Signatures,
		s.MeanWidth(), s.FullGroupShare()*100)
	if s.EmptyBatches > 0 {
		fmt.Fprintf(&b, " empty=%d", s.EmptyBatches)
	}
	if len(s.Widths) == 0 {
		return b.String()
	}
	b.WriteString(" widths=[")
	for i, w := range s.Widths {
		if i > 0 {
			b.WriteString(" ")
		}
		if w.Upper == 0 {
			fmt.Fprintf(&b, ">%d:%d", batchWidthBuckets[len(batchWidthBuckets)-1], w.Batches)
			continue
		}
		fmt.Fprintf(&b, "%d:%d", w.Upper, w.Batches)
	}
	b.WriteString("]")
	return b.String()
}
