package accountsdb

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/cockroachdb/pebble/v2"
)

// FoldTimings splits one CommitBatch into its sequential steps. They are
// disjoint and run in this order, so together with LockWait they add up to
// the call's wall time (up to bookkeeping between steps).
type FoldTimings struct {
	LockWait     time.Duration // waiting for foldMu (compaction, rewind or recovery held it)
	Dedupe       time.Duration // newest-wins union of the batch's slot deltas, disk budget check
	FileID       time.Duration // fileId allocation and its fsynced high-water mark
	SegmentWrite time.Duration // segment file write, fsync and directory fsync
	UndoCapture  time.Duration // previous index entries of every key (Pebble reads)
	Manifest     time.Duration // manifest tmp write, fsync, rename and directory fsync
	BankhashRows time.Duration // advisory bankhash rows (unsynced)
	IndexCommit  time.Duration // publication and the synced Pebble index batch
	CacheRefresh time.Duration // read-cache refresh with the folded values
}

func (t FoldTimings) String() string {
	return fmt.Sprintf("lock_wait=%s dedupe=%s file_id=%s segment_write=%s undo_capture=%s manifest=%s bankhash_rows=%s index_commit=%s cache_refresh=%s",
		r(t.LockWait), r(t.Dedupe), r(t.FileID), r(t.SegmentWrite), r(t.UndoCapture), r(t.Manifest), r(t.BankhashRows), r(t.IndexCommit), r(t.CacheRefresh))
}

func r(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

// stepClock advances through sequential steps, charging each the time since
// the previous mark.
type stepClock struct{ last time.Time }

func (c *stepClock) step(into *time.Duration) {
	now := time.Now()
	*into += now.Sub(c.last)
	c.last = now
}

// pebbleEventListener reports one Pebble store's flushes, compactions, write
// stalls and slow disk operations into the diagnostics summary, with warnings
// for any write stall or slow disk operation and for unusually long jobs.
func pebbleEventListener(store string) *pebble.EventListener {
	flush := metrics.Background("pebble."+store+".flush", 2*time.Second)
	compaction := metrics.Background("pebble."+store+".compaction", 10*time.Second)
	stall := metrics.Background("pebble."+store+".write_stall", time.Millisecond)
	slow := metrics.Background("pebble."+store+".disk_slow", time.Millisecond)
	var stallStart atomic.Int64
	var stallReason atomic.Pointer[string]
	return &pebble.EventListener{
		FlushEnd: func(info pebble.FlushInfo) {
			if info.Err != nil {
				return
			}
			var out uint64
			for _, t := range info.Output {
				out += t.Size
			}
			flush.Observe(info.TotalDuration, func() string {
				return fmt.Sprintf("%s, %d MiB in, %d MiB written", info.Reason, info.InputBytes>>20, out>>20)
			})
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			if info.Err != nil {
				return
			}
			compaction.Observe(info.TotalDuration, func() string {
				var levels []string
				for _, l := range info.Input {
					levels = append(levels, fmt.Sprintf("L%d", l.Level))
				}
				return fmt.Sprintf("%s, %s -> L%d", info.Reason, strings.Join(levels, "+"), info.Output.Level)
			})
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			reason := info.Reason
			stallReason.Store(&reason)
			stallStart.Store(time.Now().UnixNano())
		},
		WriteStallEnd: func() {
			start := stallStart.Swap(0)
			if start == 0 {
				return
			}
			stall.Observe(time.Since(time.Unix(0, start)), func() string {
				if p := stallReason.Load(); p != nil {
					return "writes blocked: " + *p
				}
				return "writes blocked"
			})
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			slow.Observe(info.Duration, func() string { return info.String() })
		},
	}
}
