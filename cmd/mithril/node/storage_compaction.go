package node

import (
	"context"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

type accountsCompactor interface {
	CompactOnce(accountsdb.CompactionConfig) (accountsdb.CompactStats, error)
}

// stop joins the worker before AccountsDB closes. A timer after each completed
// cycle avoids queued back-to-back work if storage is temporarily slow.
// compactionTask budgets one compaction cycle, which holds the fold lock.
var compactionTask = metrics.Background("accountsdb.compaction", 500*time.Millisecond)

func startAccountsCompactor(parent context.Context, db accountsCompactor, cfg accountsdb.CompactionConfig, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	mlog.Log.Infof("accountsdb: background compaction enabled (interval=%s rewind_horizon=%d batches)", interval, cfg.RewindHorizonBatches)
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			run := compactionTask.Start()
			stats, err := db.CompactOnce(cfg)
			if err == nil {
				scan := stats.RewindScan
				mlog.Log.FileOnlyf("rewind map scan: head_slot=%d head_seq=%d path=%s manifests_decoded=%d records_decoded=%d pins_ns=%d lock_ns=%d acquired_ns=%d files_compacted=%d files_sparsified=%d files_deleted=%d live_bytes_moved=%d bytes_reclaimed=%d",
					scan.HeadSlot, scan.HeadSeq, scan.Path, scan.DecodedManifests, scan.DecodedRecords, scan.PinsNS, scan.LockNS, scan.AcquiredNS, stats.FilesCompacted, stats.FilesSparsified, stats.FilesDeleted, stats.LiveBytesMoved, stats.BytesReclaimed)
			}
			run.EndErr(err, func() string {
				return fmt.Sprintf("%d scanned, %d compacted, %d sparsified, %d deleted, %d bytes moved", stats.CandidatesScanned, stats.FilesCompacted, stats.FilesSparsified, stats.FilesDeleted, stats.LiveBytesMoved)
			})
			if err != nil {
				mlog.Log.Warnf("compaction cycle failed: %v", err)
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
