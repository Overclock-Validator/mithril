package bankhash

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

func updateAcctsLtHash(slotCtx *sealevel.SlotCtx, modifiedAccts []*accounts.Account) {
	deltaLtHash := calculateDeltaLtHash(slotCtx, modifiedAccts)
	slotCtx.AcctsLtHash.Add(deltaLtHash)
	if ltDebug && (ltDebugSlot == 0 || ltDebugSlot == slotCtx.Slot) {
		dumpPerAcctDeltas(slotCtx, modifiedAccts)
	}
}

// ltDebug (MITHRIL_LTHASH_DEBUG=1) dumps every account's (key, old, new) state per
// slot so two runs can be diffed to the exact diverging account.
var ltDebug = os.Getenv("MITHRIL_LTHASH_DEBUG") == "1"
var ltDebugSlot, _ = strconv.ParseUint(os.Getenv("MITHRIL_LTHASH_DEBUG_SLOT"), 10, 64)

func dumpPerAcctDeltas(slotCtx *sealevel.SlotCtx, modifiedAccts []*accounts.Account) {
	for _, a := range dedupeModifiedAccts(modifiedAccts) {
		prev, err := slotCtx.GetParentAccount(a.Key)
		oldSum := "ERR"
		if err == nil {
			var oh lthash.LtHash
			oh.InitWithAcct(prev)
			oldSum = fmt.Sprintf("%x:%d", oh.Checksum(), prev.Lamports)
		}
		var nh lthash.LtHash
		nh.InitWithAcct(a)
		mlog.Log.Infof("LTDBG slot=%d key=%s old=%s new=%x:%d",
			slotCtx.Slot, a.Key, oldSum, nh.Checksum(), a.Lamports)
	}
}

func calculateDeltaLtHash(slotCtx *sealevel.SlotCtx, modifiedAccts []*accounts.Account) *lthash.LtHash {
	// Dedupe by key (keep the last/newest value): an account that appears more
	// than once (e.g. both rent-collected and modified, or a sysvar collected via
	// multiple paths) would otherwise have its delta counted multiple times,
	// corrupting the cumulative LtHash.
	recordMetrics := slotCtx.Replay
	var dedupeStart time.Time
	if recordMetrics {
		metrics.GlobalBlockReplay.LtHashInputAccounts = uint64(len(modifiedAccts))
		metrics.GlobalBlockReplay.LtHashUniqueAccounts = 0
		metrics.GlobalBlockReplay.LtHashUnchangedAccounts = 0
		metrics.GlobalBlockReplay.LtHashCreatedAccounts = 0
		metrics.GlobalBlockReplay.LtHashDeletedAccounts = 0
		metrics.GlobalBlockReplay.LtHashOldDataBytes = 0
		metrics.GlobalBlockReplay.LtHashNewDataBytes = 0
		metrics.GlobalBlockReplay.LtHashUnchangedDataBytes = 0
		dedupeStart = time.Now()
	}
	modifiedAccts = dedupeModifiedAccts(modifiedAccts)
	if recordMetrics {
		metrics.GlobalBlockReplay.LtHashDedupe.AddTimingSince(dedupeStart)
		metrics.GlobalBlockReplay.LtHashUniqueAccounts = uint64(len(modifiedAccts))
	}
	if len(modifiedAccts) == 0 {
		return &lthash.LtHash{}
	}

	numWorkers := min(32, len(modifiedAccts))
	partials := make([]lthash.LtHash, numWorkers)
	workerStats := make([]ltHashWorkerStats, numWorkers)
	chunkSize := (len(modifiedAccts) + numWorkers - 1) / numWorkers

	var workerStart time.Time
	if recordMetrics {
		workerStart = time.Now()
	}
	var wg sync.WaitGroup
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * chunkSize
			end := min(start+chunkSize, len(modifiedAccts))
			var partial lthash.LtHash
			var stats ltHashWorkerStats

			// Reuse one 2 KiB hash and one BLAKE3 hasher for every old/new
			// account value owned by this worker.
			var scratch lthash.LtHash
			var hasher lthash.AccountHasher

			for j := start; j < end; j++ {
				accumulateSingleDeltaLtHash(slotCtx, modifiedAccts[j], &partial, &scratch, &hasher, &stats)
			}
			partials[workerID] = partial
			workerStats[workerID] = stats
		}(i)
	}
	wg.Wait()
	if recordMetrics {
		metrics.GlobalBlockReplay.LtHashWorkerCompute.AddTimingSince(workerStart)
	}

	var reduceStart time.Time
	if recordMetrics {
		reduceStart = time.Now()
	}
	var deltaHash lthash.LtHash
	var totals ltHashWorkerStats
	for i := range partials {
		deltaHash.Add(&partials[i])
		totals.add(workerStats[i])
	}
	if recordMetrics {
		metrics.GlobalBlockReplay.LtHashPartialReduce.AddTimingSince(reduceStart)
		metrics.GlobalBlockReplay.LtHashUnchangedAccounts = totals.unchangedAccounts
		metrics.GlobalBlockReplay.LtHashCreatedAccounts = totals.createdAccounts
		metrics.GlobalBlockReplay.LtHashDeletedAccounts = totals.deletedAccounts
		metrics.GlobalBlockReplay.LtHashOldDataBytes = totals.oldDataBytes
		metrics.GlobalBlockReplay.LtHashNewDataBytes = totals.newDataBytes
		metrics.GlobalBlockReplay.LtHashUnchangedDataBytes = totals.unchangedDataBytes
	}

	return &deltaHash
}

type ltHashWorkerStats struct {
	unchangedAccounts  uint64
	createdAccounts    uint64
	deletedAccounts    uint64
	oldDataBytes       uint64
	newDataBytes       uint64
	unchangedDataBytes uint64
}

func (stats *ltHashWorkerStats) add(other ltHashWorkerStats) {
	stats.unchangedAccounts += other.unchangedAccounts
	stats.createdAccounts += other.createdAccounts
	stats.deletedAccounts += other.deletedAccounts
	stats.oldDataBytes += other.oldDataBytes
	stats.newDataBytes += other.newDataBytes
	stats.unchangedDataBytes += other.unchangedDataBytes
}

// dedupeModifiedAccts collapses duplicate keys, keeping each key's last
// occurrence (the newest value) and preserving first-seen order. nil entries are
// dropped. Without this, a key appearing twice would have its LtHash delta
// applied twice.
func dedupeModifiedAccts(modifiedAccts []*accounts.Account) []*accounts.Account {
	if len(modifiedAccts) == 0 {
		return modifiedAccts
	}
	if len(modifiedAccts) == 1 {
		if modifiedAccts[0] == nil {
			return nil
		}
		return modifiedAccts
	}

	unique := make([]*accounts.Account, 0, len(modifiedAccts))
	seen := make(map[[32]byte]int, len(modifiedAccts))
	for _, acct := range modifiedAccts {
		if acct == nil {
			continue
		}
		key := [32]byte(acct.Key)
		if idx, ok := seen[key]; ok {
			unique[idx] = acct
			continue
		}
		seen[key] = len(unique)
		unique = append(unique, acct)
	}
	return unique
}

func accumulateSingleDeltaLtHash(
	slotCtx *sealevel.SlotCtx,
	modifiedAcct *accounts.Account,
	deltaLtHash *lthash.LtHash,
	scratch *lthash.LtHash,
	hasher *lthash.AccountHasher,
	stats *ltHashWorkerStats,
) {
	previousAcct, err := slotCtx.GetParentAccount(modifiedAcct.Key)
	if err != nil {
		panic(fmt.Sprintf("couldn't find parent acct for %s for slot %d", modifiedAcct.Key, slotCtx.Slot))
	}

	// Zero-lamport accounts do not contribute, regardless of their other
	// fields. This also handles the zero-to-zero no-op without hashing.
	if previousAcct.Lamports == 0 {
		if modifiedAcct.Lamports == 0 {
			stats.unchangedAccounts++
			return
		}

		stats.createdAccounts++
		stats.newDataBytes += uint64(len(modifiedAcct.Data))
		hasher.HashInto(scratch, modifiedAcct)
		deltaLtHash.Add(scratch)
		return
	}

	if modifiedAcct.Lamports == 0 {
		stats.deletedAccounts++
		stats.oldDataBytes += uint64(len(previousAcct.Data))
		hasher.HashInto(scratch, previousAcct)
		deltaLtHash.Sub(scratch)
		return
	}

	// Rent epoch is deliberately absent: it is not an input to the accounts
	// LtHash. A rent-epoch-only write therefore has an exact zero delta.
	if acctsLtHashEqual(modifiedAcct, previousAcct) {
		stats.unchangedAccounts++
		stats.unchangedDataBytes += uint64(len(modifiedAcct.Data))
		return
	}

	stats.oldDataBytes += uint64(len(previousAcct.Data))
	stats.newDataBytes += uint64(len(modifiedAcct.Data))
	hasher.HashInto(scratch, previousAcct)
	deltaLtHash.Sub(scratch)
	hasher.HashInto(scratch, modifiedAcct)
	deltaLtHash.Add(scratch)
}

func acctsLtHashEqual(a *accounts.Account, b *accounts.Account) bool {
	return a.Lamports == b.Lamports &&
		a.Executable == b.Executable &&
		a.Owner == b.Owner &&
		bytes.Equal(a.Data, b.Data)
}
