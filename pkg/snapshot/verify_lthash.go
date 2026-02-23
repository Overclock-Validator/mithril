package snapshot

import (
	"fmt"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
)

// VerifySnapshotLtHash re-computes the lattice hash from all accounts in the
// AccountsDB and compares it against the LtHash declared in the snapshot manifest.
//
// If they don't match, the snapshot may have been tampered with and should be
// discarded. This prevents a DoS vector where a malicious RPC node serves a
// snapshot with altered account data.
//
// The computation is parallelized: a single goroutine iterates the pebble index
// to collect pubkeys in batches, and a pool of workers reads the accounts and
// computes their individual LtHash contributions. Partial results are merged at
// the end.
func VerifySnapshotLtHash(accountsDb *accountsdb.AccountsDb, manifestLtHash *lthash.LtHash) error {
	if manifestLtHash == nil {
		mlog.Log.Warnf("Snapshot does not contain an LtHash — skipping verification (pre-LtHash snapshot)")
		return nil
	}

	mlog.Log.Infof("Verifying snapshot integrity: computing LtHash over all accounts...")
	start := time.Now()

	// Iterate over all keys in the pebble index
	iter, err := accountsDb.Index.NewIter(&pebble.IterOptions{})
	if err != nil {
		return fmt.Errorf("creating pebble iterator: %w", err)
	}
	defer iter.Close()

	// Collect all pubkeys first so we can parallelize the hash computation.
	// Each key in the index is a 32-byte Solana public key.
	var allPubkeys []solana.PublicKey

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != 32 {
			continue // skip malformed entries
		}
		var pk solana.PublicKey
		copy(pk[:], key)
		allPubkeys = append(allPubkeys, pk)
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating pebble index: %w", err)
	}

	totalAccounts := len(allPubkeys)
	if totalAccounts == 0 {
		return fmt.Errorf("SNAPSHOT INTEGRITY FAILURE: pebble index contains 0 accounts — snapshot may be empty or corrupted")
	}
	mlog.Log.Infof("LtHash verification: hashing %d accounts...", totalAccounts)

	// Parallelize: split pubkeys into chunks, each worker computes a partial LtHash
	numWorkers := 32
	if totalAccounts < numWorkers {
		numWorkers = max(1, totalAccounts)
	}
	chunkSize := (totalAccounts + numWorkers - 1) / numWorkers

	partialHashes := make([]lthash.LtHash, numWorkers)
	var wg sync.WaitGroup

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startIdx := workerID * chunkSize
			endIdx := min(startIdx+chunkSize, totalAccounts)

			var partial lthash.LtHash
			for i := startIdx; i < endIdx; i++ {
				pk := allPubkeys[i]
				acct, err := accountsDb.GetAccount(0, pk)
				if err != nil {
					continue // account may have been deleted (0 lamports)
				}
				if acct.Lamports == 0 {
					continue // zero-lamport accounts don't contribute to LtHash
				}

				var acctHash lthash.LtHash
				acctHash.InitWithAcct(acct)
				partial.MixIn(&acctHash)
			}
			partialHashes[workerID] = partial
		}(w)
	}
	wg.Wait()

	// Merge all partial hashes
	var computed lthash.LtHash
	for i := range numWorkers {
		computed.MixIn(&partialHashes[i])
	}

	elapsed := time.Since(start)

	if !computed.Equals(manifestLtHash) {
		return fmt.Errorf(
			"SNAPSHOT INTEGRITY FAILURE: computed LtHash does not match manifest LtHash "+
				"(%d accounts verified in %s). This snapshot may have been tampered with — "+
				"discard it and download from a different source",
			totalAccounts, fmtDuration(elapsed),
		)
	}

	mlog.Log.Infof("✓ Snapshot LtHash verified: %d accounts match manifest (%s)", totalAccounts, fmtDuration(elapsed))
	return nil
}
