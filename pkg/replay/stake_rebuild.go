package replay

import (
	"context"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

type stakeAccountReader interface {
	GetAccount(uint64, solana.PublicKey) (*accounts.Account, error)
	GetAccountsBatch(context.Context, uint64, []solana.PublicKey) ([]*accounts.Account, error)
}

const stakeRebuildBatchSize = 2000

// loadStakeAccountTotals reads the current database view during startup cache
// initialization. Index entries provide keys and locality hints, never cached stake data.
func loadStakeAccountTotals(db stakeAccountReader, slot uint64, entries []accountsdb.StakeIndexEntry) map[solana.PublicKey]uint64 {
	if len(entries) <= 2*stakeRebuildBatchSize {
		return loadStakeAccountRange(db, slot, entries)
	}
	// Overlap two bounded scans so one range can read while the other decodes.
	// Prefer a nearby file boundary without giving a large file most of the work.
	mid := len(entries) / 2
	boundary := mid
	for boundary < len(entries) && boundary-mid < stakeRebuildBatchSize && entries[boundary].FileId == entries[boundary-1].FileId {
		boundary++
	}
	if boundary < len(entries) && entries[boundary].FileId != entries[boundary-1].FileId {
		mid = boundary
	}
	var left map[solana.PublicKey]uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		left = loadStakeAccountRange(db, slot, entries[:mid])
	}()
	right := loadStakeAccountRange(db, slot, entries[mid:])
	wg.Wait()
	for voter, stake := range right {
		left[voter] += stake
	}
	return left
}

func loadStakeAccountRange(db stakeAccountReader, slot uint64, entries []accountsdb.StakeIndexEntry) map[solana.PublicKey]uint64 {
	const maxBatchSize = stakeRebuildBatchSize
	totals := make(map[solana.PublicKey]uint64)
	keys := make([]solana.PublicKey, 0, maxBatchSize)
	for start := 0; start < len(entries); {
		end := start + 1
		for end < len(entries) && end-start < maxBatchSize && entries[end].FileId == entries[start].FileId {
			end++
		}
		keys = keys[:0]
		for _, entry := range entries[start:end] {
			keys = append(keys, entry.Pubkey)
		}
		// The batch reader already parallelizes index and appendvec reads. Keep
		// one batch per range in flight, bounding the total at two batches.
		batch, batchErr := db.GetAccountsBatch(context.Background(), slot, keys)
		for i, key := range keys {
			var acct *accounts.Account
			if batchErr == nil && len(batch) == len(keys) {
				acct = batch[i]
			}
			// Batch reads normalize zero-lamport records to empty placeholders
			// and fail as a unit on I/O errors. Preserve the previous per-account
			// behavior in both cases, including stake data on zero-lamport records.
			if acct == nil || acct.Lamports == 0 {
				var err error
				acct, err = db.GetAccount(slot, key)
				if err != nil || acct == nil {
					continue
				}
			}
			stake, err := sealevel.UnmarshalStakeState(acct.Data)
			if err != nil || stake.Status != sealevel.StakeStateV2StatusStake {
				continue
			}
			delegation := stake.Stake.Stake.Delegation
			totals[delegation.VoterPubkey] += delegation.StakeLamports
		}
		start = end
	}
	return totals
}
